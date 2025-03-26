from h5coro import h5coro, h5promise, s3driver, filedriver, webdriver, logger
from h5coro.h5view import H5View
from h5coro.h5promise import massagePath
from h5coro.lazyh5dataset import LazyH5Dataset, LazyBackendArray
import xarray as xr
from xarray.backends import BackendEntrypoint
from xarray.core.dataset import Dataset
import os
import re
import copy

class H5CoroBackendEntrypoint(BackendEntrypoint):
    '''
    A class for integrating h5coro into xarray as a custom backend engine.
    '''

    def open_dataset(
        self,
        filename_or_obj,
        *,
        group,
        credentials={},
        log_level='ERROR',
        verbose=False,
        multi_process=False,
        col_convs={},
        col_coords={},
        pick_variables=None,
        drop_variables=None,
        hyperslices=[]
    ) -> Dataset:
        '''
        Constructor for the H5CoroBackendEntrypoint class which extends the BackendEntrypoint class from xarray.

        Parameters
        ----------
        group:          str; REQUIRED
                        A path indicating which group within the hdf file to read. Root group should be
                        specified by '/'.
        credentials:    dict
                        (1) a dict with keys aws_access_key_id, aws_secret_access_key, and aws_session_token;
                        (2) a dict with keys accessKeyID, secretAccessKey, and sessionToken;
                        (3) an aws boto3 object;
                        (4) an earthaccess credentials object
        log_level:      str
                        Python logging levels supplied as an all uppercase string;
                        DEBUG, INFO, WARNING, ERROR, CRITICAL
        verbose:        bool
                        flag indicating whether h5coro code should print lots of debug messages
        multi_process:  bool
                        flag indicating whether h5coro should run in multiprocessing mode
        col_convs:      dict
                        dictionary of conversion functions; the key is the column name and the signature
                        of the conversion function is (raw_value) => converted_value
        col_coords:     dict
                        dictionary of coordinate names for the provided columns;
                        {"<col_name>": ("<dim1_coord>", "<dim2_coord>", ...)}
        pick_variables: list
                        list of variables to exclusively include in the final xarray
        drop_variables: list
                        list of variables not to include in the final xarray
        hyperslices:    list
                        A list of lists specifying the range of data to extract for each dimension.
                        Each variable in the group will have the corresponding slices applied to its dimensions.
                        [[start_dim0, end_dim0], [start_dim1, end_dim1], ...]
        '''
        # sanitize input parameters
        group = massagePath(group)

        # set h5coro logging level
        logger.config(log_level)

        # determine driver
        if filename_or_obj.startswith("file://"):
            filename_or_obj = filename_or_obj[len("file://"):]
            driver = filedriver.FileDriver
        elif filename_or_obj.startswith("https://"):
            driver = webdriver.HTTPDriver
        else:
            driver = s3driver.S3Driver

        # connect to the s3 object
        h5obj = h5coro.H5Coro(filename_or_obj, driver, credentials=credentials, verbose=verbose, multiProcess=multi_process)

        # determine the variables and attributes in the specified group
        variables, group_attr, _groups = h5obj.list(group, w_attr=True)

        # override variables requested to be read (remove everything not in list)
        if type(pick_variables) == list and len(pick_variables) > 0:
            vars_to_drop = []
            for var_to_pick in variables:
                if var_to_pick not in pick_variables:
                    vars_to_drop.append(var_to_pick)
            for var_to_drop in vars_to_drop:
                variables.pop(var_to_drop)

        # remove variables that have been requested to be dropped
        if type(drop_variables) == list:
            for var_to_drop in drop_variables:
                if var_to_drop in variables:
                    variables.pop(var_to_drop)


        # get variable coordinates
        coord_names = set()
        for var in variables.keys():
            try:
                # check for coordinate variables
                var_coords = re.split(';|,| |\n', variables[var]['coordinates'])
                var_coords = [c for c in var_coords if c]
                # add any coordinates to the coord_names set
                for c in var_coords:
                    coord_names.add(c)
            except KeyError:
                # if no coordinates were listed for that variable then set it's coordinate as itself
                var_coords = [var]

        # create xarray dataset
        var_paths = [os.path.join(group, name) for name in variables.keys()]
        data_vars = {}
        dataset_list = []
        lazy_datasets = {}
        for var_path in var_paths:
            var_name = var_path.split("/")[-1]
            meta = variables[var_name]['__metadata__']
            dtype = meta.getNumpyType()
            shape = meta.getShape()

            # apply only relevant hyperslices based on shape length
            num_dims = len(shape)
            trimmed_slices = []
            for i in range(num_dims):
                dim_len = shape[i]
                if i < len(hyperslices):
                    start, stop = hyperslices[i]

                    # clamp the start/stop to valid bounds
                    start = max(0, min(start, dim_len))
                    stop = max(start, min(stop, dim_len))

                    trimmed_slices.append([start, stop])
                else:
                    # no slice specified for this dimension: take the whole thing
                    trimmed_slices.append([0, dim_len])

            # create a list of dataset dictionaries to pass to readDatasets
            dataset_dict = {"dataset": var_path, "hyperslice": copy.deepcopy(trimmed_slices)}
            dataset_list.append(dataset_dict)

            # compute the sliced shape from the trimmed slices
            sliced_shape = tuple(stop - start for (start, stop) in trimmed_slices)

            # create a lazy dataset wrapper that will trigger async read when accessed
            lazy_ds = LazyH5Dataset(var_path, sliced_shape, dtype)
            lazy_datasets[var_path] = lazy_ds

            # determine dimension names
            data_dims = len(shape)
            if var_path in col_coords:
                dim_names = col_coords[var_path]
            elif data_dims == 1:
                dim_names = var_coords[0]
            else:
                dim_names = [f"{var_path}_{i}" for i in range(len(shape))]

            # store the variable in xarray.Dataset with the short name
            data_vars[var_name] = (dim_names, xr.Variable(dim_names, LazyBackendArray(lazy_ds).to_xarray_lazy()))

        # trigger full data read (non-blocking)
        promise = h5obj.readDatasets(dataset_list, block=False)

        # assign promise to LazyH5Dataset for deferred reading
        for ds_name in lazy_datasets:
            lazy_datasets[ds_name].set_promise(promise)

        # seperate out the coordinate variables from the data variables
        coords = {}
        for coord_name in coord_names:
            if coord_name in data_vars:
                # xarry expects coordinates to be immediately available
                data = lazy_datasets[os.path.join(group, coord_name)].read()
                dims = data_vars[coord_name][0]
                coords[coord_name] = (dims, data)

                # remove coordinate from data_vars
                data_vars.pop(coord_name, None)

        # Ensure consistency of dimension coordinates
        dimension_coordinates = [val[0] for val in data_vars.values()]
        for coord_name, coordinate in coords.items():
            # For any of the coordinates that are dimension coordinates, ensure that their own coordinate
            # is set to itself
            if coord_name in dimension_coordinates:
                coords[coord_name] = (coord_name, coordinate[1])

        return xr.Dataset(
                data_vars,
                coords = coords,
                attrs = group_attr,
            )

    def guess_can_open(self, filename_or_obj) -> bool:
        try:
            _, ext = os.path.splitext(filename_or_obj)
        except TypeError:
            return False
        return ext in {".h5", ".h5co", "nc"}

    description = "Support for reading HDF5 files in S3 from H5Coro in xarray"
