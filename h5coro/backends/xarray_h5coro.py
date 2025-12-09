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
        # Sanitize input parameters
        group = massagePath(group)

        # Set h5coro logging level
        logger.config(log_level)

        # Determine driver
        if filename_or_obj.startswith("file://"):
            filename_or_obj = filename_or_obj[len("file://"):]
            driver = filedriver.FileDriver
        elif filename_or_obj.startswith("https://"):
            driver = webdriver.HTTPDriver
        else:
            driver = s3driver.S3Driver

        # Connect to the s3 object
        h5obj = h5coro.H5Coro(filename_or_obj, driver, credentials=credentials, verbose=verbose, multiProcess=multi_process)

        # Determine the variables and attributes in the specified group
        variables, group_attr, _groups = h5obj.list(group, w_attr=True)

        # Override variables requested to be read (remove everything not in list)
        if type(pick_variables) == list and len(pick_variables) > 0:
            vars_to_drop = []
            for var_to_pick in variables:
                if var_to_pick not in pick_variables:
                    vars_to_drop.append(var_to_pick)
            for var_to_drop in vars_to_drop:
                variables.pop(var_to_drop)

        # Remove variables that have been requested to be dropped
        if type(drop_variables) == list:
            for var_to_drop in drop_variables:
                if var_to_drop in variables:
                    variables.pop(var_to_drop)

        # Get variable coordinates
        coord_names = set()
        for var in variables.keys():
            try:
                # Check for coordinate variables
                var_coords = re.split(';|,| |\n', variables[var]['coordinates'])
                var_coords = [c for c in var_coords if c]
                # Add any coordinates to the coord_names set
                for c in var_coords:
                    coord_names.add(c)
            except KeyError:
                # If no coordinates were listed for that variable then set it's coordinate as itself
                var_coords = [var]

        # Create xarray dataset
        var_paths = [os.path.join(group, name) for name in variables.keys()]
        data_vars = {}
        lazy_datasets = {}
        coord_datasets = []
        data_datasets = []
        data_promise = None
        for var_path in var_paths:
            var_name = var_path.split("/")[-1]
            meta = variables[var_name]['__metadata__']
            dtype = meta.getNumpyType()
            shape = meta.getShape()

            # Apply only relevant hyperslices based on shape length
            num_dims = len(shape)
            trimmed_slices = []
            for i in range(num_dims):
                dim_len = shape[i]
                if i < len(hyperslices):
                    start, stop = hyperslices[i]

                    # Clamp the start/stop to valid bounds
                    start = max(0, min(start, dim_len))
                    stop = max(start, min(stop, dim_len))
                    trimmed_slices.append([start, stop])
                else:
                    # No slice specified for this dimension: take the whole thing
                    trimmed_slices.append([0, dim_len])

            # Compute the sliced shape from the trimmed slices
            sliced_shape = tuple(stop - start for (start, stop) in trimmed_slices)

            # Create a lazy dataset wrapper that will trigger async read when accessed
            lazy_ds = LazyH5Dataset(var_path, sliced_shape, dtype)
            lazy_datasets[var_path] = lazy_ds

            # Determine dimension names
            data_dims = len(shape)
            if var_name in col_coords:
                dim_names = col_coords[var_name]
            elif data_dims == 1:
                dim_names = var_coords[0]
            else:
                dim_names = [f"{var_name}_{i}" for i in range(len(shape))]

            dataset_dict = {"dataset": var_path, "hyperslice": copy.deepcopy(trimmed_slices)}

            # Separate coordinate vs data variable datasets, they will be read separately
            if var_name in coord_names:
                coord_datasets.append(dataset_dict)
            else:
                data_datasets.append(dataset_dict)

            # Store the variable in xarray.Dataset
            data_vars[var_name] = (dim_names, xr.Variable(dim_names, LazyBackendArray(lazy_ds)))

        # NOTE: xarray doesn't support lazy coordinates. They must be resolved at the time xr.Dataset is called.
        # Read coordinate datasets (blocking)
        coords = {}
        if len(coord_datasets) > 0:
            coord_promise = h5obj.readDatasets(coord_datasets, block=True)
            for d in coord_datasets:
                var_path = d["dataset"]
                var_name = var_path.split("/")[-1]
                lazy_ds = lazy_datasets[var_path]
                lazy_ds.set_promise(coord_promise)
                data = lazy_ds.read()
                dims = data_vars[var_name][0]
                coords[var_name] = (dims, data)

                # Remove coordinate variable from data_vars
                data_vars.pop(var_name, None)

        # Trigger async read of remaining datasets (non-blocking)
        if len(data_datasets) > 0:
            data_promise = h5obj.readDatasets(data_datasets, block=False)
            for d in data_datasets:
                var_path = d["dataset"]
                lazy_datasets[var_path].set_promise(data_promise)

        # Ensure consistency of dimension coordinates
        dimension_coordinates = [val[0] for val in data_vars.values()]
        for coord_name, coordinate in coords.items():
            # For any of the coordinates that are dimension coordinates, ensure that their own coordinate
            # is set to itself
            if coord_name in dimension_coordinates:
                coords[coord_name] = (coord_name, coordinate[1])

        ds = xr.Dataset(
                data_vars,
                coords = coords,
                attrs = group_attr,
            )

        # Define a cleanup function registered with xarray instead of relying on GC.
        cleanup_called = False

        def cleanup():
            nonlocal cleanup_called
            if cleanup_called:
                return
            cleanup_called = True

            # Ensure background reads finish before closing the driver.
            if data_promise is not None:
                for dataset in data_promise.keys():
                    try:
                        data_promise.waitOnResult(dataset)
                    except Exception as exc:
                        logger.log.warning(f"Error waiting on promise for {dataset}: {exc}")

            for ld in lazy_datasets.values():
                ld.release()  # Drop the internal reference to shared memory if in use.
            h5obj.close()     # Close the underlying I/O resources.

        ds.set_close(cleanup)
        return ds

    def guess_can_open(self, filename_or_obj) -> bool:
        try:
            _, ext = os.path.splitext(filename_or_obj)
        except TypeError:
            return False
        return ext in {".h5", ".h5co", "nc"}

    description = "Support for reading HDF5 files in S3 from H5Coro in xarray"
