from h5coro import h5coro, s3driver, filedriver, logger
from h5coro.h5view import H5View
from h5coro.h5promise import massagePath
import xarray as xr
from xarray.backends import BackendEntrypoint
from xarray.core.dataset import Dataset
import os
import re

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
        drop_variables=None
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
        '''
        # sanitize input parameters
        group = massagePath(group)

        # set h5coro logging level
        logger.config(log_level)
                
        # determine driver
        if filename_or_obj.startswith("file://"):
            filename_or_obj = filename_or_obj[len("file://"):]
            driver = filedriver.FileDriver
        else: # elif filename_or_obj.startswith("s3://"):
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

        # submit data request for variables and attributes
        var_paths = [os.path.join(group, name) for name in variables.keys()]
        promise = h5obj.readDatasets(var_paths, block=True)

        # create view and move it to the lowest branch node in the tree
        view = H5View(promise)
        for step in group.split('/'):
                view = view[step]
        
        # format the data variables (and coordinate variables)
        data_vars = {}
        coord_names = set()
        for var in view.keys():  
            # get variable coordinates
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
            # set variable data
            if var in col_convs:
                data = col_convs[var](view[var])
            else:
                data = view[var]
            # get dimensionality of data
            data_dims = len(data.shape)
            # set dimension names
            if var in col_coords:
                dim_names = col_coords[var]
            elif data_dims == 1:
                dim_names = var_coords[0]
            else:
                # unable to set multidimensional coordinates automatically
                dim_names = [f'{var}_{n}' for n in range(data_dims)]
            # add to data variables (if a coordinate, will later be moved to coordinates)
            data_vars[var] = (dim_names, data)

        # seperate out the coordinate variables from the data variables
        coords = {}
        for coord_name in coord_names:
            if coord_name in data_vars:
                # move coordiante variable from data_vars to coords
                coords[coord_name] = data_vars.pop(coord_name)
        
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
