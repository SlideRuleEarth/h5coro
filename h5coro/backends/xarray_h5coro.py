import logging
import re
import warnings

from xarray.backends import BackendEntrypoint
from h5coro import h5coro, s3driver, filedriver, logger
from h5coro.h5view import H5View
import xarray as xr
import numpy as np
import earthaccess # removed from requirements.txt
import os

from xarray.core.dataset import Dataset


class H5CoroBackendEntrypoint(BackendEntrypoint):
    '''
    A class for integrating h5coro into xarray as a custom backend engine.
    '''

    def open_dataset(
        self,
        filename_or_obj,
        *,
        group,
        creds,  # TODO creds is required because there is an indirect error in h5coro if not
        log_level='ERROR',
        col_convs={},
        drop_variables=None,
    ) -> Dataset:
        '''
        creds: either: a dict with keys aws_access_key_id, aws_secret_access_key, and aws_session_token,
        a dict withkeys accessKeyID, secretAccessKey, and sessionToken, an aws boto3 object, or an 
        earthaccess Auth object
        log_level: indicates level of debugging output to produce. Passed to h5coro logger.config()
        parameter logLevel
        '''
        # set h5coro config to info
        logger.config(log_level)
        
        # extract the s3 credentials dictionary if creds is an earthaccess Auth object
        if isinstance(creds, earthaccess.auth.Auth):
            creds = creds.get_s3_credentials(daac='NSIDC')
        
        # connect to the s3 object
        h5obj = h5coro.H5Coro(filename_or_obj, s3driver.S3Driver, credentials=creds)
        
        # determine the variables and attributes in the specified group
        variables, group_attr, groups = h5obj.list(group, w_attr=True)
        var_paths = [os.path.join(group, name) for name in variables.keys()]
        
        # submit data request for variables and attributes and create data view
        promise = h5obj.readDatasets(var_paths, block=True)
        view = H5View(promise)
        for step in group.split('/'):
            if step != '':  # First group will be '' if there was a leading `/` in the group path
                view = view[step]
        
        # Format the data variables (and coordinate variables)
        variable_dicts = {}
        coordinate_names = []
        for var in view.keys():  
            # check dimensionality
            if variables[var]['__metadata__'].ndims > 1:
                # ignore 2d variables
                warnings.warn((f'Variable {var} has more than 1 dimension. Reading variables with'
                               'more than 1 dimension is not currently supported. This variable will be'
                               'dropped.'))
                continue
            else:
                # check for coordinate variables and add any coordinates to the coordinate_names list
                try:
                    coord = re.split(';|,| |\n', variables[var]['coordinates'])
                    coord = [c for c in coord if c]
                    for c in coord:
                        if c not in coordinate_names:
                            coordinate_names.append(c) 
                except KeyError:
                    # if no coordinates were listed for that variable then set it's coordinate as itself
                    coord = [var]

                # add the variable contents as a tuple to the data variables dictionary
                # (use only the first coordinate since xarray doesn't except more coordinates that dimensions)
                if var in col_convs:
                    variable_dicts[var] = (coord[0], col_convs[var](view[var]), variables[var])
                else:
                    variable_dicts[var] = (coord[0], view[var], variables[var])

        
        # seperate out the coordinate variables from the data variables
        coords = {}
        for coord_name in coordinate_names:
            # drop the coordiante variable from variable_dicts
            coordinate = variable_dicts.pop(coord_name)
            # add the coordiante variable to the coords dictionary
            coords[coord_name] = coordinate
        
        # Ensure consistency of dimension coordinates
        dimension_coordinates = [val[0] for val in variable_dicts.values()]
        for coord_name, coordinate in coords.items():
            # For any of the coordinates that are dimension coordinates, ensure that their own coordinate
            # is set to itself
            if coord_name in dimension_coordinates:
                coords[coord_name] = (coord_name, coordinate[1], coordinate[2])
        
        return xr.Dataset(
                variable_dicts,
                coords = coords,
                attrs = group_attr,
            )

    open_dataset_parameters = ["filename_or_obj", "drop_variables"]

    def guess_can_open(self, filename_or_obj) -> bool:
        try:
            _, ext = os.path.splitext(filename_or_obj)
        except TypeError:
            return False
        return ext in {".h5", ".h5co"}

    description = "Support for reading HDF5 files in S3 from H5Coro in xarray"
