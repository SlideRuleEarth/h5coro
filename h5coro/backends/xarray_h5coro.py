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

from pprint import pprint


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
        log_level='INFO',
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
        var_paths, attr_paths = h5obj.listGroup(group, w_attr=True, w_inspect=False)
        var_paths = [os.path.join(group, p) for p in var_paths]
        attr_paths = [os.path.join(group, p) for p in attr_paths]
        
        # submit data request for variables and attributes and create data view
        var_promise = h5obj.readDatasets(var_paths, block=True)
        attr_promise = h5obj.readDatasets(attr_paths, block=True)
        view = H5View(var_promise)
        view_attr = H5View(attr_promise)
        for step in group.split('/'):
            if step != '':  # First group will be '' if there was a leading `/` in the group path
                view = view[step]
                view_attr = view_attr[step]
        
        # Format the attributes associated with the user provided group path
        toplevel_attrs = {}
        for var in view_attr.keys():
            toplevel_attrs[var] = view_attr[var]
        
        # Format the data variables (and coordinate variables)
        variable_dicts = {}
        coordinate_names = []
        for var in view.keys(): 
            # pull out attributes for that variable
            info = h5obj.listGroup(os.path.join(group, var), w_attr=True, w_inspect=True)
            
            # check dimensionality
            if info['coordinates']['__metadata__'].ndims > 1:
                # ignore the 2d variable
                warnings.warn((f'Variable {var} has more than 1 dimension. Reading variables with'
                               'more than 1 dimension is not currently supported. This variable will be'
                               'dropped.'))
                continue
            else:
                # check for coordinate variables and add any coordinates to the coordinate_names list
                try:
                    coord = re.split(';|,| |\n', info['DIMENSION_LIST']['coordinates'])
                    coord = [c for c in coord if c]
                    for c in coord:
                        if c not in coordinate_names:
                            coordinate_names.append(c) 
                except KeyError:
                    # if no coordinates were listed for that variable then set it's coordinate as itself
                    coord = [var]

                # add the variable contents as a tuple to the data variables dictionary
                # (use only the first coordinate since xarray doesn't except more coordinates that dimensions)
                variable_dicts[var] = (coord[0], view[var], info['description'])
        
        # seperate out the coordinate variables from the data variables
        coords = {}
        for coord_name in coordinate_names:
            # drop the coordiante variable from variable_dicts
            coordinate = variable_dicts.pop(coord_name)
            # add the coordiante variable to the coords dictionary
            coords[coord_name] = coordinate
            
        return xr.Dataset(
            variable_dicts,
            coords = coords,
            attrs = toplevel_attrs,
        )

    open_dataset_parameters = ["filename_or_obj", "drop_variables"]

    def guess_can_open(self, filename_or_obj) -> bool:
        try:
            _, ext = os.path.splitext(filename_or_obj)
        except TypeError:
            return False
        return ext in {".h5", ".h5co"}

    description = "Support for reading HDF5 files in S3 from H5Coro in xarray"
