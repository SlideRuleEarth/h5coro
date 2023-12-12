import logging
import re
import warnings

from xarray.backends import BackendEntrypoint
from h5coro import h5coro, s3driver, filedriver
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
        drop_variables=None,
    ) -> Dataset:
        '''
        creds: either dict with keys aws_access_key_id, aws_secret_access_key, and aws_session_token 
        or aws boto3 object or earthaccess Auth object
        '''
        
        # format credentials
        if isinstance(creds, earthaccess.auth.Auth):
            earthaccess_dict = creds.get_s3_credentials(daac='NSIDC')
            creds = {}  # reset creds to an empty dictionary
            creds['aws_access_key_id'] = earthaccess_dict['accessKeyId']
            creds['aws_secret_access_key'] = earthaccess_dict['secretAccessKey']
            creds['aws_session_token'] = earthaccess_dict['sessionToken']
        
        # connect to the s3 object
        h5obj = h5coro.H5Coro(filename_or_obj, s3driver.S3Driver, credentials=creds)
        
        # list the variables and attributes in the specified group and read their values
        # TODO what if there is a group inside that group?
        var_paths, attr_paths = h5obj.listGroup(group, w_attr=True, w_inspect=False)
        var_paths = [os.path.join(group, p) for p in var_paths]
        attr_paths = [os.path.join(group, p) for p in attr_paths]
        var_promise = h5obj.readDatasets(var_paths, block=True)
        attr_promise = h5obj.readDatasets(attr_paths, block=True)
        view = H5View(var_promise)['gt1l']['heights']
        view_attr = H5View(attr_promise)['gt1l']['heights']
        
        # Format the top level attributes
        toplevel_attrs = {}
        for var in view_attr.keys():
            toplevel_attrs[var] = view_attr[var]
        
        # Format the data variables
        dataarray_dicts = {}
        coordinate_names = []
        
        for var in view.keys():   
            # pull out metadata
            info = h5obj.listGroup(os.path.join(group, var), w_attr=True, w_inspect=True)
            
            # check dimensionality and build dataarray dictionary with relevant variables
            # QUESTION are the attributes repeated twice?
            if info['coordinates']['__metadata__'].ndims > 1:
                # ignore the 2d variable
                warnings.warn((f'Variable {var} has more than 1 dimension. Reading variables with'
                               'more than 1 dimension is not currently supported. This variable will be'
                               'dropped.'))
                continue
            else:
                # build the coordinate list
                try:
                    coord = re.split(';|,| |\n', info['DIMENSION_LIST']['coordinates'])
                    coord = [c for c in coord if c]
                    for c in coord:
                        if not os.path.join(group, c) in coordinate_names:
                            coordinate_names.append(os.path.join(group, c))
                except KeyError:
                    # no coordinates were listed for that variable
                    coord = ['delta_time']

                # add data to the dict
                dataarray_dicts[var] = (coord[0], view[var], info['description'])
        
        # build the dictionary of coordinates
        coords = {}
        for coordinate in coordinate_names:
            short_name = coordinate.split('/')[-1]
            # drop the coordiantes from the dataarray
            coord_values = dataarray_dicts.pop(short_name)
            # add the coordiantes to the dictionary
            coords[short_name] = coord_values
        
        # manually set delta_time to have delta_time as a coordinate
        # if coords['delta_time']:
        #     coords['delta_time'] = coords['delta_time'][1]

        # print('DAs\n')
        # pprint(dataarray_dicts)
        # print('\n\n\ncoords')
        # pprint(coords)
        # print('\n\n\nattrs')
        # pprint(toplevel_attrs)
        return xr.Dataset(
            dataarray_dicts,
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
