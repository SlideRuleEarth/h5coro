import logging
import re
import warnings

from xarray.backends import BackendEntrypoint
from h5coro import h5coro, s3driver, filedriver
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
        drop_variables=None,
    ) -> Dataset:
        '''
        creds: either dict with keys aws_access_key_id, aws_secret_access_key, and aws_session_token 
        or aws boto3 object or earthaccess Auth object
        '''
        
        h5coro.config(logLevel=logging.ERROR)
        
        # format credentials
        if isinstance(creds, earthaccess.auth.Auth):
            earthaccess_dict = creds.get_s3_credentials(daac='NSIDC')
            creds = {}  # reset creds to an empty dictionary
            creds['aws_access_key_id'] = earthaccess_dict['accessKeyId']
            creds['aws_secret_access_key'] = earthaccess_dict['secretAccessKey']
            creds['aws_session_token'] = earthaccess_dict['sessionToken']
        
        # connect to the s3 object
        h5obj = h5coro.H5Coro(filename_or_obj, s3driver.S3Driver, credentials=creds)
        
        # define a function for formatting the output
        def format_variable_attrs(results):
            attrs = {}
            for attribute, value in results["attributes"].items():
                value_str = "<unsupported>"
                if value != None:
                    value_str = f'{value.values}'
                attrs[attribute] = value_str
            return attrs
        
        # loop through that dictionary of datasets to generate a list of groups/variables and attrs
        variables, attributes = h5obj.listGroup(group, w_attr=True, w_inspect=True)
        dataarray_dicts = {}
        coordinate_names = []
        for variable, results in variables.items():
            # pull out data
            # print('------ PROCESSING VARIABLE -----', variable)
            var_data = h5obj.readDatasets(datasets=[variable[1:]], block=True)
            
            # pull out metadata
            var_attrs = format_variable_attrs(results)
            
            # check dimensionality and build dataarray dictionary with relevant variables
            if results['metadata'].ndims and results['metadata'].ndims > 1:
                # ignore the 2d variable
                warnings.warn((f'Variable {variable} has more than 1 dimension. Reading variables with'
                               'more than 1 dimension is not currently supported. This variable will be'
                               'dropped.'))
                continue
            else:
                # build the coordinate list
                try:
                    coord = results['attributes'][os.path.join(variable, 'coordinates')].values
                    coord = re.split(';|,| |\n', coord)
                    coord = [c for c in coord if c]
                    # print('COORD', coord)
                    for c in coord:
                        if not os.path.join(group, c) in coordinate_names:
                            # print('appending ', os.path.join(group, c))
                            coordinate_names.append(os.path.join(group, c))
                except KeyError:
                    # no coordinates were listed for that variable
                    coord = [None]

                # add data to the dict
                dataarray_dicts[variable.split('/')[-1]] = (coord[0], var_data[variable[1:]].values, var_attrs)
        
        # build the dictionary of coordinates
        coords = {}
        # print('coordinate names', coordinate_names)
        for coordinate in coordinate_names:
            # print('coordinate', coordinate)
            short_name = coordinate.split('/')[-1]
            # drop the coordiantes from the dataarray
            coord_values = dataarray_dicts.pop(short_name)
            # add the coordiantes to the dictionary
            coords[short_name] = coord_values[1]

        return xr.Dataset(
            dataarray_dicts,
            coords=coords,
            # attrs={'test': 123},
        )

    open_dataset_parameters = ["filename_or_obj", "drop_variables"]

    def guess_can_open(self, filename_or_obj) -> bool:
        try:
            _, ext = os.path.splitext(filename_or_obj)
        except TypeError:
            return False
        return ext in {".h5", ".h5co"}

    description = "Support for reading HDF5 files in S3 from H5Coro in xarray"
