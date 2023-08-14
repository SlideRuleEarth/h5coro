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
        
        h5coro.config(errorChecking=True, verbose=False, enableAttributes=False)
        
        # format credentials
        if isinstance(creds, earthaccess.auth.Auth):
            earthaccess_dict = creds.get_s3_credentials(daac='NSIDC')
            creds = {}  # reset creds to an empty dictionary
            creds['aws_access_key_id'] = earthaccess_dict['accessKeyId']
            creds['aws_secret_access_key'] = earthaccess_dict['secretAccessKey']
            creds['aws_session_token'] = earthaccess_dict['sessionToken']
        
        # connect to the s3 object
        h5obj = h5coro.H5Coro(filename_or_obj, s3driver.S3Driver, credentials=creds)
        
        # retrieve a list of variables in that group and create a list of dataset paths
        variables = h5obj.listDirectory(group)
        if drop_variables:
            variables = [v for v in variables if v not in drop_variables]
        datasets = {v: os.path.join(group, v) for v in variables}
        
        # read the data
        h5obj.readDatasets(datasets=datasets.values(), block=True)
        
        # TODO make a list of coordiante variables
        # TODO check for and remove any data variables that have > 1 dimension
        
        # create a dictionary that xarray can use to create DataArrays
        dataarray_dicts = {}
        for v in variables:
            # exclude the coordinate variables (and the problem variable)
            if v not in ['lon_ph', 'lat_ph', 'delta_time', 'signal_conf_ph']:
                dataarray_dicts[v] = ("delta_time", h5obj[os.path.join(group, v)].values)
        
        return xr.Dataset(
            dataarray_dicts,
            coords={
                "lon_ph": h5obj[datasets['lon_ph']].values,
                "lat_ph": h5obj[datasets['lat_ph']].values,
                "delta_time": h5obj[datasets['delta_time']].values,
            },
        )

    open_dataset_parameters = ["filename_or_obj", "drop_variables"]

    def guess_can_open(self, filename_or_obj) -> bool:
        try:
            _, ext = os.path.splitext(filename_or_obj)
        except TypeError:
            return False
        return ext in {".h5", ".h5co"}

    description = "Support for reading HDF5 files in S3 from H5Coro in Xarray"
