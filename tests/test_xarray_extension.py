import pytest
import boto3
import numpy as np
import xarray as xr
from .test_helpers import *

class TestXArrayExtension:
    @classmethod
    def setup_class(cls):
        """Set up the class by downloading the file and reading with h5py."""
        cls.local_file = download_hdf_to_local()
        cls.datasets = [{'dataset': '/gt1l/heights/delta_time', 'hyperslice': None},
                        {'dataset': '/gt1l/heights/dist_ph_across', 'hyperslice': None},
                        {'dataset': '/gt1l/heights/dist_ph_along', 'hyperslice': None},
                        {'dataset': '/gt1l/heights/h_ph', 'hyperslice': None}]

        cls.group ='/gt1l/heights'
        cls.pick_variables = ['delta_time', 'dist_ph_across', 'dist_ph_along', 'h_ph']

        # Read with h5coro file driver
        h5obj = h5coro.H5Coro(cls.local_file, filedriver.FileDriver, verbose=True, errorChecking=True, multiProcess=True)
        promise = h5obj.readDatasets(cls.datasets)
        # Collect results from h5coro
        cls.h5coro_results = {dataset: promise[dataset] for dataset in promise}

        # Convert h5coro results to NumPy arrays
        cls.h5coro_results = {dataset: np.array(data) for dataset, data in cls.h5coro_results.items()}

        h5obj.close()
        h5obj = None

    def check_results(self, xarray_results):
        for dataset in self.h5coro_results:
            dataset_var_name = dataset.split("/")[-1]
            assert dataset_var_name in xarray_results, f"Dataset {dataset} missing in xarray results"
            assert self.h5coro_results[dataset].shape == xarray_results[dataset_var_name].shape, f"Shape mismatch for {dataset}"
            assert np.allclose(self.h5coro_results[dataset], xarray_results[dataset_var_name], atol=1e-6), f"Data mismatch in {dataset}"

    def get_dataset_source(self, src):
        # Resolve local file reference at runtime
        if src == "local_file":
            src = "file://" + self.local_file
        elif src == "s3_object":
            src = HDF_OBJECT_S3[5:]
        elif src == "http_object":
            s3 = boto3.client("s3", region_name="us-west-2")
            src = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": "sliderule", "Key": "data/test/ATL03_20200401184200_00950707_005_01.h5"},
                ExpiresIn=3600 # 1 hour
            )

        return src


    # Define a parameterized test for different dataset sources
    @pytest.mark.parametrize("dataset_source", [
        pytest.param("local_file",  id="Local File"),
        pytest.param("s3_object",   id="S3 Object"),
        pytest.param("http_object", id="HTTP Object")
    ])

    def test_open_dataset(self, dataset_source):
        src = self.get_dataset_source(dataset_source)

        # Open dataset using xarray with h5coro backend
        ds_xarray = xr.open_dataset(src,
                                    engine="h5coro",
                                    group=self.group,
                                    pick_variables=self.pick_variables)

        print(ds_xarray)

        # Use only the variable name inside the selected group
        xarray_results = {entry["dataset"].split("/")[-1]: ds_xarray[entry["dataset"].split("/")[-1]].values for entry in self.datasets}

        # Compare results
        self.check_results(xarray_results)

        # Clean up
        ds_xarray.close()