import pytest
import h5coro
from h5coro import s3driver, filedriver, webdriver
import boto3
import os
import numpy as np
import sys
import copy
import h5py

HDF_OBJECT_S3    = "s3://sliderule/data/test/ATL03_20200401184200_00950707_005_01.h5"
HDF_OBJECT_LOCAL = "/tmp/ATL03_20200401184200_00950707_005_01.h5"

DATASET_PATHS = [
   "/gt1l/heights/delta_time",
   "/gt1l/heights/dist_ph_across",
   "/gt1l/heights/dist_ph_along",
   "/gt1l/heights/h_ph",
   "/gt1l/heights/lat_ph",
   "/gt1l/heights/lon_ph",
   "/gt1l/heights/pce_mframe_cnt",
   "/gt1l/heights/ph_id_channel",
   "/gt1l/heights/ph_id_count",
   "/gt1l/heights/ph_id_pulse",
   "/gt1l/heights/quality_ph",
   "/gt1l/heights/signal_conf_ph"
]

HYPERSLICES = [[100, 5100]]
HYPERSLICES_2D = [[100, 5100], [0, 2]]

@pytest.mark.region
@pytest.mark.parametrize("multiProcess", [False, True])
class TestHDF:

    def download_hdf_to_local(self):
        """Downloads the HDF file to the local system if not already present."""
        if not os.path.exists(HDF_OBJECT_LOCAL):
            print("\nDownloading HDF object to local temp directory...")
            s3_url_parts = HDF_OBJECT_S3.replace("s3://", "").split("/", 1)
            bucket_name = s3_url_parts[0]
            key = s3_url_parts[1]

            s3 = boto3.client("s3")
            s3.download_file(bucket_name, key, HDF_OBJECT_LOCAL)

        return HDF_OBJECT_LOCAL

    def compare_results(self, h5coro_results, h5py_results):
        """Compares the results between two datasets."""
        for dataset in h5py_results:
            expected_data = h5py_results[dataset]

            # Normalize the dataset paths by stripping leading slashes in h5py results
            normalized_dataset = dataset.lstrip('/')
            actual_data = h5coro_results.get(normalized_dataset)

            if actual_data is None:
                raise AssertionError(f"Dataset '{dataset}' not found in actual results")

            # Use numpy's array comparison if both data are arrays
            if isinstance(expected_data, np.ndarray) and isinstance(actual_data, np.ndarray):
                np.testing.assert_array_equal(expected_data, actual_data, err_msg=f"Mismatch in dataset: {dataset}")
            else:
                # Compare non-array data directly
                assert expected_data == actual_data, f"Mismatch in dataset: {dataset}"

    def read_with_h5py(self, file_path):
        """Reads datasets using h5py and returns the results."""
        results = {}
        with h5py.File(file_path, 'r') as hdf_file:
            for dataset_path in DATASET_PATHS:
                if dataset_path in hdf_file:
                    dataset = hdf_file[dataset_path]

                    # Check if dataset is 2D or 1D and apply the appropriate hyperslice
                    if dataset.ndim == 1:
                        results[dataset_path] = dataset[HYPERSLICES[0][0]:HYPERSLICES[0][1]]
                    elif dataset.ndim == 2:
                        results[dataset_path] = dataset[HYPERSLICES_2D[0][0]:HYPERSLICES_2D[0][1],
                                                        HYPERSLICES_2D[1][0]:HYPERSLICES_2D[1][1]]
        return results


    def read_datasets(self, hdf_object, driver, multiProcess):
        """Reads datasets from the specified HDF object and driver."""
        datasets = [
            {'dataset': DATASET_PATHS[i], 'hyperslice': HYPERSLICES if i < 11 else HYPERSLICES_2D}
            for i in range(len(DATASET_PATHS))
        ]

        # Initialize the H5Coro object and read the datasets
        h5obj = h5coro.H5Coro(hdf_object, driver, errorChecking=True, multiProcess=multiProcess)
        promise = h5obj.readDatasets(datasets, block=True)

        # Collect and return results
        # results = {dataset: promise[dataset] for dataset in promise}
        # NOTE: This line causes a resource_tracker warnings about leaking shared memory objects
        # Running 'ipcs -m' after the test ends shows no shared memory objects are left so the warning is harmless?
        # For now, to avoid this warning, deep copy the results into regular memory

        # Deep copy the results into regular memory
        results = copy.deepcopy({dataset: promise[dataset] for dataset in promise})

        return results

    def test_dataset_read(self, multiProcess):
        """Reads datasets from S3 and local file with multiProcess enabled/disabled, then compares the results."""

        # Step 1: Download the file to the local system
        local_file = self.download_hdf_to_local()

        # Step 2: Read using h5py
        print("\nReading with h5py")
        h5py_results = self.read_with_h5py(local_file)

        # Step 3: Read from the local file
        print(f"Reading with filedriver, multiProcess={multiProcess}")
        filedriver_results = self.read_datasets(local_file, driver=filedriver.FileDriver, multiProcess=multiProcess)

        # Step 4: Read from S3
        print(f"Reading with s3driver,   multiProcess={multiProcess}")
        s3driver_results = self.read_datasets(HDF_OBJECT_S3[5:], driver=s3driver.S3Driver, multiProcess=multiProcess)

        # Step 5: Read using WebDriver
        # Generate a pre-signed URL to the same test file
        s3 = boto3.client("s3", region_name="us-west-2")
        pre_signed_url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": "sliderule", "Key": "data/test/ATL03_20200401184200_00950707_005_01.h5"},
            ExpiresIn=3600  # URL valid for 1 hour
        )
        print(f"Reading with webdriver,  multiProcess={multiProcess}")
        webdriver_results = self.read_datasets(pre_signed_url, driver=webdriver.HTTPDriver, multiProcess=multiProcess)

        # Step 6: Check the results against h5py results
        print("Check results:")
        print("\tfiledriver vs h5py")
        self.compare_results(h5coro_results=filedriver_results, h5py_results=h5py_results)

        print("\ts3driver   vs h5py")
        self.compare_results(h5coro_results=s3driver_results, h5py_results=h5py_results)

        print("\twebdriver  vs h5py")
        self.compare_results(h5coro_results=webdriver_results, h5py_results=h5py_results)