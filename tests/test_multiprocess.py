import pytest
import h5coro
from h5coro import s3driver
from h5coro import filedriver
import boto3
import os
import numpy as np
import sys
import copy

HDF_OBJECT_S3    = "s3://sliderule/data/test/ATL03_20200401184200_00950707_005_01.h5"
HDF_OBJECT_LOCAL = "/tmp/ATL03_20200401184200_00950707_005_01.h5"

DATASET_PATHS = [
   "/gt1l/heights/delta_time"
   "/gt1l/heights/dist_ph_across"
   "/gt1l/heights/dist_ph_along"
   "/gt1l/heights/h_ph"
   "/gt1l/heights/lat_ph"
   "/gt1l/heights/lon_ph"
   "/gt1l/heights/pce_mframe_cnt"
   "/gt1l/heights/ph_id_channel"
   "/gt1l/heights/ph_id_count"
   "/gt1l/heights/ph_id_pulse"
   "/gt1l/heights/quality_ph"
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

    def compare_results(self, s3_results, local_results):
        """Compares the results between the S3 and local dataset reads."""
        for dataset in s3_results:
            s3_data = s3_results[dataset]
            local_data = local_results[dataset]

            # Use numpy's array comparison if both data are arrays
            if isinstance(s3_data, np.ndarray) and isinstance(local_data, np.ndarray):
                np.testing.assert_array_equal(s3_data, local_data, err_msg=f"Mismatch in dataset: {dataset}")
            else:
                # Compare non-array data directly
                assert s3_data == local_data, f"Mismatch in dataset: {dataset}"

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

        # Step 1: Read from S3
        print(f"\nReading datasets from S3 with multiProcess={multiProcess}...")
        s3_results = self.read_datasets(HDF_OBJECT_S3[5:], driver=s3driver.S3Driver, multiProcess=multiProcess)

        # Step 2: Download the file to the local system
        local_file = self.download_hdf_to_local()

        # Step 3: Read from the local file
        print(f"\nReading datasets from local file with multiProcess={multiProcess}...")
        local_results = self.read_datasets(local_file, driver=filedriver.FileDriver, multiProcess=multiProcess)

        # Step 4: Compare results
        print("\nComparing results between S3 and local reads...")
        self.compare_results(s3_results, local_results)