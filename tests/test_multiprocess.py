import pytest
import h5coro
from h5coro import s3driver, filedriver, webdriver
import boto3
import os
import numpy as np
import sys
import copy
import h5py
import hashlib
import time

HDF_OBJECT_S3     = "s3://sliderule/data/test/ATL03_20200401184200_00950707_005_01.h5"
HDF_OBJECT_LOCAL  = "/tmp/ATL03_20200401184200_00950707_005_01.h5"
HDF_OBJECT_MD5SUM = "b0945a316dd02c18cb86fd7f207c0c54"

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


def compute_md5(file_path):
    """Compute the MD5 checksum of a file."""
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as file:
        for chunk in iter(lambda: file.read(4096), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()

def validate_file_md5(file_path, expected_checksum):
    """Validate the MD5 checksum of the file against the expected checksum."""
    computed_checksum = compute_md5(file_path)
    if computed_checksum != expected_checksum:
        raise ValueError(
            f"Checksum mismatch for {file_path}! "
            f"Expected: {expected_checksum}, Got: {computed_checksum}"
        )
    print(f"MD5 checksum valid for {file_path}")


def download_hdf_to_local():
    if os.path.exists(HDF_OBJECT_LOCAL):
        return HDF_OBJECT_LOCAL

    print("\nDownloading HDF object to local temp directory...")
    s3_url_parts = HDF_OBJECT_S3.replace("s3://", "").split("/", 1)
    bucket_name = s3_url_parts[0]
    key = s3_url_parts[1]

    s3 = boto3.client("s3")
    s3.download_file(bucket_name, key, HDF_OBJECT_LOCAL)
    validate_file_md5(HDF_OBJECT_LOCAL, HDF_OBJECT_MD5SUM)

    return HDF_OBJECT_LOCAL

def read_with_h5py(file_path):
    results = {}
    with h5py.File(file_path, 'r') as hdf_file:
        for dataset_path in DATASET_PATHS:
            if dataset_path in hdf_file:
                dataset = hdf_file[dataset_path]

                # Check if dataset is 2D or 1D and apply the appropriate hyperslice
                if dataset.ndim == 1:
                    results[dataset_path] = dataset[HYPERSLICES[0][0]:HYPERSLICES[0][1]]
                elif dataset.ndim == 2:
                    results[dataset_path] = dataset[
                        HYPERSLICES_2D[0][0]:HYPERSLICES_2D[0][1],
                        HYPERSLICES_2D[1][0]:HYPERSLICES_2D[1][1]
                    ]
    return results


@pytest.mark.region
@pytest.mark.parametrize("multiProcess", [False, True])
class TestHDF:
    @classmethod
    def setup_class(cls):
        """Set up the class by downloading the file and reading with h5py."""
        cls.local_file = download_hdf_to_local()
        cls.h5py_results = read_with_h5py(cls.local_file)

        # Create a pre-signed URL
        cls.s3 = boto3.client("s3", region_name="us-west-2")
        cls.pre_signed_url = cls.s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": "sliderule", "Key": "data/test/ATL03_20200401184200_00950707_005_01.h5"},
            ExpiresIn=36000 # 10 hours for long test runs
        )

        cls.datasets = [
            {'dataset': DATASET_PATHS[i], 'hyperslice': HYPERSLICES if i < 11 else HYPERSLICES_2D}
            for i in range(len(DATASET_PATHS))
        ]


    def compare_results(self, h5coro_results):
        """Compares the results between two datasets."""
        for dataset in self.h5py_results:
            expected_data = self.h5py_results[dataset]

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

    def test_dataset_read(self, multiProcess):
        """Reads datasets from S3 and local file with multiProcess enabled/disabled, then compares the results."""

        print(f"\nmultiProcess:    {multiProcess}")

        # Step 1: Read from the local file
        start_time = time.perf_counter()
        h5obj = h5coro.H5Coro(self.local_file, filedriver.FileDriver, errorChecking=True, multiProcess=multiProcess)
        promise = h5obj.readDatasets(self.datasets, block=True)
        results = {dataset: promise[dataset] for dataset in promise}
        print(f"filedriver read: {time.perf_counter() - start_time:.2f} seconds")
        self.compare_results(results)
        results = None  # Must be set to None to avoid shared memory leaks warnings

        # Step 2: Read from S3
        start_time = time.perf_counter()
        h5obj = h5coro.H5Coro(HDF_OBJECT_S3[5:], s3driver.S3Driver, errorChecking=True, multiProcess=multiProcess)
        promise = h5obj.readDatasets(self.datasets, block=True)
        results = {dataset: promise[dataset] for dataset in promise}
        print(f"s3driver read:   {time.perf_counter() - start_time:.2f} seconds")
        self.compare_results(results)
        results = None

        # # # Step 3: Read using WebDriver
        start_time = time.perf_counter()
        h5obj = h5coro.H5Coro(self.pre_signed_url, webdriver.HTTPDriver, errorChecking=True, multiProcess=multiProcess)
        promise = h5obj.readDatasets(self.datasets, block=True)
        results = {dataset: promise[dataset] for dataset in promise}
        print(f"webdriver read:  {time.perf_counter() - start_time:.2f} seconds")
        self.compare_results(results)
        results = None
