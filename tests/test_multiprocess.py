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
    "/gt1l/heights/signal_conf_ph",
    "/gt1l/geolocation/pitch",
    "/gt1l/geolocation/podppd_flag",
    "/gt1l/geolocation/segment_dist_x",
    "/gt1l/geolocation/segment_id",
    "/gt1l/geolocation/solar_azimuth",
    "/gt1l/geolocation/solar_elevation",
    "/gt1l/geolocation/surf_type",

    "/gt1r/heights/delta_time",
    "/gt1r/heights/dist_ph_across",
    "/gt1r/heights/dist_ph_along",
    "/gt1r/heights/h_ph",
    "/gt1r/heights/lat_ph",
    "/gt1r/heights/lon_ph",
    "/gt1r/heights/pce_mframe_cnt",
    "/gt1r/heights/ph_id_channel",
    "/gt1r/heights/ph_id_count",
    "/gt1r/heights/ph_id_pulse",
    "/gt1r/heights/quality_ph",
    "/gt1r/heights/signal_conf_ph",
    "/gt1r/geolocation/pitch",
    "/gt1r/geolocation/podppd_flag",
    "/gt1r/geolocation/segment_dist_x",
    "/gt1r/geolocation/segment_id",
    "/gt1r/geolocation/solar_azimuth",
    "/gt1r/geolocation/solar_elevation",
    "/gt1r/geolocation/surf_type",

    "/gt2l/heights/delta_time",
    "/gt2l/heights/dist_ph_across",
    "/gt2l/heights/dist_ph_along",
    "/gt2l/heights/h_ph",
    "/gt2l/heights/lat_ph",
    "/gt2l/heights/lon_ph",
    "/gt2l/heights/pce_mframe_cnt",
    "/gt2l/heights/ph_id_channel",
    "/gt2l/heights/ph_id_count",
    "/gt2l/heights/ph_id_pulse",
    "/gt2l/heights/quality_ph",
    "/gt2l/heights/signal_conf_ph",
    "/gt2l/geolocation/pitch",
    "/gt2l/geolocation/podppd_flag",
    "/gt2l/geolocation/segment_dist_x",
    "/gt2l/geolocation/segment_id",
    "/gt2l/geolocation/solar_azimuth",
    "/gt2l/geolocation/solar_elevation",
    "/gt2l/geolocation/surf_type",

    "/gt2r/heights/delta_time",
    "/gt2r/heights/dist_ph_across",
    "/gt2r/heights/dist_ph_along",
    "/gt2r/heights/h_ph",
    "/gt2r/heights/lat_ph",
    "/gt2r/heights/lon_ph",
    "/gt2r/heights/pce_mframe_cnt",
    "/gt2r/heights/ph_id_channel",
    "/gt2r/heights/ph_id_count",
    "/gt2r/heights/ph_id_pulse",
    "/gt2r/heights/quality_ph",
    "/gt2r/heights/signal_conf_ph",
    "/gt2r/geolocation/pitch",
    "/gt2r/geolocation/podppd_flag",
    "/gt2r/geolocation/segment_dist_x",
    "/gt2r/geolocation/segment_id",
    "/gt2r/geolocation/solar_azimuth",
    "/gt2r/geolocation/solar_elevation",
    "/gt2r/geolocation/surf_type",

    "/gt3l/heights/delta_time",
    "/gt3l/heights/dist_ph_across",
    "/gt3l/heights/dist_ph_along",
    "/gt3l/heights/h_ph",
    "/gt3l/heights/lat_ph",
    "/gt3l/heights/lon_ph",
    "/gt3l/heights/pce_mframe_cnt",
    "/gt3l/heights/ph_id_channel",
    "/gt3l/heights/ph_id_count",
    "/gt3l/heights/ph_id_pulse",
    "/gt3l/heights/quality_ph",
    "/gt3l/heights/signal_conf_ph",
    "/gt3l/geolocation/pitch",
    "/gt3l/geolocation/podppd_flag",
    "/gt3l/geolocation/segment_dist_x",
    "/gt3l/geolocation/segment_id",
    "/gt3l/geolocation/solar_azimuth",
    "/gt3l/geolocation/solar_elevation",
    "/gt3l/geolocation/surf_type",

    "/gt3r/heights/delta_time",
    "/gt3r/heights/dist_ph_across",
    "/gt3r/heights/dist_ph_along",
    "/gt3r/heights/h_ph",
    "/gt3r/heights/lat_ph",
    # "/gt3r/heights/lon_ph",
    # "/gt3r/heights/pce_mframe_cnt",
    # "/gt3r/heights/ph_id_channel",
    # "/gt3r/heights/ph_id_count",
    # "/gt3r/heights/ph_id_pulse",
    # "/gt3r/heights/quality_ph",
    # "/gt3r/heights/signal_conf_ph",
    # "/gt3r/geolocation/pitch",
    # "/gt3r/geolocation/podppd_flag",
    # "/gt3r/geolocation/segment_dist_x",
    # "/gt3r/geolocation/segment_id",
    # "/gt3r/geolocation/solar_azimuth",
    # "/gt3r/geolocation/solar_elevation",
    # "/gt3r/geolocation/surf_type",
]


HYPERSLICES = [[100, 510]]
HYPERSLICES_2D = [[100, 510], [0, 2]]

# HYPERSLICES = [[100,  71506]]
# HYPERSLICES_2D = [[100, 71506], [0, 2]]

# Specify the datasets that require a 2D hyperslice
DATASETS_REQUIRING_2D = {
   "/gt1l/heights/signal_conf_ph",
   "/gt1r/heights/signal_conf_ph",
   "/gt2l/heights/signal_conf_ph",
   "/gt2r/heights/signal_conf_ph",
   "/gt3l/heights/signal_conf_ph",
   "/gt3r/heights/signal_conf_ph",
   "/gt1l/geolocation/surf_type",
   "/gt1r/geolocation/surf_type",
   "/gt2l/geolocation/surf_type",
   "/gt2r/geolocation/surf_type",
   "/gt3l/geolocation/surf_type",
   "/gt3r/geolocation/surf_type",
}


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
# NOTE: currently asyncronous read is not working - it hangs, git issue #35
# @pytest.mark.parametrize("multiProcess, block", [(False, False), (False, True), (True, False), (True, True)])
@pytest.mark.parametrize("multiProcess, block", [(False, True), (True, True)])
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
            ExpiresIn=3600 # 1 hour
        )

        cls.datasets = [
            {'dataset': path, 'hyperslice': HYPERSLICES_2D if path in DATASETS_REQUIRING_2D else HYPERSLICES}
            for path in DATASET_PATHS
        ]

        cls.datasets_cnt = len(cls.datasets)

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

    def test_dataset_read(self, multiProcess, block):
        """Reads datasets from S3 and local file with multiProcess enabled/disabled, then compares the results."""

        print(f"\nmultiProcess:    {multiProcess}, async: {not block}, {'process' if multiProcess else 'thread'} count: {self.datasets_cnt}")

        # Step 1: Read from the local file
        start_time = time.perf_counter()
        h5obj = h5coro.H5Coro(self.local_file, filedriver.FileDriver, errorChecking=True, multiProcess=multiProcess)
        promise = h5obj.readDatasets(self.datasets, block=block)
        results = {dataset: promise[dataset] for dataset in promise}
        print(f"filedriver read: {time.perf_counter() - start_time:.2f} secs")
        self.compare_results(results)
        results = None  # Must be set to None to avoid shared memory leaks warnings

        # Step 2: Read from S3
        start_time = time.perf_counter()
        h5obj = h5coro.H5Coro(HDF_OBJECT_S3[5:], s3driver.S3Driver, errorChecking=True, multiProcess=multiProcess)
        promise = h5obj.readDatasets(self.datasets, block=block)
        results = {dataset: promise[dataset] for dataset in promise}
        print(f"s3driver read:   {time.perf_counter() - start_time:.2f} secs")
        self.compare_results(results)
        results = None

        # Step 3: Read using WebDriver
        start_time = time.perf_counter()
        h5obj = h5coro.H5Coro(self.pre_signed_url, webdriver.HTTPDriver, errorChecking=True, multiProcess=multiProcess)
        promise = h5obj.readDatasets(self.datasets, block=block)
        results = {dataset: promise[dataset] for dataset in promise}
        print(f"webdriver read:  {time.perf_counter() - start_time:.2f} secs")
        self.compare_results(results)
        results = None
