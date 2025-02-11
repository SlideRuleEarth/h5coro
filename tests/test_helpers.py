import os
import time
import hashlib
import boto3
import h5py
import h5coro
from h5coro import s3driver, filedriver, webdriver
import numpy as np

# Shared constants
HDF_OBJECT_S3 = "s3://sliderule/data/test/ATL03_20200401184200_00950707_005_01.h5"
HDF_OBJECT_LOCAL = "/tmp/ATL03_20200401184200_00950707_005_01.h5"
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

    # To this line we have 50 sets

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

    # To this line we have 100 sets
]


HYPERSLICES = [[110, 610]]
HYPERSLICES_2D = [[110, 610], [0, 2]]

# HYPERSLICES = [[0,  70000]]
# HYPERSLICES_2D = [[0, 70000], [0, 2]]

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

# Get the list of datasets
def get_datasets():
    """Prepare and return the list of datasets with appropriate hyperslices."""
    return [
        {'dataset': path, 'hyperslice': HYPERSLICES_2D if path in DATASETS_REQUIRING_2D else HYPERSLICES}
        for path in DATASET_PATHS
    ]

def get_hyperslice_range():
    start, end = HYPERSLICES[0]
    return end - start

# Common functions
def compute_md5(file_path):
    """Compute the MD5 checksum of a file."""
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as file:
        for chunk in iter(lambda: file.read(4096), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()

def validate_file_md5(file_path, expected_checksum):
    """Validate the MD5 checksum of the file."""
    computed_checksum = compute_md5(file_path)
    if computed_checksum != expected_checksum:
        raise ValueError(f"Checksum mismatch! Expected: {expected_checksum}, Got: {computed_checksum}")
    print(f"MD5 checksum valid for {file_path}")

def download_hdf_to_local():
    """Download the HDF file locally if it doesn't exist."""
    if os.path.exists(HDF_OBJECT_LOCAL):
        return HDF_OBJECT_LOCAL

    print("\nDownloading HDF object to local temp directory...")
    s3_url_parts = HDF_OBJECT_S3.replace("s3://", "").split("/", 1)
    bucket_name, key = s3_url_parts[0], s3_url_parts[1]

    s3 = boto3.client("s3")
    s3.download_file(bucket_name, key, HDF_OBJECT_LOCAL)
    validate_file_md5(HDF_OBJECT_LOCAL, HDF_OBJECT_MD5SUM)

    return HDF_OBJECT_LOCAL

def read_with_h5py(file_path):
    """Read datasets using h5py."""
    results = {}
    with h5py.File(file_path, 'r') as hdf_file:
        for dataset_path in DATASET_PATHS:
            if dataset_path in hdf_file:
                dataset = hdf_file[dataset_path]
                if dataset.ndim == 1:
                    results[dataset_path] = dataset[HYPERSLICES[0][0]:HYPERSLICES[0][1]]
                elif dataset.ndim == 2:
                    results[dataset_path] = dataset[
                        HYPERSLICES_2D[0][0]:HYPERSLICES_2D[0][1],
                        HYPERSLICES_2D[1][0]:HYPERSLICES_2D[1][1]
                    ]
    return results

def compare_results(h5py_results, h5coro_results):
    """Compare h5py results to h5coro results."""
    for dataset in h5py_results:
        expected_data = h5py_results[dataset]
        normalized_dataset = dataset.lstrip('/')
        actual_data = h5coro_results.get(normalized_dataset)

        if actual_data is None:
            raise AssertionError(f"Dataset '{dataset}' not found in h5coro results")

        if isinstance(expected_data, np.ndarray) and isinstance(actual_data, np.ndarray):
            np.testing.assert_array_equal(expected_data, actual_data, err_msg=f"Mismatch in dataset: {dataset}")
        else:
            assert expected_data == actual_data, f"Mismatch in dataset: {dataset}"
