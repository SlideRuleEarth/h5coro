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

    # To this line we have 10 sets

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

    # To this line we have 20 sets

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

    # To this line we have 30 sets

    # "/gt1r/heights/signal_conf_ph",
    # "/gt1r/geolocation/pitch",
    # "/gt1r/geolocation/podppd_flag",
    # "/gt1r/geolocation/segment_dist_x",
    # "/gt1r/geolocation/segment_id",
    # "/gt1r/geolocation/solar_azimuth",
    # "/gt1r/geolocation/solar_elevation",
    # "/gt1r/geolocation/surf_type",
    # "/gt2l/heights/delta_time",
    # "/gt2l/heights/dist_ph_across",
    # "/gt2l/heights/dist_ph_along",
    # "/gt2l/heights/h_ph",
    # "/gt2l/heights/lat_ph",
    # "/gt2l/heights/lon_ph",
    # "/gt2l/heights/pce_mframe_cnt",
    # "/gt2l/heights/ph_id_channel",
    # "/gt2l/heights/ph_id_count",
    # "/gt2l/heights/ph_id_pulse",
    # "/gt2l/heights/quality_ph",
    # "/gt2l/heights/signal_conf_ph",

    # To this line we have 50 sets

    # "/gt2l/geolocation/pitch",
    # "/gt2l/geolocation/podppd_flag",
    # "/gt2l/geolocation/segment_dist_x",
    # "/gt2l/geolocation/segment_id",
    # "/gt2l/geolocation/solar_azimuth",
    # "/gt2l/geolocation/solar_elevation",
    # "/gt2l/geolocation/surf_type",
    # "/gt2r/heights/delta_time",
    # "/gt2r/heights/dist_ph_across",
    # "/gt2r/heights/dist_ph_along",
    # "/gt2r/heights/h_ph",
    # "/gt2r/heights/lat_ph",
    # "/gt2r/heights/lon_ph",
    # "/gt2r/heights/pce_mframe_cnt",
    # "/gt2r/heights/ph_id_channel",
    # "/gt2r/heights/ph_id_count",
    # "/gt2r/heights/ph_id_pulse",
    # "/gt2r/heights/quality_ph",
    # "/gt2r/heights/signal_conf_ph",
    # "/gt2r/geolocation/pitch",
    # "/gt2r/geolocation/podppd_flag",
    # "/gt2r/geolocation/segment_dist_x",
    # "/gt2r/geolocation/segment_id",
    # "/gt2r/geolocation/solar_azimuth",
    # "/gt2r/geolocation/solar_elevation",
    # "/gt2r/geolocation/surf_type",
    # "/gt3l/heights/delta_time",
    # "/gt3l/heights/dist_ph_across",
    # "/gt3l/heights/dist_ph_along",
    # "/gt3l/heights/h_ph",
    # "/gt3l/heights/lat_ph",
    # "/gt3l/heights/lon_ph",
    # "/gt3l/heights/pce_mframe_cnt",
    # "/gt3l/heights/ph_id_channel",
    # "/gt3l/heights/ph_id_count",
    # "/gt3l/heights/ph_id_pulse",
    # "/gt3l/heights/quality_ph",
    # "/gt3l/heights/signal_conf_ph",
    # "/gt3l/geolocation/pitch",
    # "/gt3l/geolocation/podppd_flag",
    # "/gt3l/geolocation/segment_dist_x",
    # "/gt3l/geolocation/segment_id",
    # "/gt3l/geolocation/solar_azimuth",
    # "/gt3l/geolocation/solar_elevation",
    # "/gt3l/geolocation/surf_type",
    # "/gt3r/heights/delta_time",
    # "/gt3r/heights/dist_ph_across",
    # "/gt3r/heights/dist_ph_along",
    # "/gt3r/heights/h_ph",
    # "/gt3r/heights/lat_ph",

    # To this line we have 100 sets
]

HYPERSLICES = [[110, 1110]]
HYPERSLICES_2D = [[110, 1110], [0, 2]]

# HYPERSLICES = [[0, 70000]]
# HYPERSLICES_2D = [[0, 70000], [0, 5]]

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

GROUP_FOR_XARRAY ='/gt1l/heights'

# Use all varaibles in a group
VARIABLES_FOR_XARRAY = [
    "delta_time",
    "dist_ph_across",
    "dist_ph_along",
    "h_ph",
    "lat_ph",
    "lon_ph",
    "pce_mframe_cnt",
    "ph_id_channel",
    "ph_id_count",
    "ph_id_pulse",
    "quality_ph",
    "signal_conf_ph",
]

XARRAY_COORDINATES = ["delta_time", "lat_ph", "lon_ph"]

COL_COORDS_FOR_XARRAY = {
    # Coordinate variables
    "delta_time": ("photon",),
    "lat_ph":     ("photon",),
    "lon_ph":     ("photon",),

    # Photon-level variables (1D)
    "dist_ph_across": ("photon",),
    "dist_ph_along":  ("photon",),
    "h_ph":           ("photon",),
    "pce_mframe_cnt": ("photon",),
    "ph_id_channel":  ("photon",),
    "ph_id_count":    ("photon",),
    "ph_id_pulse":    ("photon",),
    "quality_ph":     ("photon",),

    # 2D variable
    "signal_conf_ph": ("photon", "surface_type"),
}

XARRAY_EXPECTED_SIZES = {
    "photon": 687003,
    "surface_type": 5,
}

XARRAY_EXPECTED_VARS_SHAPES = {
    "quality_ph":               (687003,),
    "ph_id_pulse":              (687003,),
    "ph_id_channel":            (687003,),
    "ph_id_count":              (687003,),
    "dist_ph_across":           (687003,),
    "h_ph":                     (687003,),
    "pce_mframe_cnt":           (687003,),
    "dist_ph_along":            (687003,),
    "signal_conf_ph":           (687003, 5),
}

XARRAY_EXPECTED_COORDS_SHAPES = {
    "delta_time":               (687003,),
    "lat_ph":                   (687003,),
    "lon_ph":                   (687003,),
}


photon_size = HYPERSLICES_2D[0][1] - HYPERSLICES_2D[0][0]
surface_type_size = HYPERSLICES_2D[1][1] - HYPERSLICES_2D[1][0]

SLICED_XARRAY_EXPECTED_SIZES = {
    "photon": photon_size,
    "surface_type": surface_type_size,
}

SLICED_XARRAY_EXPECTED_VARS_SHAPES = {
    "quality_ph":         (photon_size,),
    "ph_id_pulse":        (photon_size,),
    "ph_id_channel":      (photon_size,),
    "ph_id_count":        (photon_size,),
    "dist_ph_across":     (photon_size,),
    "h_ph":               (photon_size,),
    "pce_mframe_cnt":     (photon_size,),
    "dist_ph_along":      (photon_size,),
    "signal_conf_ph":     (photon_size, surface_type_size),
}

SLICED_XARRAY_EXPECTED_COORDS_SHAPES = {
    "delta_time":         (photon_size,),
    "lat_ph":             (photon_size,),
    "lon_ph":             (photon_size,),
}


DATASET_PATHS_FOR_XARRAY = [
    f"{GROUP_FOR_XARRAY}/{variable}" for variable in VARIABLES_FOR_XARRAY
]


# Get the list of datasets for xarray
def get_datasets_for_xarray(use_hyperslice):
    """Prepare and return the list of datasets with appropriate hyperslices."""
    dataset_list = [
        {'dataset': path, 'hyperslice': HYPERSLICES_2D if path in DATASETS_REQUIRING_2D else HYPERSLICES}
        for path in DATASET_PATHS_FOR_XARRAY
    ] if use_hyperslice else [
        {'dataset': path, 'hyperslice': None}
        for path in DATASET_PATHS_FOR_XARRAY
    ]

    return dataset_list

# Get the list of datasets
def get_datasets(use_hyperslice, datasets_cnt=None):
    """Prepare and return the list of datasets with appropriate hyperslices."""
    dataset_list = [
        {'dataset': path, 'hyperslice': HYPERSLICES_2D if path in DATASETS_REQUIRING_2D else HYPERSLICES}
        for path in DATASET_PATHS
    ] if use_hyperslice else [
        {'dataset': path, 'hyperslice': None}
        for path in DATASET_PATHS
    ]

    return dataset_list[:datasets_cnt] if datasets_cnt is not None else dataset_list

def get_hyperslice_range(use_hyperslice):
    if not use_hyperslice:
        return "all"

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

def read_with_h5py(file_path, use_hyperslice):
    """Read datasets using h5py."""
    results = {}
    with h5py.File(file_path, 'r') as hdf_file:
        for dataset_path in DATASET_PATHS:
            if dataset_path in hdf_file:
                dataset = hdf_file[dataset_path]
                if use_hyperslice:
                    if dataset.ndim == 1:
                        results[dataset_path] = dataset[HYPERSLICES[0][0]:HYPERSLICES[0][1]]
                    elif dataset.ndim == 2:
                        results[dataset_path] = dataset[
                            HYPERSLICES_2D[0][0]:HYPERSLICES_2D[0][1],
                            HYPERSLICES_2D[1][0]:HYPERSLICES_2D[1][1]
                        ]
                else:
                    results[dataset_path] = dataset[:]  # Read entire dataset
    return results

def compare_results(expected_results, actual_results, *, atol=1e-6, use_allclose=True, key_mode='normalized'):
    """
    Compare two dictionaries of dataset arrays.

    Parameters:
    - expected_results: dict mapping dataset path to expected NumPy arrays.
    - actual_results: dict mapping dataset names/paths to actual NumPy arrays.
    - atol: absolute tolerance for np.allclose.
    - use_allclose: whether to use np.allclose (True) or assert_array_equal (False).
    - key_mode: one of 'full', 'normalized', or 'short'
        'full'      => use full dataset path like '/gt1l/heights/delta_time'
        'normalized'=> remove leading slash (default)
        'short'     => only use basename like 'delta_time'
    """
    for dataset_path, expected in expected_results.items():
        if key_mode == 'full':
            actual_key = dataset_path
        elif key_mode == 'normalized':
            actual_key = dataset_path.lstrip('/')
        elif key_mode == 'short':
            actual_key = dataset_path.split('/')[-1]
        else:
            raise ValueError(f"Invalid key_mode: {key_mode}")

        actual = actual_results.get(actual_key)
        if actual is None:
            raise AssertionError(f"Dataset '{actual_key}' not found in actual results")

        if expected.shape != actual.shape:
            raise AssertionError(f"Shape mismatch in '{actual_key}': {expected.shape} vs {actual.shape}")

        if isinstance(expected, np.ndarray) and isinstance(actual, np.ndarray):
            if use_allclose:
                assert np.allclose(expected, actual, atol=atol), f"Data mismatch in '{actual_key}'"
            else:
                np.testing.assert_array_equal(expected, actual, err_msg=f"Mismatch in dataset: {actual_key}")
        else:
            assert expected == actual, f"Value mismatch in '{actual_key}'"



def get_hdf5_group_structure(file_path, group_path, var_list):
    """Extract dataset dimensions and possible coordinates for specific variables in an HDF5 group."""
    dims = {}  # Store dataset shapes (dimensions)
    coords = {}  # Store possible coordinate variables
    vars_info = {}  # Store variable details

    with h5py.File(file_path, "r") as f:
        if group_path not in f:
            raise ValueError(f"Group '{group_path}' not found in the file.")

        group = f[group_path]  # Navigate to the specific group

        for name in var_list:
            if name not in group:
                print(f"Warning: Variable '{name}' not found in group '{group_path}', skipping.")
                continue

            obj = group[name]
            if isinstance(obj, h5py.Dataset):
                # Save dataset shape (used to determine dimensions)
                dims[name] = obj.shape

                # Save dataset metadata
                attrs = dict(obj.attrs)
                vars_info[name] = {"shape": obj.shape, "dtype": obj.dtype, "attrs": attrs}

                # Detect possible coordinate variables
                if any(key in attrs for key in ["coordinates"]):
                    coords[name] = obj[...]

    return dims, coords, vars_info