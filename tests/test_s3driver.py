import pytest
import time
from .test_helpers import *

# Define parameters to test

@pytest.mark.parametrize("use_hyperslice, multiProcess, block", [
    # First pass: use_hyperslice=True, multiProcess=False
    (True, False, True),
    (True, False, False),
    # First pass: use_hyperslice=True, multiProcess=True
    (True, True, True),
    (True, True, False),
    # Second pass: use_hyperslice=False, multiProcess=False
    (False, False, True),
    (False, False, False),
    # Second pass: use_hyperslice=False, multiProcess=True
    (False, True, True),
    (False, True, False)
])

class TestS3Driver:
    @classmethod
    def setup_class(cls):
        """Set up the class by downloading the file and reading with h5py."""
        cls.local_file = download_hdf_to_local()

    def test_dataset_read(self, use_hyperslice, multiProcess, block):
        h5py_results = read_with_h5py(self.local_file, use_hyperslice)

        # Read with h5coro file driver
        print(f"\nmultiProcess: {multiProcess}, async: {not block}, hyperslice: {get_hyperslice_range(use_hyperslice)}, {'process' if multiProcess else 'thread'} count: {len(DATASET_PATHS)}")
        start_time = time.perf_counter()
        h5obj = h5coro.H5Coro(HDF_OBJECT_S3[5:], s3driver.S3Driver, errorChecking=True, multiProcess=multiProcess)
        promise = h5obj.readDatasets(get_datasets(use_hyperslice), block=block)
        # Collect results from h5coro
        h5coro_results = {dataset: promise[dataset] for dataset in promise}
        print(f"read time:    {time.perf_counter() - start_time:.2f} secs")

        # Compare results
        compare_results(h5py_results, h5coro_results)
        h5coro_results = None   # Must be set to None to avoid shared memory leaks warnings
        h5obj.close()           # Close the session, GC may not free it in time for next run

    def test_variable_length_string(self, use_hyperslice, multiProcess, block):

        url = "sliderule/data/test/ATL24_20220826125316_10021606_006_01_001_01.h5"
        exp_sliderule_metadata = (
            '{"beams": ["gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r"], "rgt": 1002, '
            '"environment_version": "v4.9.3-0-gbc060a08", "srt": "dynamic", '
            '"cnf": ["atl03_not_considered", "atl03_background", "atl03_within_10m", '
            '"atl03_low", "atl03_medium", "atl03_high"], "key_space": 0, '
            '"min_dem_delta": -100.0, "generate_ndwi": false, "max_dem_delta": 50.0, '
            '"day": 26, "atl06_fields": [], "sigma_r_max": 5.0, "atl03_corr_fields": [], '
            '"atl13_fields": [], "track": 0, "openoceanspp": {"set_class": false, '
            '"verbose": true, "set_surface": false}, "build_information": "v4.9.3-0-gbc060a08", '
            '"atl08_class": [], "coastnet_version": "5cc4b1b-dirty", "read_timeout": 60000, '
            '"atl08_fields": [], "asset09": "icesat2", "uncertainty": {"asset_kd": "viirsj1-s3"}, '
            '"timeout": 60000, "rqst_timeout": 60000, "yapc": {"min_knn": 5, "win_h": 6.0, '
            '"version": 3, "win_x": 15.0, "knn": 0, "score": 0}, "openoceanspp_version": "3c474b8", '
            '"output": {"open_on_complete": false, "with_validation": false, "format": "h5", '
            '"with_checksum": false, "asset": "sliderule-stage", "region": "us-west-2", '
            '"path": "s3://sliderule-public/ATL24_20220826125316_10021606_006_01_001_01.h5", '
            '"credentials": {}, "as_geo": false, "ancillary": []}, "region": 6, '
            '"coastnet": {"set_class": true, "verbose": true, "model": "coastnet_model-20241111.json", '
            '"set_surface": true}, "resource": "ATL03_20220826125316_10021606_006_01.h5", "month": 8, '
            '"atl03_ph_fields": [], "year": 2022, "refraction": {"use_water_ri_mask": true, '
            '"ri_water": 1.00029, "ri_air": 1.00029}, "use_bathy_mask": true, "cluster_size_hint": 0, '
            '"dist_in_seg": false, "pass_invalid": true, "res": 20.0, "classifiers": ["qtrees", '
            '"coastnet", "openoceanspp", "medianfilter", "cshelph", "bathypathfinder", "ensemble"], '
            '"version": 6, "min_geoid_delta": -100.0, "phoreal": {"use_abs_h": false, '
            '"above_classifier": false, "geoloc": "median", "binsize": 1.0, "send_waveform": false}, '
            '"asset": "icesat2", "max_geoid_delta": 50.0, "proj": "auto", "maxi": 5, "ats": 20.0, '
            '"cnt": 10, "region_mask": {"rows": 0, "cols": 0, "cellsize": 0.0, "latmin": 0.0, '
            '"geojson": "", "lonmax": 0.0, "latmax": 0.0, "lonmin": 0.0}, "len": 40.0, "cycle": 16, '
            '"sliderule_version": "v4.9.3", "surface": {"signal_threshold": 3.0, '
            '"min_peak_separation": 0.5, "highest_peak_ration": 1.2, "model_as_poisoon": true, '
            '"max_bins": 10000, "bin_size": 0.5, "surace_width": 3.0, "max_range": 1000.0}, '
            '"raster": {"rows": 0, "cols": 0, "cellsize": 0.0, "latmin": 0.0, "geojson": "", '
            '"lonmax": 0.0, "latmax": 0.0, "lonmin": 0.0}, "atl03_geo_fields": [], '
            '"spots": [1, 2, 3, 4, 5, 6], "poly": [], "ph_in_extent": 8192, "H_min_win": 3.0, '
            '"quality_ph": ["atl03_quality_nominal"], "qtrees_version": "35833ce-dirty", '
            '"qtrees": {"set_class": false, "verbose": true, "model": "qtrees_model-20241105.json", '
            '"set_surface": false}, "node_timeout": 60000, "samples": {}, "points_in_polygon": 0, '
            '"cshelph": {"version": "f1bbb00"}, "medianfilter": {"version": "4d0b946"}, '
            '"ensemble": {"version": "6eabb00", "model": "/data/ensemble_model-20250201.json"}}'
        )

        datasets = ['metadata/sliderule', 'metadata/stats', 'metadata/profile']

        # print(f"\nmultiProcess: {multiProcess}, async: {not block}, {'process' if multiProcess else 'thread'} count: {len(datasets)}")

        # Add test for git issue #31, metadata variables are not being corectly read as strings
        credentials = {"profile":"default"}
        h5obj = h5coro.H5Coro(url, s3driver.S3Driver, errorChecking=True, verbose=True, credentials=credentials, multiProcess=multiProcess)
        promise = h5obj.readDatasets(datasets, block=block, enableAttributes=False)

        # Extract metadata
        sliderule_metadata = promise["metadata/sliderule"]
        stats_metadata = promise["metadata/stats"]
        profile_metadata = promise["metadata/profile"]

        # Validate types, they must be strings
        assert isinstance(sliderule_metadata, (str, bytes)), f"metadata/sliderule is not a string, got {type(sliderule_metadata)}"
        assert isinstance(stats_metadata, (str, bytes)), f"metadata/stats is not a string, got {type(stats_metadata)}"
        assert isinstance(profile_metadata, (str, bytes)), f"metadata/profile is not a string, got {type(profile_metadata)}"

        # Compare to expected value
        assert sliderule_metadata == exp_sliderule_metadata, f"metadata/sliderule does not match expected value, got {sliderule_metadata}"

        h5obj.close() # Close the session, GC may not free it in time for next run
        sliderule_metadata = None
        stats_metadata = None
        profile_metadata = None
