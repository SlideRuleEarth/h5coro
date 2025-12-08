import pytest
import boto3
import numpy as np
import xarray as xr
from h5coro.lazyh5dataset import LazyBackendArray
from .test_helpers import *

def get_backend_lazy_array(array):
    """Unwrap nested xarray wrappers to get to LazyBackendArray."""
    visited = set()
    while hasattr(array, "array") and id(array) not in visited:
        visited.add(id(array))
        array = array.array
    return array

class TestXArrayExtension:
    @classmethod
    def setup_class(cls):
        """Set up the class by downloading the file and reading with h5py."""
        cls.local_file = download_hdf_to_local()
        cls.datasets = get_datasets_for_xarray(use_hyperslice=False)
        cls.datasets_with_slice = get_datasets_for_xarray(use_hyperslice=True)

        cls.group = GROUP_FOR_XARRAY
        cls.pick_variables = VARIABLES_FOR_XARRAY

        # Read with h5coro file driver
        h5obj = h5coro.H5Coro(cls.local_file, filedriver.FileDriver, verbose=True, errorChecking=True, multiProcess=False)
        promise = h5obj.readDatasets(cls.datasets)
        cls.h5coro_results = {dataset: promise[dataset] for dataset in promise}

        # Convert h5coro results to NumPy arrays
        cls.h5coro_results = {dataset: np.array(data) for dataset, data in cls.h5coro_results.items()}
        promise = None

        # Read sliced data
        promise = h5obj.readDatasets(cls.datasets_with_slice)
        cls.h5coro_results_with_slice = {dataset: promise[dataset] for dataset in promise}

        # Convert h5coro results to NumPy arrays
        cls.h5coro_results_with_slice = {dataset: np.array(data) for dataset, data in cls.h5coro_results_with_slice.items()}

        promise = None
        h5obj.close()
        h5obj = None

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
    @pytest.mark.parametrize("multi_process", [
        pytest.param(False, id="Single process"),
        pytest.param(True,  id="Multi process")
    ])
    def test_open_dataset(self, dataset_source, multi_process):
        src = self.get_dataset_source(dataset_source)

        # Open dataset using xarray with h5coro backend
        ds = xr.open_dataset(src,
                             engine="h5coro",
                             group=self.group,
                             col_coords=COL_COORDS_FOR_XARRAY,
                             pick_variables=self.pick_variables,
                             multi_process=multi_process)

        print("\nDataset opened with source:", dataset_source, "multi_process:", multi_process)
        print(ds)


        # Check that all expected variables are present and aligned with 'photon'
        assert "photon" in ds.dims
        for var in VARIABLES_FOR_XARRAY:
            if var in ds:
                assert ds[var].dims[0] == "photon"

        print("All variables are aligned with 'photon' dimension.")

        # Check that all expected coordinates are 1D and aligned with 'photon'
        for coord in ds.coords:
            dims = ds[coord].dims
            assert len(dims) == 1, f"{coord} is not 1D"
            assert dims[0] == "photon", f"{coord} is not aligned with 'photon'"

        print("All coordinates are 1D and aligned with 'photon' dimension.")

        # Check that all expected coordinates are present and only those
        assert set(ds.coords) == set(XARRAY_COORDINATES), f"Expected coords {XARRAY_COORDINATES}, got {list(ds.coords)}"

        # Compute expected data variables (i.e., all variables excluding coords)
        expected_data_vars = [var for var in VARIABLES_FOR_XARRAY if var not in XARRAY_COORDINATES]

        # Check that all expected data variables are present and only those
        assert set(ds.data_vars) == set(expected_data_vars), f"Expected data_vars {expected_data_vars}, got {list(ds.data_vars)}"

        print("All expected data variables are present and only those.")

        # Verify that variables were NOT eagerly read
        for var in ds.data_vars:
            lazyarray = get_backend_lazy_array(ds[var].variable._data)
            assert isinstance(lazyarray, LazyBackendArray), f"{var} is not backed by LazyBackendArray (got {type(lazyarray)})"
            assert not lazyarray.lazy_ds.was_read, f"{var} was eagerly read"

        print("All variables were NOT eagerly read.")

        # Verify that coordinates were read - xarray needs to read and resolve all coordinates data to be able to read variables
        for coord in ds.coords:
            lazyarray = get_backend_lazy_array(ds[var].variable._data)
            assert isinstance(lazyarray, LazyBackendArray), f"{coord} is not backed by LazyBackendArray (got {type(lazyarray)})"
            assert not lazyarray.lazy_ds.was_read, f"{coord} was not read"

        print("All coordinates were read.")

        # Verify that all expected dimensions are present and have the correct size
        for dim, expected_size in XARRAY_EXPECTED_SIZES.items():
            assert dim in ds.sizes, f"Missing dimension: {dim}"
            assert ds.sizes[dim] == expected_size, f"Dimension {dim} size mismatch: expected {expected_size}, got {ds.sizes[dim]}"

        print("All expected dimensions are present and have the correct size.")

        # Verify that all expected variables have the correct shape
        for var, shape in XARRAY_EXPECTED_VARS_SHAPES.items():
            assert var in ds.data_vars, f"Missing variable: {var}"
            assert ds[var].shape == shape, f"Shape mismatch for {var}: expected {shape}, got {ds[var].shape}"

        print("All expected variables have the correct shape.")

        # Verify that all expected variables have the correct shape
        for coord, shape in XARRAY_EXPECTED_COORDS_SHAPES.items():
            assert coord in ds.coords, f"Missing coordinate: {coord}"
            assert ds[coord].shape == shape, f"Shape mismatch for {coord}: expected {shape}, got {ds[coord].shape}"

        print("All expected coordinates have the correct shape.")

        # Use only the variable name inside the selected group
        xarray_results = {entry["dataset"].split("/")[-1]: ds[entry["dataset"].split("/")[-1]].values for entry in self.datasets}
        compare_results(self.h5coro_results, xarray_results, key_mode='short')

        # Clean up
        ds.close()
        print("Dataset closed.")



    # Define a parameterized test for different dataset sources
    @pytest.mark.parametrize("dataset_source", [
        pytest.param("local_file",  id="Local File"),
        pytest.param("s3_object",   id="S3 Object"),
        pytest.param("http_object", id="HTTP Object")
    ])
    def test_open_dataset_with_slice(self, dataset_source):
        src = self.get_dataset_source(dataset_source)

        # Open dataset using xarray with h5coro backend
        ds = xr.open_dataset(src,
                             engine="h5coro",
                             group=self.group,
                             pick_variables=self.pick_variables,
                             col_coords=COL_COORDS_FOR_XARRAY,
                             hyperslices=HYPERSLICES_2D)

        print(ds)

        # Verify that all expected dimensions are present and have the correct size
        for dim, expected_size in SLICED_XARRAY_EXPECTED_SIZES.items():
            assert dim in ds.sizes, f"Missing dimension: {dim}"
            assert ds.sizes[dim] == expected_size, f"Dimension {dim} size mismatch: expected {expected_size}, got {ds.sizes[dim]}"

        # Verify that all expected variables have the correct shape
        for var, shape in SLICED_XARRAY_EXPECTED_VARS_SHAPES.items():
            assert var in ds.data_vars, f"Missing variable: {var}"
            assert ds[var].shape == shape, f"Shape mismatch for {var}: expected {shape}, got {ds[var].shape}"

        # Verify that all expected variables have the correct shape
        for coord, shape in SLICED_XARRAY_EXPECTED_COORDS_SHAPES.items():
            assert coord in ds.coords, f"Missing coordinate: {coord}"
            assert ds[coord].shape == shape, f"Shape mismatch for {coord}: expected {shape}, got {ds[coord].shape}"

        # Use only the variable name inside the selected group
        xarray_results = {entry["dataset"].split("/")[-1]: ds[entry["dataset"].split("/")[-1]].values for entry in self.datasets_with_slice}
        compare_results(self.h5coro_results_with_slice, xarray_results, key_mode='short')

        # Clean up
        xarray_results = None
        ds.close()

    def test_open_dataset_with_slice_of_slice(self):
        src = self.get_dataset_source('s3_object')

        # Open dataset using xarray with h5coro backend
        ds = xr.open_dataset(src,
                             engine="h5coro",
                             group=self.group,
                             col_coords=COL_COORDS_FOR_XARRAY,
                             hyperslices=HYPERSLICES_2D)

        print(ds)

        ds_slice = ds.where((ds.lat_ph >= 20.0) & (ds.lat_ph <= 21.0), drop=True)
        assert 'lat_ph' in ds_slice.coords, "lat_ph coordinate missing after slicing"
        assert ds_slice.sizes['photon'] > 0
        assert float(ds_slice['lat_ph'].min()) >= 20.0, "Minimum lat_ph is below 20.0"
        assert float(ds_slice['lat_ph'].max()) <= 22.5, "Maximum lat_ph is above 22.5"

        # Slice by time
        min_time = float(ds.delta_time.min())
        max_time = float(ds.delta_time.max())

        range_span = max_time - min_time
        quarter = range_span / 4

        window_start = min_time + quarter
        window_end = max_time - quarter

        ds_slice = ds.where((ds.delta_time >= window_start) & (ds.delta_time <= window_end), drop=True)
        assert 'delta_time' in ds_slice.coords, "delta_time coordinate missing after slicing"
        assert ds_slice.sizes['photon'] > 0, "No data points selected in the given time range"

        # Check the range of the selected delta_time values
        min_dt = ds_slice.delta_time.min().item()
        max_dt = ds_slice.delta_time.max().item()
        assert min_dt >= window_start, f"Minimum delta_time too low: {min_dt}, expected >= {window_start}"
        assert max_dt <= window_end, f"Maximum delta_time too high: {max_dt}, expected <= {window_end}"

        # First 100 photon events
        ds_slice = ds.isel(photon=slice(0, 100))
        assert 'lat_ph' in ds_slice.coords, "'lat_ph' coordinate missing after slicing"
        assert ds_slice.sizes['photon'] == 100, f"Expected 100 photon events, got {ds_slice.sizes['lat_ph']}"
        assert ds_slice.lat_ph.dims == ("photon",), "lat_ph should align with photon dimension"

        # validate that the data is actually sliced from the original dataset
        assert np.allclose(ds.lat_ph[:100].values, ds_slice.lat_ph.values), "Sliced lat_ph values do not match original data"

        # Clean up
        ds.close()