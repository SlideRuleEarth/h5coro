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

class TestFileDriver:
    @classmethod
    def setup_class(cls):
        """Set up the class by downloading the file and reading with h5py."""
        cls.local_file = download_hdf_to_local()

    def test_dataset_read(self, use_hyperslice, multiProcess, block):
        h5py_results = read_with_h5py(self.local_file, use_hyperslice)

        # Read with h5coro file driver
        print(f"\nmultiProcess: {multiProcess}, async: {not block}, hyperslice: {get_hyperslice_range(use_hyperslice)}, {'process' if multiProcess else 'thread'} count: {len(DATASET_PATHS)}")
        start_time = time.perf_counter()
        h5obj = h5coro.H5Coro(self.local_file, filedriver.FileDriver, verbose=True, errorChecking=True, multiProcess=multiProcess)
        promise = h5obj.readDatasets(get_datasets(use_hyperslice), block=block)
        # # Collect results from h5coro
        h5coro_results = {dataset: promise[dataset] for dataset in promise}
        print(f"read time:    {time.perf_counter() - start_time:.2f} secs")

        # Compare results
        compare_results(h5py_results, h5coro_results)

        # Clean up
        h5coro_results = None
        h5obj.close()
        h5obj = None


@pytest.mark.parametrize("boundary_chunk", [1, 2, 5])
def test_chunk_boundary_read(boundary_chunk):
    """Regression: a hyperslice starting exactly on a chunk boundary past the first
    chunk used to raise FatalError('invalid location to read chunk')."""
    local_file = download_hdf_to_local()
    dataset_path, chunk_size, num_elements = find_chunked_1d_dataset(local_file)

    start = boundary_chunk * chunk_size
    assert start % chunk_size == 0 and start > 0
    stop = min(start + 10, num_elements)

    expected = read_slice_with_h5py(local_file, dataset_path, start, stop)

    h5obj = h5coro.H5Coro(local_file, filedriver.FileDriver, errorChecking=True)
    promise = h5obj.readDatasets(
        [{'dataset': dataset_path, 'hyperslice': [[start, stop]]}], block=True
    )
    actual = promise[dataset_path]
    np.testing.assert_array_equal(actual, expected)
    promise = None
    h5obj.close()
    h5obj = None
