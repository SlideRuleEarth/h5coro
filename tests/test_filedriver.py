import pytest
import time
from .test_helpers import *

# Define parameters to test
# NOTE: currently asynchronous read is not working - it hangs, git issue #35
# @pytest.mark.parametrize("multiProcess, block", [(False, False), (False, True), (True, False), (True, True)])
@pytest.mark.parametrize("multiProcess, block", [(False, True), (False, False), (True, True)])
class TestFileDriver:
    @classmethod
    def setup_class(cls):
        """Set up the class by downloading the file and reading with h5py."""
        cls.local_file = download_hdf_to_local()
        cls.h5py_results = read_with_h5py(cls.local_file)

    def test_dataset_read(self, multiProcess, block):
        # Read with h5coro file driver
        print(f"\nmultiProcess: {multiProcess}, async: {not block}, hyperslice_len: {get_hyperslice_range()}, {'process' if multiProcess else 'thread'} count: {len(DATASET_PATHS)}")
        start_time = time.perf_counter()
        h5obj = h5coro.H5Coro(self.local_file, filedriver.FileDriver, verbose=True, errorChecking=True, multiProcess=multiProcess)
        promise = h5obj.readDatasets(get_datasets(), block=block)
        # # Collect results from h5coro
        h5coro_results = {dataset: promise[dataset] for dataset in promise}
        print(f"read time:    {time.perf_counter() - start_time:.2f} secs")

        # Compare results
        compare_results(self.h5py_results, h5coro_results)

        # Clean up
        h5coro_results = None
        h5obj.close()
        h5obj = None
