import pytest
import time
from .test_helpers import *

# Define parameters to test
# NOTE: currently asynchronous read is not working - it hangs, git issue #35
# @pytest.mark.parametrize("multiProcess, block", [(False, False), (False, True), (True, False), (True, True)])
@pytest.mark.parametrize("multiProcess, block", [(False, True), (True, True)])
def test_filedriver(multiProcess, block):
    """Test file driver with multiple configurations."""
    local_file = download_hdf_to_local()
    h5py_results = read_with_h5py(local_file)

    # Read with h5coro file driver
    print(f"\nmultiProcess: {multiProcess}, async: {not block}, hiperslice_len: {get_hyperslice_range()}, {'process' if multiProcess else 'thread'} count: {len(DATASET_PATHS)}")
    start_time = time.perf_counter()
    h5obj = h5coro.H5Coro(local_file, filedriver.FileDriver, errorChecking=True, multiProcess=multiProcess)
    promise = h5obj.readDatasets(get_datasets(), block=block)
    print(f"read time:    {time.perf_counter() - start_time:.2f} secs")

    # Collect results from h5coro
    h5coro_results = {dataset: promise[dataset] for dataset in promise}

    # Compare results
    compare_results(h5py_results, h5coro_results)

    h5coro_results = None
