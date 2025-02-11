import time
from test_helpers import *
import h5coro
from h5coro import filedriver

def main():
    """Standalone test for file driver with multiple configurations."""
    local_file = download_hdf_to_local()
    h5py_results = read_with_h5py(local_file)

    # NOTE: currently asynchronous read is not working - it hangs, git issue #35
    # test_configs = [(False, False), (False, True), (True, False), (True, True)]
    test_configs = [(False, True), (True, True)]

    for multiProcess, block in test_configs:
        # Print test configuration

        # Read with h5coro file driver
        print(f"\nmultiProcess: {multiProcess}, async: {not block}, {'process' if multiProcess else 'thread'} count: {len(DATASET_PATHS)}")
        start_time = time.perf_counter()
        h5obj = h5coro.H5Coro(local_file, filedriver.FileDriver, errorChecking=True, multiProcess=multiProcess)
        promise = h5obj.readDatasets(get_datasets(), block=block)
        print(f"read time: {time.perf_counter() - start_time:.2f} secs")

        # Collect results from h5coro
        h5coro_results = {dataset: promise[dataset] for dataset in promise}

        # Compare results
        try:
            compare_results(h5py_results, h5coro_results)
            print("Test passed!")
        except AssertionError as e:
            print(f"Test failed: {e}")

        # Clean up
        h5coro_results = None
        h5py_results = None
        h5obj = None
        promise = None

if __name__ == "__main__":
    main()
