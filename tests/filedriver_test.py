import time
from test_helpers import *

def main():
    """Standalone test for file driver with multiple configurations."""
    local_file = download_hdf_to_local()
    h5py_results = read_with_h5py(local_file)

    # NOTE: currently asynchronous read is not working - it hangs, git issue #35
    # test_configs = [(False, False), (False, True), (True, False), (True, True)]

    test_configs = [(False, True), (True, True)]

    for multiProcess, block in test_configs:
        # Print test configuration
        print(f"\nmultiProcess: {multiProcess}, async: {not block}, hiperslice_len: {get_hyperslice_range()}, {'process' if multiProcess else 'thread'} count: {len(DATASET_PATHS)}")

        # Read with h5coro file driver
        start_time = time.perf_counter()
        h5obj = h5coro.H5Coro(local_file, filedriver.FileDriver, errorChecking=True, multiProcess=multiProcess)
        promise = h5obj.readDatasets(get_datasets(), block=block)
        print(f"read time: {time.perf_counter() - start_time:.2f} secs")

        # Collect results from h5coro
        h5coro_results = {dataset: promise[dataset] for dataset in promise}

        # Compare results
        compare_results(h5py_results, h5coro_results)

        # Clean up
        h5coro_results = None
        h5obj.close()
        h5obj = None

    h5py_results = None

if __name__ == "__main__":
    main()
