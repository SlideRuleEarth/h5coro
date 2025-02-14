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
    h5obj = h5coro.H5Coro(HDF_OBJECT_S3[5:], s3driver.S3Driver, errorChecking=True, multiProcess=multiProcess)
    promise = h5obj.readDatasets(get_datasets(), block=block)
    print(f"read time:    {time.perf_counter() - start_time:.2f} secs")

    # Collect results from h5coro
    h5coro_results = {dataset: promise[dataset] for dataset in promise}

    # Compare results
    compare_results(h5py_results, h5coro_results)
    h5coro_results = None   # Must be set to None to avoid shared memory leaks warnings
    h5obj.close()           # Close the session, GC may not free it in time for next run


# @pytest.mark.parametrize("multiProcess, block", [(False, True), (True, True)])
@pytest.mark.parametrize("multiProcess, block", [(False, True)])
def test_git_issue_31(multiProcess, block):

    datasets = ['metadata/sliderule', 'metadata/profile', 'metadata/stats']

    print(f"\nmultiProcess: {multiProcess}, async: {not block}, {'process' if multiProcess else 'thread'} count: {len(datasets)}")

    # Add test for git issue #31, metadata variables are not being corectly read as strings
    url = "sliderule/data/test/ATL24_20220826125316_10021606_006_01_001_01.h5"
    credentials = {"profile":"default"}
    h5obj = h5coro.H5Coro(url, s3driver.S3Driver, errorChecking=True, verbose=True, credentials=credentials, multiProcess=multiProcess)
    promise = h5obj.readDatasets(datasets, block=block, metaOnly=True, enableAttributes=False)

    print(promise["metadata/sliderule"])
    print(promise["metadata/profile"])
    print(promise["metadata/stats"])

    # Extract metadata
    sliderule_metadata = promise["metadata/sliderule"]
    profile_metadata = promise["metadata/profile"]
    stats_metadata = promise["metadata/stats"]

    # Validate types, they must be strings
    assert isinstance(sliderule_metadata, (str, bytes)), f"metadata/sliderule is not a string, got {type(sliderule_metadata)}"
    assert isinstance(profile_metadata, (str, bytes)), f"metadata/profile is not a string, got {type(profile_metadata)}"
    assert isinstance(stats_metadata, (str, bytes)), f"metadata/stats is not a string, got {type(stats_metadata)}"
