import pytest
import time
import boto3
from .test_helpers import *


# Define parameters to test
# NOTE: currently asynchronous read is not working - it hangs, git issue #35
# @pytest.mark.parametrize("multiProcess, block", [(False, False), (False, True), (True, False), (True, True)])

@pytest.mark.parametrize("multiProcess, block", [(False, True), (True, True)])
def test_filedriver(multiProcess, block):
    """Test file driver with multiple configurations."""
    local_file = download_hdf_to_local()
    h5py_results = read_with_h5py(local_file)

    s3 = boto3.client("s3", region_name="us-west-2")
    pre_signed_url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": "sliderule", "Key": "data/test/ATL03_20200401184200_00950707_005_01.h5"},
        ExpiresIn=3600 # 1 hour
    )

    # Read with h5coro file driver
    print(f"\nmultiProcess: {multiProcess}, async: {not block}, hiperslice_len: {get_hyperslice_range()}, {'process' if multiProcess else 'thread'} count: {len(DATASET_PATHS)}")
    start_time = time.perf_counter()
    h5obj = h5coro.H5Coro(pre_signed_url, webdriver.HTTPDriver, errorChecking=True, multiProcess=multiProcess)
    promise = h5obj.readDatasets(get_datasets(), block=block)
    print(f"read time:    {time.perf_counter() - start_time:.2f} secs")

    # Collect results from h5coro
    h5coro_results = {dataset: promise[dataset] for dataset in promise}

    # Compare results
    compare_results(h5py_results, h5coro_results)
    h5coro_results = None   # Must be set to None to avoid shared memory leaks warnings
    h5obj.close()           # Close the session, GC may not free it in time for next run
