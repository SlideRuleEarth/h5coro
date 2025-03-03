import pytest
import time
import boto3
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


class TestWebDriver:
    @classmethod
    def setup_class(cls):
        """Set up the class by downloading the file and reading with h5py."""
        cls.local_file = download_hdf_to_local()

    def test_dataset_read(self, use_hyperslice, multiProcess, block):
        h5py_results = read_with_h5py(self.local_file, use_hyperslice)

        s3 = boto3.client("s3", region_name="us-west-2")
        pre_signed_url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": "sliderule", "Key": "data/test/ATL03_20200401184200_00950707_005_01.h5"},
            ExpiresIn=3600 # 1 hour
        )

        # Read with h5coro file driver
        print(f"\nmultiProcess: {multiProcess}, async: {not block}, hyperslice: {get_hyperslice_range(use_hyperslice)}, {'process' if multiProcess else 'thread'} count: {len(DATASET_PATHS)}")
        start_time = time.perf_counter()
        h5obj = h5coro.H5Coro(pre_signed_url, webdriver.HTTPDriver, errorChecking=True, multiProcess=multiProcess)
        promise = h5obj.readDatasets(get_datasets(use_hyperslice), block=block)
        # Collect results from h5coro
        h5coro_results = {dataset: promise[dataset] for dataset in promise}
        print(f"read time:    {time.perf_counter() - start_time:.2f} secs")

        # Compare results
        compare_results(h5py_results, h5coro_results)
        h5coro_results = None   # Must be set to None to avoid shared memory leaks warnings
        h5obj.close()           # Close the session, GC may not free it in time for next run
