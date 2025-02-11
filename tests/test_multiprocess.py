import pytest
import time
import boto3
from .test_helpers import *

@pytest.mark.region
# NOTE: currently asyncronous read is not working - it hangs, git issue #35
# @pytest.mark.parametrize("multiProcess, block", [(False, False), (False, True), (True, False), (True, True)])
@pytest.mark.parametrize("multiProcess, block", [(False, True), (True, True)])
class TestHDF:
    @classmethod
    def setup_class(cls):
        """Set up the class by downloading the file and reading with h5py."""
        cls.local_file = download_hdf_to_local()
        cls.h5py_results = read_with_h5py(cls.local_file)

        # Create a pre-signed URL
        cls.s3 = boto3.client("s3", region_name="us-west-2")
        cls.pre_signed_url = cls.s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": "sliderule", "Key": "data/test/ATL03_20200401184200_00950707_005_01.h5"},
            ExpiresIn=3600 # 1 hour
        )

    def test_dataset_read(self, multiProcess, block):
        """Reads datasets from S3 and local file with multiProcess enabled/disabled, then compares the results."""

        print(f"\nmultiProcess:    {multiProcess}, async: {not block}, hiperslice_len: {get_hyperslice_range()}, {'process' if multiProcess else 'thread'} count: {len(DATASET_PATHS)}")

        # Step 1: Read from the local file
        start_time = time.perf_counter()
        h5obj = h5coro.H5Coro(self.local_file, filedriver.FileDriver, errorChecking=True, multiProcess=multiProcess)
        promise = h5obj.readDatasets(get_datasets(), block=block)
        print(f"filedriver read: {time.perf_counter() - start_time:.2f} secs")
        results = {dataset: promise[dataset] for dataset in promise}
        compare_results(self.h5py_results, results)
        results = None  # Must be set to None to avoid shared memory leaks warnings
        h5obj.close()   # Close the session, GC may not free it in time

        # Step 2: Read from S3
        start_time = time.perf_counter()
        h5obj = h5coro.H5Coro(HDF_OBJECT_S3[5:], s3driver.S3Driver, errorChecking=True, multiProcess=multiProcess)
        promise = h5obj.readDatasets(get_datasets(), block=block)
        print(f"s3driver read:   {time.perf_counter() - start_time:.2f} secs")
        results = {dataset: promise[dataset] for dataset in promise}
        compare_results(self.h5py_results, results)
        results = None  # Must be set to None to avoid shared memory leaks warnings
        h5obj.close()   # Close the session, GC may not free it in time

        # Step 3: Read using WebDriver
        start_time = time.perf_counter()
        h5obj = h5coro.H5Coro(self.pre_signed_url, webdriver.HTTPDriver, errorChecking=True, multiProcess=multiProcess)
        promise = h5obj.readDatasets(get_datasets(), block=block)
        print(f"webdriver read:  {time.perf_counter() - start_time:.2f} secs")
        results = {dataset: promise[dataset] for dataset in promise}
        compare_results(self.h5py_results, results)
        results = None  # Must be set to None to avoid shared memory leaks warnings
        h5obj.close()   # Close the session, GC may not free it in time
