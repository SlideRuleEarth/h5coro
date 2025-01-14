import pytest
import h5coro
from h5coro import s3driver

HDF_OBJECT = "sliderule/data/test/ou_process.h5"

@pytest.mark.region
class TestHDF:

    def test_dataset_read(self):
        dataset = "/dataset"
        hyperslice = [[0,1], [0,5]]
        datasets = [{'dataset': dataset, 'hyperslice': hyperslice}]
        h5obj = h5coro.H5Coro(HDF_OBJECT, s3driver.S3Driver, errorChecking=True)
        promise = h5obj.readDatasets(datasets, block=True)
        expected = [ 0.0, 0.00370616, -0.00038263, -0.00219702, 0.01771416]
        for i in range(len(expected)):
            assert abs(promise[dataset][0][i] - expected[i]) < 0.0001
