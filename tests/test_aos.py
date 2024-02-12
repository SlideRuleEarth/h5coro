import pytest
import h5coro
import s3fs
import h5py
from h5coro import s3driver

GRANULE = 'OR_ABI-L1b-RadF-M6C14_G16_s20192950450355_e20192950500042_c20192950500130.nc'
BUCKET_NAME = 'eso-west2-curated'
OBJECT_NAME = 'AOS/PoR/geo/GOES-16-ABI-L1B-FULLD/2019/295/04/' + GRANULE
OBJECT_URL = 's3://' + BUCKET_NAME + '/' + OBJECT_NAME

@pytest.mark.region
class TestAOS:

    def test_rad_read(self):
        # test parameters
        variable = '/Rad'
        datasets = [ {"dataset": variable, "hyperslice": [[17,18], [2400,2600]]} ]
        credentials = {"profile":"default"}

        # read dataset with h5py
        s3 = s3fs.S3FileSystem()
        fp = h5py.File(s3.open(OBJECT_URL, 'rb'), mode='r')
            
        # read dataset with h5coro
        h5obj = h5coro.H5Coro(BUCKET_NAME + '/' + OBJECT_NAME, s3driver.S3Driver, errorChecking=True, verbose=True, credentials=credentials, multiProcess=False)
        promise = h5obj.readDatasets(datasets, block=True, enableAttributes=False)

        # compare datasets
        i = 0
        for row in fp[variable][17:18]:
            j = 0
            for value in row[2400:2600]:
                assert value == promise[variable][i][j]
                j += 1
            i += 1
