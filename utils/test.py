# imports
import time
import pathlib
import s3fs
import h5py
import boto3
import h5coro
from h5coro import s3driver, logger

# configure h5coro
import logging
logger.config(logLevel=logging.INFO)

# test parameters
granule = 'OR_ABI-L1b-RadF-M6C14_G16_s20192950450355_e20192950500042_c20192950500130.nc'
bucketname = 'eso-west2-curated'
objectname = 'AOS/PoR/geo/GOES-16-ABI-L1B-FULLD/2019/295/04/' + granule
objecturl = 's3://' + bucketname + '/' + objectname
variable = '/Rad'
datasets = [ {"dataset": variable, "hyperslice": [[17,18], [2500,2600]]} ]
credentials = {"profile":"default"}

# read dataset
#s3 = s3fs.S3FileSystem()
#fp = h5py.File(s3.open(objecturl, 'rb'), mode='r')
    
# read dataset
h5obj = h5coro.H5Coro(bucketname + '/' + objectname, s3driver.S3Driver, errorChecking=True, verbose=True, credentials=credentials, multiProcess=False)
promise = h5obj.readDatasets(datasets, block=True, enableAttributes=False)

# compare datasets
#for row in range(len(fp[variable])):
#    print(f'Checking row: {row}')
#    for column in range(len(fp[variable][row])):
#        if fp[variable][row][column] != promise[variable][row][column]:
#            print(f'====> Mismatch at {row},{column}: {fp[variable][row][column]} != {promise[variable][row][column]}')

for row in promise[variable]:
    for i in range(0, len(row), 10):
        print(i, row[i:i+10])