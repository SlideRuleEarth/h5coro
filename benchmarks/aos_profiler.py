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
logger.config(logLevel=logging.CRITICAL)

# h5py test
def eso_h5py_test(hyperslice, with_s3fs):

    # test parameters
    granule = 'OR_ABI-L1b-RadF-M6C14_G16_s20192950450355_e20192950500042_c20192950500130.nc'
    bucketname = 'eso-west2-curated'
    objectname = 'AOS/PoR/geo/GOES-16-ABI-L1B-FULLD/2019/295/04/' + granule
    objecturl = 's3://' + bucketname + '/' + objectname
    filename = '/data/AOS/' + granule
    variable = '/Rad'

    # remove file first
    try:
        pathlib.Path.unlink(filename)
    except:
        pass

    # read dataset
    start = time.perf_counter()
    if with_s3fs:
        s3 = s3fs.S3FileSystem()
        fp = h5py.File(s3.open(objecturl, 'rb'), mode='r')
    else:
        s3 = boto3.client('s3')
        with open(filename, 'wb') as f:
            s3.download_fileobj(bucketname, objectname, f)
        fp = h5py.File(filename, mode='r')
    duration1 = time.perf_counter() - start
    
    # sum dataset
    start = time.perf_counter()
    total = 0
    if len(hyperslice) > 0:
        for row in fp[variable][hyperslice[0][0]:hyperslice[0][1]]:
            total += sum(row[hyperslice[1][0]:hyperslice[1][1]])
    else:
        for row in fp[variable]:
            total += sum(row)
    duration2 = time.perf_counter() - start

    # return results
    return duration1, duration2, total

# h5coro test
def eso_h5coro_test(hyperslice):

    # test parameters
    granule = 'eso-west2-curated/AOS/PoR/geo/GOES-16-ABI-L1B-FULLD/2019/295/04/OR_ABI-L1b-RadF-M6C14_G16_s20192950450355_e20192950500042_c20192950500130.nc'
    variable = '/Rad'
    datasets = [ {"dataset": variable, "hyperslice": hyperslice } ]
    credentials = {"profile":"default"}

    # read dataset
    start = time.perf_counter()
    h5obj = h5coro.H5Coro(granule, s3driver.S3Driver, errorChecking=True, verbose=False, credentials=credentials, multiProcess=False)
    promise = h5obj.readDatasets(datasets, block=True, enableAttributes=False)
    duration1 = time.perf_counter() - start
    
    # sum dataset
    start = time.perf_counter()
    total = 0
    for row in promise[variable]:
        total += sum(row)
    duration2 = time.perf_counter() - start

    # return results
    return duration1, duration2, total

# h5py - hypersliced - with s3fs
hyperslice=[(0,10), (0,10)]
request_time, sum_time, result = eso_h5py_test(hyperslice, True)
print(f'\ns3fs: {hyperslice}\n======================')
print(f'Result = {result}')
print(f'Opening Time = {request_time:.3f} secs')
print(f'Summing Time = {sum_time:.3f} secs')
print(f'Total Time = {sum_time + request_time:.3f} secs')

# h5py - full dataset - with s3fs
hyperslice=[]
request_time, sum_time, result = eso_h5py_test(hyperslice, True)
print(f'\ns3fs: {hyperslice}\n======================')
print(f'Result = {result}')
print(f'Opening Time = {request_time:.3f} secs')
print(f'Summing Time = {sum_time:.3f} secs')
print(f'Total Time = {sum_time + request_time:.3f} secs')

# h5py - hypersliced - download
hyperslice=[(0,10), (0,10)]
request_time, sum_time, result = eso_h5py_test(hyperslice, False)
print(f'\nh5py: {hyperslice}\n======================')
print(f'Result = {result}')
print(f'Opening Time = {request_time:.3f} secs')
print(f'Summing Time = {sum_time:.3f} secs')
print(f'Total Time = {sum_time + request_time:.3f} secs')

# h5py - full dataset - download
hyperslice=[]
request_time, sum_time, result = eso_h5py_test(hyperslice, True)
print(f'\nh5py: {hyperslice}\n======================')
print(f'Result = {result}')
print(f'Opening Time = {request_time:.3f} secs')
print(f'Summing Time = {sum_time:.3f} secs')
print(f'Total Time = {sum_time + request_time:.3f} secs')

# h5coro - hypersliced
hyperslice=[(0,10), (0,10)]
request_time, sum_time, result = eso_h5coro_test(hyperslice=hyperslice)
print(f'\nh5coro: {hyperslice}\n======================')
print(f'Result = {result}')
print(f'Opening Time = {request_time:.3f} secs')
print(f'Summing Time = {sum_time:.3f} secs')
print(f'Total Time = {sum_time + request_time:.3f} secs')

# h5coro - full dataset
hyperslice=[]
request_time, sum_time, result = eso_h5coro_test(hyperslice=hyperslice)
print(f'\nh5coro: {hyperslice}\n======================')
print(f'Result = {result}')
print(f'Opening Time = {request_time:.3f} secs')
print(f'Summing Time = {sum_time:.3f} secs')
print(f'Total Time = {sum_time + request_time:.3f} secs')
