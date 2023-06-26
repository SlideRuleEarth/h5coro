import logging
import h5coro
from h5coro import filedriver, s3driver

h5coro.config(errorChecking=True, verbose=True, enableAttributes=True, logLevel=logging.INFO)

#h5obj = h5coro.H5Coro("/data/ATLAS/ATL03_20181017222812_02950102_005_01.h5", filedriver.FileDriver, datasets=['/gt2l/heights/h_ph'])
h5obj = h5coro.H5Coro("sliderule/data/ATLAS/ATL03_20181017222812_02950102_005_01.h5", s3driver.S3Driver, datasets=['/gt2l/heights/h_ph'])

print(h5obj.datasets)
