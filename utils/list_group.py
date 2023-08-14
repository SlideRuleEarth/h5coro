import h5coro
from h5coro import s3driver
import logging

class args:
    checkErrors = True
    verbose = False
    logLevel = logging.WARNING
    profile = "default"
    granule = "sliderule/data/ATLAS/ATL03_20181017222812_02950102_005_01.h5"
    driver = s3driver.S3Driver

h5coro.config(errorChecking=args.checkErrors, verbose=args.verbose, logLevel=logging.INFO)
h5obj = h5coro.H5Coro(args.granule, args.driver, block=False)

metadata, attributes = h5obj.inspectVariable('/gt2l/heights/h_ph', w_attr=True)
print("metadata:", metadata)
print("attributes:", attributes)
#print("dimensions:", metadata.dimensions)
#print("coordinates:", attributes["/gt2l/heights/h_ph/coordinates"].values)

variables, attributes = h5obj.listGroup('/gt2l/heights', w_attr=True, w_inspect=True)
print('variables', variables)
#print("dimensions:", variables['/gt2l/heights/h_ph']['metadata'].dimensions)
#print("coordinates:", variables['/gt2l/heights/h_ph']['attributes']["/gt2l/heights/h_ph/coordinates"].values)
