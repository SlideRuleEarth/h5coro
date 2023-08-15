import h5coro
from h5coro import s3driver
import logging

class args:
    checkErrors = True
    verbose = False
    logLevel = logging.WARNING
    profile = "default"
    granule = "sliderule/data/ATLAS/ATL03_20181017222812_02950102_005_01.h5"
    variables = ["/gt2l/heights/h_ph"]
    driver = s3driver.S3Driver

h5coro.config(errorChecking=args.checkErrors, verbose=args.verbose, logLevel=logging.INFO)

h5obj = h5coro.H5Coro(args.granule, args.driver, block=False)
metadata, attributes = h5obj.inspectVariable('/gt2l/heights/h_ph', w_attr=True)
print("metadata:", metadata)
print("attributes:", attributes)