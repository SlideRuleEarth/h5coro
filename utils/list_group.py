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
h5obj = h5coro.H5Coro(args.granule, args.driver, datasets=args.variables, block=False)
h_ph = h5obj['/gt2l/heights/h_ph']
print("heights", h_ph)
print("groups", h5obj.listGroup('/gt2l/heights'))