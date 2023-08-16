import h5coro
from h5coro import s3driver
import logging

class args:
    checkErrors = True
    verbose = False
    logLevel = logging.ERROR
    granule = "sliderule/data/ATLAS/ATL03_20181017222812_02950102_005_01.h5"
    driver = s3driver.S3Driver

h5coro.config(logLevel=args.logLevel)
h5obj = h5coro.H5Coro(args.granule, args.driver, errorChecking=args.checkErrors, verbose=args.verbose)

variables, attributes = h5obj.listGroup('/gt2l/heights', w_attr=True, w_inspect=True)
for variable, results in variables.items():
    print(variable)
    print(f'  metadata: {results["metadata"]}')
    for attribute, value in results["attributes"].items():
        value_str = "<unsupported>"
        if value != None:
            value_str = f'{value.values}'
        print(f'  {attribute}: {value_str}')
    