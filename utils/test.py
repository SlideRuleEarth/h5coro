import h5coro
from h5coro import s3driver, logger
import earthaccess
import logging

granule = 'nsidc-cumulus-prod-protected/ATLAS/ATL03/005/2018/10/17/ATL03_20181017222812_02950102_005_01.h5'
variable = '/gt2l/heights/signal_conf_ph'
datasets = [ {"dataset": variable,
              "hyperslice": [(0,10)] } ]

logger.config(logLevel=logging.CRITICAL)

auth = earthaccess.login()
s3_creds = auth.get_s3_credentials(daac='NSIDC')

h5obj = h5coro.H5Coro(granule, s3driver.S3Driver, errorChecking=True, verbose=False, credentials=s3_creds, multiProcess=False)
promise = h5obj.readDatasets(datasets, block=True, enableAttributes=False)
for variable in promise:
    print(f'{variable}: {promise[variable][:]}')

