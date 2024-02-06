import h5coro
from h5coro import s3driver, logger
import earthaccess
import logging

def icesat2_test():
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

def eso_test():
    granule = 'eso-west2-curated/AOS/PoR/geo/GOES-16-ABI-L1B-FULLD/2019/295/04/OR_ABI-L1b-RadF-M6C14_G16_s20192950450355_e20192950500042_c20192950500130.nc'
    variable = '/Rad'
    datasets = [ {"dataset": variable,
                "hyperslice": [(0,10), (0,10)] } ]

    logger.config(logLevel=logging.INFO)

    credentials = {"profile":"default"}

    h5obj = h5coro.H5Coro(granule, s3driver.S3Driver, errorChecking=True, verbose=True, credentials=credentials, multiProcess=False)
    promise = h5obj.readDatasets(datasets, block=True, enableAttributes=False)
    for variable in promise:
        print(f'{variable}: {promise[variable][:]}')

icesat2_test()