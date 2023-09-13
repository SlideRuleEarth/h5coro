
import h5coro
import logging
from h5coro import s3driver
import earthaccess

auth = earthaccess.login()
s3_creds = auth.get_s3_credentials(daac="NSIDC")
credentials = {
    "aws_access_key_id": s3_creds["accessKeyId"],
    "aws_secret_access_key": s3_creds["secretAccessKey"],
    "aws_session_token": s3_creds["sessionToken"] 
}

ATL03_S3_OBJECT = "nsidc-cumulus-prod-protected/ATLAS/ATL03/006/2018/10/17/ATL03_20181017222812_02950102_006_02.h5"

h5coro.config(logLevel=logging.INFO)
h5obj = h5coro.H5Coro(ATL03_S3_OBJECT, s3driver.S3Driver, verbose=True, credentials=credentials)

metadata, attributes = h5obj.inspectVariable('/gt2l/heights/h_ph', w_attr=True)

print(metadata)