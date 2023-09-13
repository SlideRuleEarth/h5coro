"""Tests for h5 endpoint."""

import pytest
import h5coro
from h5coro import filedriver, s3driver
import earthaccess

auth = earthaccess.login()
s3_creds = auth.get_s3_credentials(daac="NSIDC")
credentials = {
    "aws_access_key_id": s3_creds["accessKeyId"],
    "aws_secret_access_key": s3_creds["secretAccessKey"],
    "aws_session_token": s3_creds["sessionToken"] 
}

ATL03_S3_OBJECT = "nsidc-cumulus-prod-protected/ATLAS/ATL03/006/2018/10/17/ATL03_20181017222812_02950102_006_02.h5"
#ATL03_FILE = "/data/ATLAS/ATL03_20181017222812_02950102_005_01.h5"

@pytest.mark.region
class TestIcesat2:
    def test_s3driver(self):
        h5obj = h5coro.H5Coro(ATL03_S3_OBJECT, s3driver.S3Driver, credentials=credentials)
        promise = h5obj.readDatasets(['gt2l/heights/h_ph'], block=True, enableAttributes=False)
        assert promise['gt2l/heights/h_ph'].elements == 20622551
        assert promise['gt2l/heights/h_ph'].datasize == 82490204
        assert promise['gt2l/heights/h_ph'].numrows  == 20622551
        assert promise['gt2l/heights/h_ph'].numcols == 1
        assert abs(promise['gt2l/heights/h_ph'].values[0] - 2553.04) < 0.0001

#    def test_filedriver(self):
#        h5obj = h5coro.H5Coro(ATL03_FILE, filedriver.FileDriver)
#        promise = h5obj.readDatasets(['/gt2l/heights/h_ph'])
#        assert promise['/gt2l/heights/h_ph'].elements == 20622551
#        assert promise['/gt2l/heights/h_ph'].datasize == 82490204
#        assert promise['/gt2l/heights/h_ph'].numrows  == 20622551
#        assert promise['/gt2l/heights/h_ph'].numcols == 1
#        assert abs(promise['/gt2l/heights/h_ph'].values[0] - 2553.0833) < 0.0001

    def test_inspect_variable(self, daac):
        h5obj = h5coro.H5Coro(ATL03_S3_OBJECT, s3driver.S3Driver, credentials=credentials)
        metadata, attributes = h5obj.inspectVariable('/gt2l/heights/h_ph', w_attr=True)
        assert metadata.dimensions[0] == 20622551
        assert attributes['/gt2l/heights/h_ph/units'].elements == 1
        assert attributes['/gt2l/heights/h_ph/units'].datasize == 7
        assert attributes['/gt2l/heights/h_ph/units'].values == 'meters'
