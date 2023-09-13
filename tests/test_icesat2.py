"""Tests for h5 endpoint."""

import pytest
import h5coro
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

@pytest.mark.region
class TestIcesat2:
    def test_s3driver(self):
        h5obj = h5coro.H5Coro(ATL03_S3_OBJECT, s3driver.S3Driver, credentials=credentials)
        promise = h5obj.readDatasets(['gt2l/heights/h_ph'], block=True, enableAttributes=False)
        assert len(promise['gt2l/heights/h_ph']) == 20622551
        assert abs(promise['gt2l/heights/h_ph'][0] - 2553.04) < 0.0001

    def test_inspect_variable(self, daac):
        h5obj = h5coro.H5Coro(ATL03_S3_OBJECT, s3driver.S3Driver, credentials=credentials)
        metadata, attributes = h5obj.inspectVariable('gt2l/heights/h_ph', w_attr=True)
        assert metadata.dimensions[0] == 20622551
        assert attributes['units'] == 'meters'

    def test_slash(self):
        h5obj = h5coro.H5Coro(ATL03_S3_OBJECT, s3driver.S3Driver, credentials=credentials)
        promise = h5obj.readDatasets(['gt1l/geolocation/reference_photon_index'], block=True, enableAttributes=False)
        assert promise['gt1l/geolocation/reference_photon_index'][0] == 28
        promise = h5obj.readDatasets(['/gt1l/geolocation/reference_photon_index'], block=True, enableAttributes=False)
        assert promise['/gt1l/geolocation/reference_photon_index'][0] == 28
