"""Tests for h5 endpoint."""

import pytest
import h5coro
from h5coro import s3driver, webdriver
import earthaccess
import os

auth = earthaccess.login()
credentials = auth.get_s3_credentials(daac="NSIDC")

ATL03_S3_OBJECT = "nsidc-cumulus-prod-protected/ATLAS/ATL03/006/2018/10/17/ATL03_20181017222812_02950102_006_02.h5"
ATL03_HTTP_URL = "https://data.nsidc.earthdatacloud.nasa.gov/nsidc-cumulus-prod-protected/ATLAS/ATL06/006/2018/10/14/ATL06_20181014001049_02350102_006_02.h5"

@pytest.mark.region
class TestIcesat2:

    def test_http_driver(self):
        edl_token = auth.token["access_token"] 
        h5obj = h5coro.H5Coro(ATL03_HTTP_URL, webdriver.HTTPDriver, credentials=edl_token)
        promise = h5obj.readDatasets(['/gt1r/land_ice_segments/h_li'], block=True)
        assert len(promise['/gt1r/land_ice_segments/h_li']) == 3880

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
