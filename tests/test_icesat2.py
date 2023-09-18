"""Tests for h5 endpoint."""

import pytest
import h5coro
import os


from h5coro import filedriver, s3driver, webdriver

ATL03_S3_OBJECT = "sliderule/data/ATLAS/ATL03_20181017222812_02950102_005_01.h5"
ATL03_FILE = "/data/ATLAS/ATL03_20181017222812_02950102_005_01.h5"
ATL03_HTTP_URL = "https://data.nsidc.earthdatacloud.nasa.gov/nsidc-cumulus-prod-protected/ATLAS/ATL06/006/2018/10/14/ATL06_20181014001049_02350102_006_02.h5"



@pytest.mark.region
class TestIcesat2:

    def test_http_driver(self):
        edl_token = os.environ.get("EDL_TOKEN")

        h5obj = h5coro.H5Coro(ATL03_HTTP_URL, webdriver.HTTPDriver, credentials=edl_token)
        promise = h5obj.readDatasets(['/gt1r/land_ice_segments/h_li'], block=True)
        assert promise['/gt1r/land_ice_segments/h_li'].elements > 0
        assert promise['/gt1r/land_ice_segments/h_li'].numcols == 1

    def test_s3driver(self):
        h5obj = h5coro.H5Coro(ATL03_S3_OBJECT, s3driver.S3Driver)
        promise = h5obj.readDatasets(['/gt2l/heights/h_ph'])
        assert promise['/gt2l/heights/h_ph'].elements == 20622551
        assert promise['/gt2l/heights/h_ph'].datasize == 82490204
        assert promise['/gt2l/heights/h_ph'].numrows  == 20622551
        assert promise['/gt2l/heights/h_ph'].numcols == 1
        assert abs(promise['/gt2l/heights/h_ph'].values[0] - 2553.0833) < 0.0001

    def test_filedriver(self):
        h5obj = h5coro.H5Coro(ATL03_FILE, filedriver.FileDriver)
        promise = h5obj.readDatasets(['/gt2l/heights/h_ph'])
        assert promise['/gt2l/heights/h_ph'].elements == 20622551
        assert promise['/gt2l/heights/h_ph'].datasize == 82490204
        assert promise['/gt2l/heights/h_ph'].numrows  == 20622551
        assert promise['/gt2l/heights/h_ph'].numcols == 1
        assert abs(promise['/gt2l/heights/h_ph'].values[0] - 2553.0833) < 0.0001

    def test_inspect_variable(self):
        h5obj = h5coro.H5Coro(ATL03_S3_OBJECT, s3driver.S3Driver)
        metadata, attributes = h5obj.inspectVariable('/gt2l/heights/h_ph', w_attr=True)
        assert metadata.dimensions[0] == 20622551
        assert attributes['/gt2l/heights/h_ph/units'].elements == 1
        assert attributes['/gt2l/heights/h_ph/units'].datasize == 7
        assert attributes['/gt2l/heights/h_ph/units'].values == 'meters'
