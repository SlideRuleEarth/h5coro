"""Tests for h5 endpoint."""

import pytest
import h5coro
from h5coro import filedriver, s3driver

ATL03_S3_OBJECT = "sliderule/data/ATLAS/ATL03_20181017222812_02950102_005_01.h5"
ATL03_FILE = "/data/ATLAS/ATL03_20181017222812_02950102_005_01.h5"

@pytest.mark.region
class TestIcesat2:
    def test_s3driver(self):
        h5obj = h5coro.H5Coro(ATL03_S3_OBJECT, s3driver.S3Driver, datasets=['/gt2l/heights/h_ph'])
        assert h5obj['/gt2l/heights/h_ph'].elements == 20622551
        assert h5obj['/gt2l/heights/h_ph'].datasize == 82490204
        assert h5obj['/gt2l/heights/h_ph'].numrows  == 20622551
        assert h5obj['/gt2l/heights/h_ph'].numcols == 1
        assert abs(h5obj['/gt2l/heights/h_ph'].values[0] - 2553.0833) < 0.0001

    def test_filedriver(self):
        h5obj = h5coro.H5Coro(ATL03_FILE, filedriver.FileDriver, datasets=['/gt2l/heights/h_ph'])
        assert h5obj['/gt2l/heights/h_ph'].elements == 20622551
        assert h5obj['/gt2l/heights/h_ph'].datasize == 82490204
        assert h5obj['/gt2l/heights/h_ph'].numrows  == 20622551
        assert h5obj['/gt2l/heights/h_ph'].numcols == 1
        assert abs(h5obj['/gt2l/heights/h_ph'].values[0] - 2553.0833) < 0.0001
