import pytest

def pytest_addoption(parser):
    parser.addoption("--daac", action="store", default="nsidc")

@pytest.fixture(scope='session')
def daac(request):
    daac_value = request.config.option.daac
    if daac_value is None:
        pytest.skip()
    return daac_value

