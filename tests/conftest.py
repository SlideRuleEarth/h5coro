import pytest
import gc, time, multiprocessing

def pytest_addoption(parser):
    parser.addoption("--daac", action="store", default="NSIDC")

@pytest.fixture(scope='session')
def daac(request):
    daac_value = request.config.option.daac
    if daac_value is None:
        pytest.skip()
    return daac_value



# These tests use main and are not compatible with pytest
IGNORED_FILES = [
    "test_multiprocess.py", # This test combines all 3 drivers and is redundant with other tests (useful for comparing driver's performance)
]

def pytest_ignore_collect(path):
    """
    Prevent pytest from collecting certain files.
    """
    if any(path.basename == filename for filename in IGNORED_FILES):
        return True  # Ignore these files