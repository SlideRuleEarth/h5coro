import pytest
from pathlib import Path

def pytest_addoption(parser):
    parser.addoption("--daac", action="store", default="NSIDC")

@pytest.fixture(scope='session')
def daac(request):
    daac_value = request.config.option.daac
    if daac_value is None:
        pytest.skip()
    return daac_value



def pytest_ignore_collect(collection_path: Path):
    IGNORED_FILES = [
        "test_multiprocess.py", # This test combines all 3 drivers and is redundant with other tests (useful for comparing driver's performance)
    ]
    return collection_path.name in IGNORED_FILES
