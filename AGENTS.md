# AGENTS.md

Pure-Python, read-only implementation of a subset of the HDF5 spec, optimized for
concurrent range-get reads from S3. Not a general HDF5 library — only the format
elements listed in README.md's compatibility table are supported.

## Layout

- `h5coro/` — the package.
  - `h5coro.py` — `H5Coro` class: file open, cache-line block cache, `readDatasets`, `inspectPath`, `list`.
  - `h5dataset.py` — the actual HDF5 binary parsing (superblock, b-trees, messages, chunk decode, shuffle/deflate). Most spec work lives here. Defines `FatalError`.
  - `h5promise.py` — async promise returned by `readDatasets`; also `massagePath` (path normalization).
  - `h5metadata.py`, `h5view.py`, `lazyh5dataset.py` — metadata objects and lazy access.
  - Drivers (one per backend, note module vs class name mismatch):
    - `s3driver.py` → class `S3Driver`
    - `webdriver.py` → class `HTTPDriver` (not `WebDriver`)
    - `filedriver.py` → class `FileDriver`
  - `backends/xarray_h5coro.py` — xarray engine, registered via `setup.py` entry point `xarray.backends`.
  - `datasets/icesat2.py` — dataset-specific helpers.
- `tests/`, `utils/`, `benchmarks/` — see below.

## Versioning & release

- Single source of truth is `version.txt`. `h5coro/version.py` reads it via
  `importlib.metadata.metadata("h5coro")`, so `h5coro.__version__` only resolves if
  the package is **pip-installed** (`pip install -e .`); running from a bare checkout
  raises `PackageNotFoundError`.
- Release: `./RELEASE.sh <x.y.z>` — commits version.txt, tags, pushes, `gh release create`.
  A GitHub release triggers `.github/workflows/python-publish.yml` (twine → PyPI).

## Running tests

Almost all tests hit the network and need real credentials — they are integration
tests, not unit tests:

- Nearly every test downloads a real ATL03 granule from the `sliderule` S3 bucket to
  `/tmp` (`tests/test_helpers.py:download_hdf_to_local`) and compares h5coro output
  against `h5py`. Requires AWS credentials, even for `test_filedriver.py`.
- `tests/test_icesat2.py` calls `earthaccess.login()` **at import time** — the module
  fails to collect without valid NASA Earthdata (EDL) credentials.
- `@pytest.mark.region` marks tests needing in-region (AWS us-west-2) datacenter access.
- `tests/test_multiprocess.py` is intentionally skipped via `conftest.py`
  (`pytest_ignore_collect`); it's a redundant driver-comparison benchmark.
- Custom option `--daac` (default `NSIDC`) exists in `conftest.py`.

Only **`tests/test_shuffle.py`** runs fully offline (imports `h5dataset` directly, no
network). Use it to sanity-check the shuffle-filter codec:

```bash
pytest tests/test_shuffle.py
```

`tests/loop_test.sh <test_file> [count]` re-runs a test in a loop (used to catch
multiprocess/fork flakiness); it force-kills lingering pytest/multiprocessing procs.

## Runtime deps & Python quirks

- Runtime deps: `numpy`, `boto3`, `earthaccess`, `requests`, `xarray` (see
  `requirements.txt`). `h5py` is a **test-only** ground-truth dependency, never a
  runtime import in `h5coro/`.
- `python_requires>=3.10`.
- `multiProcess=True` is **auto-disabled on Python >= 3.13** (fork-after-threads
  deadlock, see `h5coro.py:__init__`). `environment.yml` deliberately pins
  `python<=3.12.0` for the same fork-behavior reason. Don't "fix" these pins.

## Conventions / gotchas

- API takes full dataset paths only (e.g. `/gt1l/heights/h_ph`); h5coro does not read
  at the group level via `readDatasets`. The xarray backend requires a `group=` kwarg.
- `H5Coro` is a context manager; tests call `h5obj.close()` and null out result vars to
  avoid shared-memory leak warnings in multiprocess mode — preserve that pattern.
- Origin/parent project is SlideRule's C++ HDF5 reader; behavior is meant to track it.
  When editing `h5dataset.py`'s `shuffleChunk`, keep it byte-identical to
  `reference_shuffle` in `tests/test_shuffle.py`.
