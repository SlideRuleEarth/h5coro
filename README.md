# h5coro

**A cloud optimized Python package for reading HDF5 data stored in S3**

## Origin and Purpose

**h5coro** is a pure Python implementation of a subset of the HDF5 specification that has been optimized for reading data out of S3.  The project has its roots in the development of an on-demand science data processing system called [SlideRule](https://github.com/SlideRuleEarth/sliderule), where a new C++ implementation of the HDF5 specification was developed for performant read access to Earth science datasets stored in AWS S3.  Over time, user's of SlideRule began requesting the ability to performantly read HDF5 and NetCDF files out of S3 from their own Python scripts.  The result is **h5coro**: the re-implementation in Python of the core HDF5 reading logic that exists in SlideRule.  Since then, **h5coro** has become its own project, which will continue to grow and diverge in functionality from its parent implementation.  For more information on SlideRule and the organization behind **h5coro**, see https://slideruleearth.io.

**h5coro** is optimized for reading HDF5 data in high-latency high-throughput environments.  It accomplishes this through a few key design decisions:
* __All reads are concurrent.__  Each dataset and/or attribute read by **h5coro** is performed in its own thread.
* __Intelligent range gets__ are used to read as many dataset chunks as possible in each read operation.  This drastically reduces the number of HTTP requests to S3 and means there is no longer a need to re-chunk the data (it actually works better on smaller chunk sizes due to the granularity of the request).
* __Block caching__ is used to minimize the number of GET requests made to S3.  S3 has a large first-byte latency (we've measured it at ~60ms on our systems), which means there is a large penalty for each read operation performed.  **h5coro** performs all reads to S3 as large block reads and then maintains data in a local cache for access to smaller amounts of data within those blocks.
* __The system is serverless__ and does not depend on any external services to read the data. This means it scales naturally as the user application scales, and it reduces overall system complexity.
* __No metadata repository is needed.__  The structure of the file are cached as they are read so that successive reads to other datasets in the same file will not have to re-read and re-build the directory structure of the file.

## Limitations

For a full list of which parts of the HDF5 specification **h5coro** implements, see the [compatibility](#compatibility) section at the end of this readme.  The major limitations currently present in the package are:
* The code only implements a subset of the HDF5 specification.  **h5coro** has been shown to work on a number of different datasets, but depending on the version of the HDF5 C library used to write the file, and what options were used during its creation, it is very possible that some part of **h5coro** will need to be updated to support reading it.  Hopefully, over time as more of the spec is implemented, this will become less of a problem.
* It is a read-only library and has no functionality to write HDF5 data.

## Installation

The simplest way to install **h5coro** is by using the [conda](https://docs.conda.io/projects/conda/en/latest/user-guide/index.html) package manager.
```bash
    conda install -c conda-forge h5coro
```
Alternatively, you can also install h5coro using [pip](https://pip.pypa.io/en/stable/).
```bash
    pip install h5coro
```
#### xarray backend

To use `h5coro` as a backend to xarray, simply install both
`xarray` and `h5coro` in your current environment.
`h5coro` will automatically be recognized by `xarray`,
so you can use it like any other [xarray engine](https://docs.xarray.dev/en/stable/internals/how-to-add-new-backend.html#how-to-add-a-new-backend):

```python
import xarray as xr
h5ds = xr.open_dataset("file.h5", engine="h5coro")
```

You can see what backends are available in xarray using:
```python
xr.backends.list_engines()
```


## Example Usage

```python
# (1) import
from h5coro import h5coro, s3driver

# (2) create
h5obj = h5coro.H5Coro(f'{my_bucket}/{path_to_hdf5_file}', s3driver.S3Driver)

# (3) read
datasets = [{'dataset': '/path/to/dataset1', 'hyperslice': []},
            {'dataset': '/path/to/dataset2', 'hyperslice': [324, 374]}]
promise = h5obj.readDatasets(datasets=datasets, block=True)

# (4) display
for variable in promise:
    print(f'{variable}: {promise[variable]}')
```

#### (1) Importing h5coro
`h5coro`: the main module implementing the HDF5 reader object

`s3driver`: the driver used to read HDF5 data from S3

#### (2) Create h5coro Object
The call to `h5coro.H5Coro` creates a reader object that opens up the HDF5 file, reads the start of the file, and is then ready to accept read requests.

The calling application must have credentials to access the object in the specified S3 bucket.  **h5coro** uses `boto3`, so any credentials supplied via the standard AWS methods will work.  If credentials need to be supplied externally, then in the call to `h5coro.H5Coro` pass in an argument `credentials` as a dictionary with the following three fields: "aws_access_key_id", "aws_secret_access_key", "aws_session_token".

#### (3) Read with h5coro Object
The `H5Coro.read` function takes a list of dictionary objects that describe the datasets that need to be read in parallel.

If the `block` parameter is set to True, then the code will wait for all of the datasets to be read before returning; otherwise, the code will return immediately and not until the dataset within the reader object is access will the code block.

#### (4) Display the Datasets
The h5coro promise is a dictionary of `numpy` arrays containing the values of the variables read, along with some additional logic that provides the ability to block while waiting for the data to be populated.

## Licensing

**h5coro** is licensed under the 3-clause BSD license found in the LICENSE file at the root of this source tree.

## Contribute

We welcome and invite contributions from anyone at any career stage and with any amount of coding experience towards the development of **h5coro**. We appreciate any and all contributions made towards the development of the project. You will be recognized for your work by being listed as one of the project contributors.

#### Ways to Contribute
* Fixing typographical or coding errors
* Submitting bug reports or feature requests through the use of GitHub issues
* Improving documentation and testing
* Sharing use cases and examples (such as Jupyter Notebooks)
* Providing code for everyone to use

#### Requesting a Feature
Check the project issues tab to see if the feature has already been suggested. If not, please submit a new issue describing your requested feature or enhancement. Please give your feature request both a clear title and description. Please let us know in your description if this is something you would like to contribute to the project.

#### Reporting a Bug
Check the project issues tab to see if the problem has already been reported. If not, please submit a new issue so that we are made aware of the problem. Please provide as much detail as possible when writing the description of your bug report. Providing detailed information and examples will help us resolve issues faster.

#### Contributing Code or Examples
We follow a standard Forking Workflow for code changes and additions. Submitted code goes through a review and comment process by the project maintainers.

#### General Guidelines
* Make each pull request as small and simple as possible
* Commit messages should be clear and describe the changes
* Larger changes should be broken down into their basic components and integrated separately
* Bug fixes should be their own pull requests with an associated GitHub issue
* Write a descriptive pull request message with a clear title
* Please be patient as reviews of pull requests can take time

#### Steps to Contribute
* Fork the repository to your personal GitHub account by clicking the “Fork” button on the project main page. This creates your own server-side copy of the repository.
* Either by cloning to your local system or working in GitHub Codespaces, create a work environment to make your changes.
* Add the original project repository as the upstream remote. While this step isn’t a necessary, it allows you to keep your fork up to date in the future.
* Create a new branch to do your work.
* Make your changes on the new branch.
* Push your work to GitHub under your fork of the project.
* Submit a Pull Request from your forked branch to the project repository.

## Compatibility

| Format Element | Supported | Contains | Missing |
|:--------------:|:---------:|:--------:|:-------:|
| ___Field Sizes___ | <span style="color:green">Yes</span> | 1, 2, 4, 8, bytes | |
| ___Superblock___   | <span style="color:blue">Partial</span> | Version 0, 2 | Version 1, 3 |
| ___Base Address___   | <span style="color:green">Yes</span> | | |
| ___B-Tree___  | <span style="color:blue">Partial</span> | Version 1 | Version 2 |
| ___Group Symbol Table___  | <span style="color:green">Yes</span> | Version 1 | |
| ___Local Heap___  | <span style="color:green">Yes</span> | Version 0 |
| ___Global Heap___  | <span style="color:red">No</span> | | Version 1 |
| ___Fractal Heap___ | <span style="color:green">Yes</span> | Version 0 | |
| ___Shared Object Header Message Table___ | <span style="color:red">No | | Version 0 |
| ___Data Object Headers___  | <span style="color:green">Yes</span> | Version 1, 2 | |
| ___Shared Message___  | <span style="color:red">No</span> | | Version 1 |
| ___NIL Message___  | <span style="color:green">Yes</span> | Unversioned | |
| ___Dataspace Message___  | <span style="color:green">Yes</span> | Version 1 | |
| ___Link Info Message___  | <span style="color:green">Yes</span> | Version 0 | |
| ___Datatype Message___  | <span style="color:blue">Partial</span> | Version 1 | Version 0, 2, 3 |
| ___Fill Value (Old) Message___  | <span style="color:red">No</span> | | Unversioned |
| ___Fill Value Message___  | <span style="color:blue">Partial</span> | Version 2, 3 | Version 1 |
| ___Link Message___  | <span style="color:green">Yes</span> | Version 1 |
| ___External Data Files Message___  | <span style="color:red">No</span> | | Version 1 |
| ___Data Layout Message___  | <span style="color:blue">Partial</span> | Version 3 | Version 1, 2 |
| ___Bogus Message___  | <span style="color:red">No</span> | | Unversioned |
| ___Group Info Message___  | <span style="color:red">No</span> | | Version 0 |
| ___Filter Pipeline Message___  | <span style="color:green">Yes</span> | Version 1, 2 | |
| ___Attribute Message___  | <span style="color:blue">Partial</span> | Version 1, 2, 3 | Shared message support for v3 |
| ___Object Comment Message___  | <span style="color:red">No</span> | | Unversioned |
| ___Object Modification Time (Old) Message___  | <span style="color:red">No</span> | | Unversioned |
| ___Shared Message Table Message___  | <span style="color:red">No</span> | | Version 0 |
| ___Object Header Continuation Message___  | <span style="color:green">Yes</span> | Version 1, 2 | |
| ___Symbol Table Message___  | <span style="color:green">Yes</span> | Unversioned | |
| ___Object Modification Time Message___  | <span style="color:red">No</span> | | Version 1 |
| ___B-Tree ‘K’ Value Message___  | <span style="color:red">No</span> | | Version 0 |
| ___Driver Info Message___  | <span style="color:red">No</span> | | Version 0 |
| ___Attribute Info Message___  | <span style="color:red">No</span> | | Version 0 |
| ___Object Reference Count Message___  | <span style="color:red">No</span> | | Version 0 |
| ___Compact Storage___  | <span style="color:green">Yes</span> | | |
| ___Continuous Storage___  | <span style="color:green">Yes</span> | | |
| ___Chunked Storage___  | <span style="color:green">Yes</span> | | |
| ___Fixed Point Type___  | <span style="color:green">Yes</span> | | |
| ___Floating Point Type___  | <span style="color:green">Yes</span> | | |
| ___Time Type___  | <span style="color:red">No</span> | | |
| ___String Type___  | <span style="color:green">Yes</span> | | |
| ___Bit Field Type___  | <span style="color:red">No</span> | | |
| ___Opaque Type___  | <span style="color:red">No</span> | | |
| ___Compound Type___  | <span style="color:red">No</span> | | |
| ___Reference Type___  | <span style="color:red">No</span> | | |
| ___Enumerated Type___  | <span style="color:red">No</span> | | |
| ___Variable Length Type___  | <span style="color:red">No</span> | | |
| ___Array Type___  | <span style="color:red">No</span> | | |
| ___Deflate Filter___  | <span style="color:green">Yes</span> | | |
| ___Shuffle Filter___  | <span style="color:green">Yes</span> | | |
| ___Fletcher32 Filter___  | <span style="color:red">No</span> | | |
| ___Szip Filter___  | <span style="color:red">No</span> | | |
| ___Nbit Filter___  | <span style="color:red">No</span> | | |
| ___Scale Offset Filter___  | <span style="color:red">No</span> | | |
