import os
from setuptools import setup, find_packages

# get long_description from README.md
with open("README.md", "r") as fh:
    long_description = fh.read()

# get install requirements
with open('requirements.txt') as fh:
    install_requires = fh.read().splitlines()

# get version
with open('version.txt') as fh:
    version = fh.read().strip()
    if version[0] == 'v':
        version = version[1:]

setup(
    name='h5coro',
    author='SlideRule Developers',
    description='Python package for reading HDF5 data from S3',
    long_description_content_type="text/markdown",
    url='https://github.com/SlideRuleEarth/h5coro/',
    license='BSD 3-Clause',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Physics',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.11',
    ],
    packages=find_packages(),
    version=version,
    install_requires=install_requires,
    entry_points={
        "xarray.backends": ["h5coro=h5coro.backends.xarray_h5coro:H5CoroBackendEntrypoint"],
    },
)
