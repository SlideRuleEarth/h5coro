#!/usr/bin/env python
u"""
version.py (11/2023)
Gets version number of a package
"""
import importlib.metadata

# package metadata
metadata = importlib.metadata.metadata("h5coro")
# get version
version = metadata["version"]
# append "v" before the version
full_version = "v{0}".format(version)
# get project name
project_name = metadata["Name"]
