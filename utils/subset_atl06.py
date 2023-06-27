# Copyright (c) 2021, University of Washington
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# 3. Neither the name of the University of Washington nor the names of its
#    contributors may be used to endorse or promote products derived from this
#    software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE UNIVERSITY OF WASHINGTON AND CONTRIBUTORS
# “AS IS” AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
# TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE UNIVERSITY OF WASHINGTON OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
# OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
# OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import logging
import argparse
import time
import h5coro
import os
import configparser
import s3fs
import h5py
from h5coro import s3driver, filedriver
from sliderule import sliderule, h5

###############################################################################
# GLOBALS
###############################################################################

# profiles
profiles = {
    "h5coro": 0,
    "sliderule": 0,
    "s3fs": 0,
    "ros3": 0
}

# Setup Config Parser for Credentials
home_directory          = os.path.expanduser('~')
aws_credential_file     = os.path.join(home_directory, '.aws', 'credentials')
config                  = configparser.RawConfigParser()
credentials             = {}

###############################################################################
# COMMAND LINE ARGUMENTS
###############################################################################

parser = argparse.ArgumentParser(description="""Subset ATL06 granules""")
parser.add_argument('--granule','-g', type=str, default="/data/ATLAS/ATL03_20181017222812_02950102_005_01.h5")
parser.add_argument('--bucket','-b', type=str, default="sliderule")
parser.add_argument('--region','-r', type=str, default="us-west-2")
parser.add_argument('--aoi','-a', type=str, default="grandmesa.geojson")
parser.add_argument('--domain','-d', type=str, default="slideruleearth.io")
parser.add_argument('--organization','-o', type=str, default="esr")
parser.add_argument('--desired_nodes','-n', type=int, default=3)
parser.add_argument('--time_to_live','-l', type=int, default=120)
parser.add_argument('--log_file', '-f', type=str, default="esr.log")
parser.add_argument('--verbose','-v', action='store_true', default=False)
args,_ = parser.parse_known_args()

###############################################################################
# LOCAL CLASSES
###############################################################################

#
# Class: H5CoroReader
#
class H5CoroReader:
    def __init__(self, resource):
        self.resource = resource
        h5coro.config(errorChecking=False, verbose=False, enableAttributes=False, logLevel=logging.INFO)
    def read(self, datasets):
        start = time.perf_counter()
        self.h5obj = h5coro.H5Coro(self.resource, s3driver.S3Driver, datasets=datasets, block=True)
        profiles["h5coro"] = time.perf_counter() - start
        return self.h5obj

#
# Class: SlideruleReader
#
class SlideruleReader:
    def __init__(self, resource):
        self.resource = resource
        sliderule.init(args.domain, organization=args.organization, desired_nodes=args.desired_nodes, time_to_live=args.time_to_live)
    def read(self, datasets):
        start = time.perf_counter()
        values = h5.h5p(datasets, self.resource, "icesat2")
        profiles["sliderule"] = time.perf_counter() - start
        return values

#
# Class: S3fsReader
#
class S3fsReader:
    def __init__(self, resource):
        self.resource = resource
        s3 = s3fs.S3FileSystem()
        resource_path = "s3://" + args.bucket + self.resource
        self.f = h5py.File(s3.open(resource_path, 'rb'), mode='r')
    def read(self, datasets):
        start = time.perf_counter()
        values = {}
        for dataset in datasets:
            values[dataset] = self.f[dataset['dataset']][dataset['startrow']:dataset['startrow']+dataset['numrows']]
        profiles["s3fs"] = time.perf_counter() - start
        return values

#
# Class: Ros3Reader
#
class Ros3Reader:
    def __init__(self, resource):
        self.resource = resource
        config.read(aws_credential_file)
        aws_region = config.get('default', 'aws_access_key_id')
        secret_id = config.get('default', 'aws_secret_access_key')
        secret_key = config.get('default', 'aws_session_token')
        resource_path = f'http://s3.{args.region}.amazonaws.com/' + args.bucket + self.resource
        self.f = h5py.File(resource_path, driver="ros3", aws_region=aws_region, secret_id=secret_id, secret_key=secret_key)
    def read(self, datasets):
        start = time.perf_counter()
        values = {}
        for dataset in datasets:
            values[dataset] = self.f[dataset['dataset']][dataset['startrow']:dataset['startrow']+dataset['numrows']]
        profiles["ros3"] = time.perf_counter() - start
        return values

#
# Class: LocalH5CoroReader
#
class LocalH5CoroReader:
    def __init__(self, resource):
        self.resource = resource
        h5coro.config(errorChecking=False, verbose=False, enableAttributes=False, logLevel=logging.INFO)
    def read(self, datasets):
        start = time.perf_counter()
        self.h5obj = h5coro.H5Coro(self.resource, filedriver.FileDriver, datasets=datasets, block=True)
        profiles["h5coro"] = time.perf_counter() - start
        return self.h5obj

#
# Class: Point
#
class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y

###############################################################################
# LOCAL FUNCTIONS
###############################################################################

#
# Function: inpoly
#
# Algorithm based off of https://wrf.ecse.rpi.edu/Research/Short_Notes/pnpoly.html;
# the copyright notice associated with code provided on the website is reproduced
# below:
#
#
# Copyright (c) 1970-2003, Wm. Randolph Franklin
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to the following
# conditions:
#
#   Redistributions of source code must retain the above copyright notice, this list of
#   conditions and the following disclaimers.
#
#   Redistributions in binary form must reproduce the above copyright notice in the
#   documentation and/or other materials provided with the distribution.
#
#   The name of W. Randolph Franklin may not be used to endorse or promote products derived
#   from this Software without specific prior written permission.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
# PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
# FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
#
def inpoly (poly, point):
    c = False
    i = 0
    j = len(poly) - 1
    while i < len(poly):
        j = i
        i += 1
        x_extent = (poly[j].x - poly[i].x) * (point.y - poly[i].y) / (poly[j].y - poly[i].y) + poly[i].x
        if ((poly[i].y > point.y) != (poly[j].y > point.y)) and (point.x < x_extent):
            c = not c

    # Return Inclusion
    #  if c == False: number of intersections were even --> point is outside polygon
    #  if c == True: number of intersections were odd --> point is inside polygon
    return c

#
# Function: subsetted_read
#
# read ATL06 resource and return variable's data within polygon
#
def subsetted_read(reader, region, variable="h_li"):

    # Initialize return variables
    data = []
    latitudes = []
    longitudes = []

    # Create polygon
    poly = [Point(coord["lon"], coord["lat"]) for coord in region]

    # List of tracks to read
    tracks = ["1l", "1r", "2l", "2r", "3l", "3r"]

    # Build list of each lat,lon dataset to read
    geodatasets = []
    for track in tracks:
        prefix = "/gt"+track+"/land_ice_segments/"
        geodatasets.append({"dataset": prefix+"latitude", "startrow": 0, "numrows": -1})
        geodatasets.append({"dataset": prefix+"longitude", "startrow": 0, "numrows": -1})

    # Read lat,lon from resource
    geocoords = reader.read(geodatasets)

    # Build list of the subsetted h_li datasets to read
    datasets = []
    for track in tracks:
        prefix = "/gt"+track+"/land_ice_segments/"
        lat_dataset = geocoords[prefix+"latitude"]
        lon_dataset = geocoords[prefix+"longitude"]
        startrow = -1
        numrows = -1
        index = 0
        while index < len(lat_dataset):
            point = Point(lon_dataset[index], lat_dataset[index])
            intersect = inpoly(poly, point)
            if startrow == -1 and intersect:
                startrow = index
            elif startrow != -1 and not intersect:
                break
            index += 10 # only sample values for speed increase
        if startrow >= 0:
            numrows = index - startrow
        if numrows > 0:
            datasets.append({"dataset": prefix+variable, "startrow": startrow, "numrows": numrows, "prefix": prefix})

    # Read h_li from resource
    if len(datasets) > 0:
        values = reader.read(datasets)

    # Append results
    for entry in datasets:
        latitudes += geocoords[entry["prefix"]+"latitude"][entry["startrow"]:entry["startrow"]+entry["numrows"]].tolist()
        longitudes += geocoords[entry["prefix"]+"longitude"][entry["startrow"]:entry["startrow"]+entry["numrows"]].tolist()
        data += values[entry["prefix"]+variable].tolist()

    # Return results
    return {"latitude":  latitudes,
            "longitude": longitudes,
            variable:    data}

###############################################################################
# MAIN
###############################################################################

region = sliderule.toregion(args.aoi)["poly"]

print(profiles)
