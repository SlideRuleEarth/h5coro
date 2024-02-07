# Copyright (c) 2023, University of Washington
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

import argparse
import time
import logging
import os
import sys
import configparser
import warnings
import s3fs
import h5py
import h5coro
from h5coro import s3driver, filedriver, logger
from sliderule import sliderule, h5, icesat2

###############################################################################
# GLOBALS
###############################################################################

# Setup Config Parser for Credentials
home_directory          = os.path.expanduser('~')
aws_credential_file     = os.path.join(home_directory, '.aws', 'credentials')
config                  = configparser.RawConfigParser()
credentials             = {}

###############################################################################
# COMMAND LINE ARGUMENTS
###############################################################################

parser = argparse.ArgumentParser(description="""Subset granules""")
parser.add_argument('--granule03','-p', type=str, default="/data/ATLAS/ATL03_20181017222812_02950102_005_01.h5")
parser.add_argument('--granule06','-c', type=str, default="/data/ATLAS/ATL06_20181017222812_02950102_005_01.h5")
parser.add_argument('--bucket','-b', type=str, default="sliderule")
parser.add_argument('--region','-r', type=str, default="us-west-2")
parser.add_argument('--aoi','-a', type=str, default="data/grandmesa.geojson")
parser.add_argument('--variable03','-x', type=str, default="h_ph")
parser.add_argument('--variable06','-y', type=str, default="h_li")
parser.add_argument('--domain','-d', type=str, default="slideruleearth.io")
parser.add_argument('--organization','-o', type=str, default="sliderule")
parser.add_argument('--desired_nodes','-n', type=int, default=1)
parser.add_argument('--time_to_live','-l', type=int, default=120)
parser.add_argument('--checkErrors','-e', action='store_true', default=False)
parser.add_argument('--enableAttributes','-t', action='store_true', default=False)
parser.add_argument('--verbose','-v', action='store_true', default=False)
parser.add_argument('--loglvl','-j', type=str, default="CRITICAL")
args,_ = parser.parse_known_args()

# Initialize Organization
if args.organization == "None":
    args.organization = None
    args.desired_nodes = None
    args.time_to_live = None

# Configure H5Coro
logger.config(logLevel=logging.INFO)

# Initialize SlideRule Client
sliderule.init(args.domain, verbose=args.verbose, organization=args.organization, desired_nodes=args.desired_nodes, time_to_live=args.time_to_live)

# Generate Region Polygon
region = sliderule.toregion(args.aoi)["poly"]

###############################################################################
# READER CLASSES
###############################################################################

#
# Class: H5CoroReader
#
class H5CoroReader:
    def __init__(self, resource,):
        self.resource = resource
        self.h5obj = h5coro.H5Coro(args.bucket + self.resource, s3driver.S3Driver, errorChecking=args.checkErrors, verbose=args.verbose)
    def read(self, datasets):
        return self.h5obj.readDatasets(datasets=datasets, block=True, enableAttributes=args.enableAttributes)

#
# Class: SlideruleReader
#
class SlideruleReader:
    def __init__(self, resource):
        self.resource = resource
    def read(self, datasets):
        values = h5.h5p(datasets, self.resource.split('/')[-1], "atlas-s3")
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
        values = {}
        for dataset in datasets:
            values[dataset['dataset']] = self.f[dataset['dataset']][dataset['startrow']:dataset['startrow']+dataset['numrows']]
        return values

#
# Class: Ros3Reader
#
class Ros3Reader:
    def __init__(self, resource):
        self.resource = resource
        config.read(aws_credential_file)
        aws_region = config.get('default', 'aws_access_key_id').encode("utf-8")
        secret_id = config.get('default', 'aws_secret_access_key').encode("utf-8")
        secret_key = config.get('default', 'aws_session_token').encode("utf-8")
        resource_path = f'http://s3.{args.region}.amazonaws.com/' + args.bucket + self.resource
        print(resource_path)
        self.f = h5py.File(resource_path, driver="ros3", aws_region=aws_region, secret_id=secret_id, secret_key=secret_key, mode='r')
    def read(self, datasets):
        values = {}
        for dataset in datasets:
            values[dataset['dataset']] = self.f[dataset['dataset']][dataset['startrow']:dataset['startrow']+dataset['numrows']]
        return values

#
# Class: H5pyReader
#
class H5pyReader:
    def __init__(self, resource):
        self.resource = resource
        self.f = h5py.File(resource, mode='r')
    def read(self, datasets):
        values = {}
        for dataset in datasets:
            values[dataset['dataset']] = self.f[dataset['dataset']][dataset['startrow']:dataset['startrow']+dataset['numrows']]
        return values

#
# Class: LocalH5CoroReader
#
class LocalH5CoroReader:
    def __init__(self, resource):
        self.resource = resource
        self.h5obj = h5coro.H5Coro(self.resource, filedriver.FileDriver)
    def read(self, datasets):
        return self.h5obj.readDatasets(datasets=datasets, block=True, enableAttributes=args.enableAttributes)

#
# Class: Profiler
#
class Profiler:
    def __init__(self, reader_class, resource):
        start = time.perf_counter()
        self.reader = reader_class(resource)
        self.duration = time.perf_counter() - start
    def read(self, datasets):
        start = time.perf_counter()
        values = self.reader.read(datasets)
        self.duration += time.perf_counter() - start
        return values

###############################################################################
# MATH UTILITIES
###############################################################################

#
# Class: Point
#
class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y
    def __str__(self):
        return f'({self.x},{self.y})'

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

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        c = False
        i = 0
        j = len(poly) - 1
        while i < len(poly):
            x_extent = (poly[j].x - poly[i].x) * (point.y - poly[i].y) / (poly[j].y - poly[i].y) + poly[i].x
            if ((poly[i].y > point.y) != (poly[j].y > point.y)) and (point.x < x_extent):
                c = not c
            j = i
            i += 1

    # Return Inclusion
    #  if c == False: number of intersections were even --> point is outside polygon
    #  if c == True: number of intersections were odd --> point is inside polygon
    return c

###############################################################################
# ATL06 FUNCTIONS
###############################################################################

#
# Function: atl06_subsetted_read
#
# read ATL06 resource and return variable's data within polygon
#
def atl06_subsetted_read(profiler, region, variable):

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
        prefix = "gt"+track+"/land_ice_segments/"
        geodatasets.append({"dataset": prefix+"latitude", "startrow": 0, "numrows": -1})
        geodatasets.append({"dataset": prefix+"longitude", "startrow": 0, "numrows": -1})

    # Read lat,lon from resource
    geocoords = profiler.read(geodatasets)

    # Build list of the subsetted variable datasets to read
    datasets = []
    for track in tracks:
        prefix = "gt"+track+"/land_ice_segments/"
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

    # Read variable from resource
    if len(datasets) > 0:
        values = profiler.read(datasets)

    # Append results
    for entry in datasets:
        latitudes += geocoords[entry["prefix"]+"latitude"][entry["startrow"]:entry["startrow"]+entry["numrows"]].tolist()
        longitudes += geocoords[entry["prefix"]+"longitude"][entry["startrow"]:entry["startrow"]+entry["numrows"]].tolist()
        data += values[entry["prefix"]+variable].tolist()

    # Return results
    return {"latitude":  latitudes,
            "longitude": longitudes,
            variable:    data}

#
# Function: atl06_profile
#
def atl06_profile():
    # Build Profilers
    profiles = {
        "s3fs":         Profiler(S3fsReader,        args.granule06),
    #    "ros3":         Profiler(Ros3Reader,        args.granule06),
        "sliderule-h5p":Profiler(SlideruleReader,   args.granule06),
        "h5coro":       Profiler(H5CoroReader,      args.granule06),
        "h5py-local":   Profiler(H5pyReader,        args.granule06),
        "h5coro-local": Profiler(LocalH5CoroReader, args.granule06)
    }

    # Profile Readers
    for profile in profiles:
        profiler = profiles[profile]
        print(f'Profiling {profile}... ', end='')
        start = time.perf_counter()
        result = atl06_subsetted_read(profiler, region, variable=args.variable06)
        print(f'[{len(result[args.variable06])}]: {profiler.duration:.2f} {(time.perf_counter() - start):.2f}')

###############################################################################
# ATL03 FUNCTIONS
###############################################################################

#
# Function: atl03_subsetted_read
#
# read ATL03 resource and return variable's data within polygon
#
def atl03_subsetted_read(profiler, region, variable, tracks = ["1l", "1r", "2l", "2r", "3l", "3r"]):

    # Initialize return variables
    data = []
    latitudes = []
    longitudes = []

    # Create polygon
    poly = [Point(coord["lon"], coord["lat"]) for coord in region]

    # Build list of each lat,lon dataset to read
    geodatasets = []
    for track in tracks:
        prefix = "gt"+track+"/geolocation/"
        geodatasets.append({"dataset": prefix+"reference_photon_lat", "startrow": 0, "numrows": -1})
        geodatasets.append({"dataset": prefix+"reference_photon_lon", "startrow": 0, "numrows": -1})
        geodatasets.append({"dataset": prefix+"segment_ph_cnt", "startrow": 0, "numrows": -1})

    # Read lat,lon from resource
    geocoords = profiler.read(geodatasets)

    # Build list of the subsetted variable datasets to read
    datasets = []
    for track in tracks:
        geoprefix = "gt"+track+"/geolocation/"
        prefix = "gt"+track+"/heights/"
        lat_dataset = geocoords[geoprefix+"reference_photon_lat"]
        lon_dataset = geocoords[geoprefix+"reference_photon_lon"]
        cnt_dataset = geocoords[geoprefix+"segment_ph_cnt"]
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
            start_ph = int(sum(cnt_dataset[:startrow]))
            num_ph = int(sum(cnt_dataset[startrow:startrow+numrows]))
            datasets.append({"dataset": prefix+variable, "startrow": start_ph, "numrows": num_ph, "col": 0, "startseg": startrow, "numsegs": numrows, "prefix": prefix, "geoprefix": geoprefix})

    # Read variable from resource
    if len(datasets) > 0:
        values = profiler.read(datasets)

    # Append results
    for entry in datasets:
        segments = geocoords[entry["geoprefix"]+"segment_ph_cnt"][entry["startseg"]:entry["startseg"]+entry["numsegs"]].tolist()
        k = 0
        for num_ph in segments:
            for i in range(num_ph):
                latitudes += [geocoords[entry["geoprefix"]+"reference_photon_lat"][k]]
                longitudes += [geocoords[entry["geoprefix"]+"reference_photon_lon"][k]]
            k += 1
        data += values[entry["prefix"]+variable].tolist()

    # Return results
    return {"latitude":  latitudes,
            "longitude": longitudes,
            variable:    data}

#
# Function: atl03_sliderule_request
#
# request subsetting from sliderule for ATL03
#
def atl03_sliderule_request(region, parquet_file=None, open_on_complete=False, geo_fields=[], ph_fields=[]):
    parms = {
        "poly":         region,
        "srt":          icesat2.SRT_LAND,
        "len":          40,
        "res":          40,
        "pass_invalid": True,
        "cnf":          -2,
    }

    if parquet_file != None:
        parms["output"] = { "path": parquet_file, "format": "parquet", "open_on_complete": open_on_complete }

    if len(geo_fields) > 0:
        parms["atl03_geo_fields"] = geo_fields

    if len(ph_fields) > 0:
        parms["atl03_ph_fields"] = ph_fields

    start = time.perf_counter()
    gdf = icesat2.atl03sp(parms, asset="atlas-s3", resources=[args.granule03.split('/')[-1]])
    duration = time.perf_counter() - start
    return len(gdf), duration

#
# Function: atl03_profile
#
def atl03_profile():
    # Build Profilers
    profiles = {
        "s3fs":         Profiler(S3fsReader,        args.granule03),
    #    "ros3":         Profiler(Ros3Reader,        args.granule03),
        "sliderule-h5p":Profiler(SlideruleReader,   args.granule03),
        "h5coro":       Profiler(H5CoroReader,      args.granule03),
        "h5py-local":   Profiler(H5pyReader,        args.granule03),
        "h5coro-local": Profiler(LocalH5CoroReader, args.granule03)
    }

    # Profile Readers
    for profile in profiles:
        profiler = profiles[profile]
        print(f'Profiling {profile}... ', end='')
        sys.stdout.flush()
        start = time.perf_counter()
        result = atl03_subsetted_read(profiler, region, variable=args.variable03)
        print(f'[{len(result[args.variable03])}]: {profiler.duration:.2f} {(time.perf_counter() - start):.2f}')

###############################################################################
# MAIN
###############################################################################

