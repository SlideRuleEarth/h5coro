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
import tempfile
import sys
import configparser
import warnings
import boto3
import s3fs
import h5py
import h5coro
from h5coro import s3driver, logger
from sliderule import sliderule, h5, icesat2

###############################################################################
# GLOBALS
###############################################################################

parser = argparse.ArgumentParser(description="""Subset granules""")
parser.add_argument('--granule03','-p', type=str, default="data/ATLAS/ATL03_20181017222812_02950102_007_01.h5")
parser.add_argument('--granule06','-c', type=str, default="data/ATLAS/ATL06_20181017222812_02950102_007_01.h5")
parser.add_argument('--bucket','-b', type=str, default="sliderule")
parser.add_argument('--region','-r', type=str, default="us-west-2")
parser.add_argument('--aoi','-a', type=str, default="data/grandmesa.geojson")
parser.add_argument('--variable03','-x', type=str, default="h_ph")
parser.add_argument('--variable06','-y', type=str, default="h_li")
parser.add_argument('--checkErrors','-e', action='store_true', default=False)
parser.add_argument('--enableAttributes','-t', action='store_true', default=False)
parser.add_argument('--verbose','-v', action='store_true', default=False)
parser.add_argument('--loglvl','-j', type=str, default="CRITICAL")
args,_ = parser.parse_known_args()

# Configure H5Coro
logger.config(logLevel=logging.INFO)

# Generate Region Polygon
aoi_poly = sliderule.toregion(args.aoi)["poly"]

###############################################################################
# READER CLASSES
###############################################################################

#
# Class: H5CoroReader
#
class H5CoroReader:
    def __init__(self, resource,):
        self.resource = resource
        self.h5obj = h5coro.H5Coro(args.bucket + "/" + self.resource, s3driver.S3Driver, errorChecking=args.checkErrors, verbose=args.verbose)
    def read(self, datasets):
        return self.h5obj.readDatasets(datasets=datasets, block=True, enableAttributes=args.enableAttributes)
    def cleanup(self):
        pass

#
# Class: SlideruleReader
#
class SlideruleReader:
    def __init__(self, resource):
        self.resource = resource
    def read(self, datasets):
        values = h5.h5p(datasets, self.resource.split('/')[-1], "atlas-s3")
        return values
    def cleanup(self):
        pass

#
# Class: S3fsReader
#
class S3fsReader:
    def __init__(self, resource):
        self.resource = resource
        s3 = s3fs.S3FileSystem()
        resource_path = "s3://" + args.bucket + "/" + self.resource
        self.f = h5py.File(s3.open(resource_path, 'rb'), mode='r')
    def read(self, datasets):
        values = {}
        for dataset in datasets:
            values[dataset['dataset']] = self.f[dataset['dataset']][dataset['startrow']:dataset['startrow']+dataset['numrows']]
        return values
    def cleanup(self):
        pass

#
# Class: H5pyReader
#
class H5pyReader:
    def __init__(self, resource):
        self.resource = os.path.join(tempfile.gettempdir(), resource.split("/")[-1])
        s3 = boto3.client("s3")
        s3.download_file(args.bucket, resource, self.resource)
        self.f = h5py.File(self.resource, mode='r')
    def read(self, datasets):
        values = {}
        for dataset in datasets:
            values[dataset['dataset']] = self.f[dataset['dataset']][dataset['startrow']:dataset['startrow']+dataset['numrows']]
        return values
    def cleanup(self):
        os.remove(self.resource)
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
    def cleanup(self):
        self.reader.cleanup()

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
def atl06_subsetted_read(profiler):

    # Configuration
    region = aoi_poly
    variable = args.variable06

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
        geodatasets.append({"dataset": prefix+"latitude", "startrow": 0, "numrows": -1, "hyperslice": [[0,None]]})
        geodatasets.append({"dataset": prefix+"longitude", "startrow": 0, "numrows": -1, "hyperslice": [[0,None]]})

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
            datasets.append({"dataset": prefix+variable, "startrow": startrow, "numrows": numrows, "prefix": prefix, "hyperslice": [[startrow, startrow+numrows]]})

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
            "result":    data}

###############################################################################
# ATL03 FUNCTIONS
###############################################################################

#
# Function: atl03_subsetted_read
#
# read ATL03 resource and return variable's data within polygon
#
def atl03_subsetted_read(profiler):

    # Configuration
    region = aoi_poly
    variable = args.variable03
    tracks = ["1l", "1r", "2l", "2r", "3l", "3r"]

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
        geodatasets.append({"dataset": prefix+"reference_photon_lat", "startrow": 0, "numrows": -1, "hyperslice": [[0,None]]})
        geodatasets.append({"dataset": prefix+"reference_photon_lon", "startrow": 0, "numrows": -1, "hyperslice": [[0,None]]})
        geodatasets.append({"dataset": prefix+"segment_ph_cnt", "startrow": 0, "numrows": -1, "hyperslice": [[0,None]]})

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
            datasets.append({"dataset": prefix+variable, "startrow": start_ph, "numrows": num_ph, "col": 0,
                             "startseg": startrow, "numsegs": numrows,
                             "prefix": prefix, "geoprefix": geoprefix, "hyperslice": [[start_ph,start_ph+num_ph]]})

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
            "result":    data}

###############################################################################
# MAIN
###############################################################################

if __name__ == '__main__':

    # Build Profilers
    profiles = {
        "s3fs-06":      {"p": Profiler(S3fsReader,      args.granule06), "f": atl06_subsetted_read},
        "sliderule-06": {"p": Profiler(SlideruleReader, args.granule06), "f": atl06_subsetted_read},
        "h5coro-06":    {"p": Profiler(H5CoroReader,    args.granule06), "f": atl06_subsetted_read},
        "h5py-06":      {"p": Profiler(H5pyReader,      args.granule06), "f": atl06_subsetted_read},
        "s3fs-03":      {"p": Profiler(S3fsReader,      args.granule03), "f": atl03_subsetted_read},
        "sliderule-03": {"p": Profiler(SlideruleReader, args.granule03), "f": atl03_subsetted_read},
        "h5coro-03":    {"p": Profiler(H5CoroReader,    args.granule03), "f": atl03_subsetted_read},
        "h5py-03":      {"p": Profiler(H5pyReader,      args.granule03), "f": atl03_subsetted_read},
    }

    # Profile Readers
    for profile in profiles:
        profiler = profiles[profile]["p"]
        target = profiles[profile]["f"]
        print(f'Profiling {profile}... ', end='')
        start = time.perf_counter()
        result = target(profiler)
        profiler.cleanup()
        print(f'[{len(result["result"])}]: {profiler.duration:.2f} {(time.perf_counter() - start):.2f}')
