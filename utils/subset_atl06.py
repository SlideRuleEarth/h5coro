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

import time
from utils import args, Point, Profiler, H5CoroReader, SlideruleReader, S3fsReader, Ros3Reader, H5pyReader, LocalH5CoroReader, inpoly, region

###############################################################################
# LOCAL FUNCTIONS
###############################################################################

#
# Function: subsetted_read
#
# read ATL06 resource and return variable's data within polygon
#
def subsetted_read(profiler, region, variable):

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
    geocoords = profiler.read(geodatasets)

    # Build list of the subsetted variable datasets to read
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

###############################################################################
# MAIN
###############################################################################

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
    result = subsetted_read(profiler, region, variable=args.variable06)
    print(f'[{len(result[args.variable06])}]: {profiler.duration:.2f} {(time.perf_counter() - start):.2f}')
