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

import sys
import time
from sliderule import icesat2
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
    distances = []

    # Create polygon
    poly = [Point(coord["lon"], coord["lat"]) for coord in region]

    # List of tracks to read
    tracks = ["1l", "1r", "2l", "2r", "3l", "3r"]

    # Build list of each lat,lon dataset to read
    geodatasets = []
    for track in tracks:
        prefix = "/gt"+track+"/geolocation/"
        geodatasets.append({"dataset": prefix+"reference_photon_lat", "startrow": 0, "numrows": -1})
        geodatasets.append({"dataset": prefix+"reference_photon_lon", "startrow": 0, "numrows": -1})
        geodatasets.append({"dataset": prefix+"segment_ph_cnt", "startrow": 0, "numrows": -1})

    # Read lat,lon from resource
    geocoords = profiler.read(geodatasets)

    # Build list of the subsetted variable datasets to read
    datasets = []
    for track in tracks:
        geoprefix = "/gt"+track+"/geolocation/"
        prefix = "/gt"+track+"/heights/"
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
            datasets.append({"dataset": prefix+variable, "startrow": start_ph, "numrows": num_ph, "prefix": prefix, "geoprefix": geoprefix})
            datasets.append({"dataset": prefix+"dist_ph_along", "startrow": start_ph, "numrows": num_ph, "prefix": prefix, "geoprefix": geoprefix})

    # Read variable from resource
    if len(datasets) > 0:
        values = profiler.read(datasets)

    # Append results
    for entry in datasets:
        segments = geocoords[entry["geoprefix"]+"segment_ph_cnt"][entry["startrow"]:entry["startrow"]+entry["numrows"]].tolist()
        k = 0
        for num_ph in segments:
            for i in range(num_ph):
                latitudes += [geocoords[entry["geoprefix"]+"reference_photon_lat"][entry["startrow"]+i]]
                longitudes += [geocoords[entry["geoprefix"]+"reference_photon_lon"][entry["startrow"]+i]]
                distances += [geocoords[entry["geoprefix"]+"segment_dist_x"][entry["startrow"]+i] + values[entry["prefix"]+"dist_ph_along"][k]]
                k += 1
        data += values[entry["prefix"]+variable].tolist()

    # Return results
    return {"latitude":  latitudes,
            "longitude": longitudes,
            "distances": distances,
            variable:    data}

#
# Function: sliderule_request
#
# request subsetting from sliderule for ATL03
#
def sliderule_request(region, parquet_file=None, open_on_complete=False, geo_fields=[], ph_fields=[]):
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

###############################################################################
# MAIN
###############################################################################

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
    result = subsetted_read(profiler, region, variable=args.variable03)
    print(f'[{len(result[args.variable03])}]: {profiler.duration:.2f} {(time.perf_counter() - start):.2f}')

# Display H5Coro Performance Statistics
h5obj = profiles["h5coro"].read([])
print(f'Metadata Table Hits: {h5obj.metaDataHits}')

# Profile SlideRule ATL03 Subsetter - flatrec03 issue
#print(f'Profiling sliderule-atl03s...', end='')
#num_photons, duration = sliderule_request(region, "atl03.parquet")
#print(f'[{num_photons}]: {duration}')