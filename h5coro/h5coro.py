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

import os
import netrc
import requests
import json
import struct
import ctypes
import time
import logging
import numpy
from datetime import datetime, timedelta
from h5coro import version
import threading

###############################################################################
# GLOBALS
###############################################################################

DAAC = "nsidc"

BASIC_TYPES = {
    "INT8":     { "fmt": 'b', "size": 1, "nptype": numpy.int8   },
    "INT16":    { "fmt": 'h', "size": 2, "nptype": numpy.int16  },
    "INT32":    { "fmt": 'i', "size": 4, "nptype": numpy.int32  },
    "INT64":    { "fmt": 'q', "size": 8, "nptype": numpy.int64  },
    "UINT8":    { "fmt": 'B', "size": 1, "nptype": numpy.uint8  },
    "UINT16":   { "fmt": 'H', "size": 2, "nptype": numpy.uint16 },
    "UINT32":   { "fmt": 'I', "size": 4, "nptype": numpy.uint32 },
    "UINT64":   { "fmt": 'Q', "size": 8, "nptype": numpy.uint64 },
    "BITFIELD": { "fmt": 'x', "size": 0, "nptype": numpy.byte   }, # unsupported
    "FLOAT":    { "fmt": 'f', "size": 4, "nptype": numpy.single },
    "DOUBLE":   { "fmt": 'd', "size": 8, "nptype": numpy.double },
    "TIME8":    { "fmt": 'q', "size": 8, "nptype": numpy.int64  }, # numpy.datetime64
    "STRING":   { "fmt": 's', "size": 1, "nptype": numpy.byte   }
}

SIZE_2_FORMAT = {
    1: 'B',
    2: 'H',
    4: 'I',
    8: 'Q'
}

session = requests.Session()
session.trust_env = False

errorChecking = True

verbose = True
logger = logging.getLogger(__name__)


###############################################################################
# EXCEPTIONS
###############################################################################

class FatalError(RuntimeError):
    pass


###############################################################################
# H5Coro Class
###############################################################################

class H5Coro:

    # Constants
    CACHE_LINE_SIZE         =               0x10 #0x400000
    CACHE_LINE_MASK         = 0xFFFFFFFFFFFFFFF0
    H5_SIGNATURE_LE         = 0x0A1A0A0D46444889
    H5_OHDR_SIGNATURE_LE    = 0x5244484F
    H5_FRHP_SIGNATURE_LE    = 0x50485246
    H5_FHDB_SIGNATURE_LE    = 0x42444846
    H5_FHIB_SIGNATURE_LE    = 0x42494846
    H5_OCHK_SIGNATURE_LE    = 0x4B48434F
    H5_TREE_SIGNATURE_LE    = 0x45455254
    H5_HEAP_SIGNATURE_LE    = 0x50414548
    H5_SNOD_SIGNATURE_LE    = 0x444F4E53

    # Constructor
    def __init__(self, resource, driver_class, datasets=[], credentials={}):
        self.resource = resource
        self.driver = driver_class(resource, credentials)
        self.datasets = datasets
        self.credentials = credentials

        self.pos = 0
        self.lock = threading.Lock()
        self.cache = {}

        self.offsetSize = 0
        self.lengthSize = 0
        self.baseAddress = 0

        self.datasetPath = []
        self.datasetLevel = 0

        root_group_offset = self.readSuperblock();
        self.pos = root_group_offset

        obj_hdr_version = self.readField(1)
        if obj_hdr_version == 0:
            self.readObjHdrV0()
        elif obj_hdr_version == 1:
            self.readObjHdrV1()

        #readDataset(info);

    # ioRequest
    def ioRequest(self, size):
        # Check if Caching
        if size <= self.CACHE_LINE_SIZE:
            data = None
            cache_line = (self.pos + self.baseAddress) & self.CACHE_LINE_MASK
            with self.lock:
                # Populate Cache (if not there already)
                if cache_line not in self.cache:
                    self.cache[cache_line] = self.driver.read(cache_line, self.CACHE_LINE_SIZE)
                # Calculate Start and Stop Indexes into Cache Line
                start_index = (self.pos + self.baseAddress) - cache_line
                stop_index = start_index + size
                # Pull Data out of Cache
                if stop_index <= self.CACHE_LINE_SIZE:
                    data = self.cache[cache_line][start_index:stop_index]
                else:
                    # Populate Next Cache Line
                    next_cache_line = (cache_line + stop_index) & self.CACHE_LINE_MASK
                    if next_cache_line not in self.cache:
                        self.cache[next_cache_line] = self.driver.read(next_cache_line, self.CACHE_LINE_SIZE)
                    next_stop_index = stop_index - self.CACHE_LINE_SIZE
                    # Concatenate Data from Two Cache Lines
                    data = self.cache[cache_line][start_index:] + self.cache[next_cache_line][:next_stop_index]
            # Move Position and Return Data
            self.pos += size
            return data
        else:
            # Direct Read and Move Position
            data = self.driver.read(self.pos + self.baseAddress, size)
            self.pos += size
            return data

    # readField
    def readField(self, size):
        raw = self.ioRequest(size)
        fmt = SIZE_2_FORMAT[size]
        return struct.unpack(f'<{fmt}', raw)[0]

    # readSuperblock
    def readSuperblock(self):

        root_group_offset = None

        if errorChecking:
            # check file signature
            self.pos = 0
            signature = self.readField(8)
            if signature != self.H5_SIGNATURE_LE:
                raise FatalError(f'invalid file signature: {signature}')

            # check file version
            superblock_version = self.readField(1)
            if superblock_version != 0 and superblock_version != 2:
                raise FatalError(f'unsupported superblock version: {superblock_version}')

        # Super Block Version 0 #
        if superblock_version == 0:
            if errorChecking:
                # check free space version
                self.pos = 9
                freespace_version = self.readField(1)
                if freespace_version != 0:
                    raise FatalError(f'unsupported free space version: {freespace_version}')

                # check root table version
                roottable_version = self.readField(1)
                if roottable_version != 0:
                    raise FatalError(f'unsupported root table version: {roottable_version}')

            # read sizes
            self.pos = 13
            self.offsetSize = self.readField(1)
            self.lengthSize = self.readField(1)

            # set base address
            self.pos = 24
            self.baseAddress = self.readField(self.offsetSize)

            # read group offset
            self.pos = 24 + (5 * self.offsetSize)
            root_group_offset = self.readField(self.offsetSize)

        # Super Block Version 1 #
        else:
            # read sizes
            self.pos = 9
            self.offsetSize = self.readField(1)
            self.lengthSize = self.readField(1)

            # set base address
            self.pos = 12
            self.baseAddress = self.readField(self.offsetSize)

            # read group offset
            self.pos = 12 + (3 * self.offsetSize)
            root_group_offset = self.readField(self.offsetSize)

        # print file information
        if verbose:
            logger.info(f'File Information')
            logger.info(f'Size of Offsets:      {self.offsetSize}')
            logger.info(f'Size of Lengths:      {self.lengthSize}')
            logger.info(f'Base Address:         {self.baseAddress}')
            logger.info(f'Root Group Offset:    {root_group_offset}')

        # return root group offset
        return root_group_offset

    # readObjHdrV0
    def readObjHdrV0(self):
        starting_position = self.pos
        if errorChecking:
            # check header signature
            signature = self.readField(4)
            if signature != self.H5_OHDR_SIGNATURE_LE:
                raise FatalError(f'invalid version 0 object header signature: {signature}')
            # check header version
            version = self.readField(1)
            if version != 2:
                raise FatalError(f'unsupported header version: {version}')
        else:
            self.pos += 5

        # file stats
        FILE_STATS_BIT = 0x20
        obj_hdr_flags = self.readField(1)
        if obj_hdr_flags & FILE_STATS_BIT:
            if verbose:
                access_time = self.readField(4)
                modification_time = self.readField(4)
                change_time = self.readField(4)
                birth_time = self.readField(4)
                logger.info(f'Access Time:          {datetime.fromtimestamp(access_time)}')
                logger.info(f'Modification Time:    {datetime.fromtimestamp(modification_time)}')
                logger.info(f'Change Time:          {datetime.fromtimestamp(change_time)}')
                logger.info(f'Birth Time:           {datetime.fromtimestamp(birth_time)}')
            else:
                self.pos += 16

        # phase attributes
        STORE_CHANGE_PHASE_BIT = 0x10
        if obj_hdr_flags & STORE_CHANGE_PHASE_BIT:
            if verbose:
                max_compact_attr = self.readField(2)
                max_dense_attr = self.readField(2)
                logger.info(f'Max Compact Attr:     {max_compact_attr}')
                logger.info(f'Max Dense Attr:       {max_dense_attr}')
            else:
                self.pos += 4

        # read header messages
        SIZE_OF_CHUNK_0_MASK = 0x3
        size_of_chunk0 = self.readField(1 << (obj_hdr_flags & SIZE_OF_CHUNK_0_MASK))
        end_of_hdr = self.pos + size_of_chunk0
        self.pos += self.readMessagesV0(end_of_hdr, obj_hdr_flags)

        # verify checksum
        if verbose:
            checksum = self.readField(4)
            logger.info(f'Checksum:             {checksum}')
        else:
            self.pos += 4

        # return bytes read
        ending_position = self.pos
        return ending_position - starting_position

    # readMessagesV0
    def readMessagesV0(self, end_of_hdr, obj_hdf_flags):
        pass

    # readObjHdrV1
    def readObjHdrV1(self):
        starting_position = self.pos

