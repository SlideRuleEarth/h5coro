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

import requests
import threading
import struct
import logging
import numpy
from datetime import datetime

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
# H5Dataset Class
###############################################################################

class H5Dataset:

    #######################
    # Constants
    #######################
    H5_OHDR_SIGNATURE_LE    = 0x5244484F
    H5_FRHP_SIGNATURE_LE    = 0x50485246
    H5_FHDB_SIGNATURE_LE    = 0x42444846
    H5_FHIB_SIGNATURE_LE    = 0x42494846
    H5_OCHK_SIGNATURE_LE    = 0x4B48434F
    H5_TREE_SIGNATURE_LE    = 0x45455254
    H5_HEAP_SIGNATURE_LE    = 0x50414548
    H5_SNOD_SIGNATURE_LE    = 0x444F4E53
    H5CORO_CUSTOM_V1_FLAG   = 0x80

    #######################
    # Constructor
    #######################
    def __init__(self, resourceObject, dataset, credentials={}):
        self.resourceObject = resourceObject
        self.dataset = dataset
        self.credentials = credentials
        self.pos = self.resourceObject.rootAddress
        self.datasetPath = dataset.split('/')
        self.datasetLevel = 0
        self.highestDatasetLevel = 0

    #######################
    # readField
    #######################
    def readField(self, size):
        raw = self.resourceObject.ioRequest(self.pos, size)
        self.pos += size
        return struct.unpack(f'<{SIZE_2_FORMAT[size]}', raw)[0]

    #######################
    # readDataset
    #######################
    def readDataset(self):
        obj_hdr_version = self.readField(1)
        if obj_hdr_version == 0:
            self.readObjHdrV0()
        elif obj_hdr_version == 1:
            self.readObjHdrV1()
        else:
            raise FatalError(f'unsupported object header version: {obj_hdr_version}')

    #######################
    # readObjHdrV0
    #######################
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
                logger.info(f'Object Information V0 [{self.datasetLevel}] @{self.pos}')
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
        return self.pos - starting_position

    #######################
    # readMessagesV0
    #######################
    def readMessagesV0(self, end_of_hdr, obj_hdr_flags):
        starting_position = self.pos
        while self.pos < end_of_hdr:
            # read message info
            msg_type = self.readField(1)
            msg_size = self.readField(2)
            msg_flags = self.readField(1)

            # read messag order
            ATTR_CREATION_TRACK_BIT = 0x4
            if obj_hdr_flags & ATTR_CREATION_TRACK_BIT:
                msg_order = self.readField(2)

            # read message
            bytes_read = self.readMessage(msg_type, msg_size, obj_hdr_flags)
            if errorChecking and (bytes_read != msg_size):
                raise FatalError(f'header message different size than specified: {bytes_read} != {msg_size}')

            # check if dataset found
            if self.highestDatasetLevel > self.datasetLevel:
                self.pos = end_of_hdr # go directory to end of header
                break # exit loop because dataset is found

            # update position
            self.pos += bytes_read

        # check bytes read
        if errorChecking and (self.pos != end_of_hdr):
            raise FatalError(f'did not read correct number of bytes: {self.pos} != {end_of_hdr}')

        # return bytes read
        return self.pos - starting_position

    #######################
    # readObjHdrV1
    #######################
    def readObjHdrV1(self):
        starting_position = self.pos

        if errorChecking:
            # read reserved field
            reserved0 = self.readField(1)
            if reserved0 != 0:
                raise FatalError(f'reserved field not zero: {reserved0}')
        else:
            self.pos += 1

        if verbose:
            # read number of header messages
            num_hdr_msgs = self.readField(2)
            logger.info(f'Object Information V1 [{self.datasetLevel}] @{self.pos}')
            logger.info(f'# Header Messages:    {num_hdr_msgs}')

            # read object reference count
            obj_ref_count = self.readField(4)
            logger.info(f'Obj Reference Count:  {obj_ref_count}')
        else:
            self.pos += 6

        # read object header size
        obj_hdr_size = self.readField(self.resourceObject.lengthSize)
        end_of_hdr = self.pos + obj_hdr_size

        # read header messages
        self.pos += self.readMessagesV1(end_of_hdr, self.H5CORO_CUSTOM_V1_FLAG)

        # return bytes read
        ending_position = self.pos
        return ending_position - starting_position

    #######################
    # readMessagesV1
    #######################
    def readMessagesV1(self, end_of_hdr, obj_hdr_flags):
        starting_position = self.pos
        SIZE_OF_V1_PREFIX = 8
        while self.pos < (end_of_hdr - SIZE_OF_V1_PREFIX):
            # read message info
            msg_type = self.readField(2)
            msg_size = self.readField(2)
            msg_flags = self.readField(1)

            # read reserved fields
            if errorChecking:
                reserved1 = self.readField(1)
                reserved2 = self.readField(2)
                if reserved1 != 0 and reserved2 != 0:
                    raise FatalError(f'invalid reserved fields: {reserved1},{reserved2}')
            else:
                self.pos += 3

            # read message
            bytes_read = self.readMessage(msg_type, msg_size, obj_hdr_flags)
            bytes_read += ((8 - (bytes_read % 8)) % 8) # align to 8-byte boundary
            if errorChecking and (bytes_read != msg_size):
                raise FatalError(f'header message different size than specified: {bytes_read} != {msg_size}')

            # check if dataset found
            if self.highestDatasetLevel > self.datasetLevel:
                self.pos = end_of_hdr # go directory to end of header
                break # exit loop because dataset is found

            # update position
            self.pos += bytes_read

        # move past gap
        if self.pos < end_of_hdr:
            self.pos = end_of_hdr

        # check bytes read
        if errorChecking and (self.pos != end_of_hdr):
            raise FatalError(f'did not read correct number of bytes: {self.pos} != {end_of_hdr}')

        # return bytes read
        return self.pos - starting_position

    #######################
    # readMessage
    #######################
    def readMessage(self, msg_type, msg_size, obj_hdr_flags):
        msg_handler_table = {
            0x1:  self.dataspaceMsgHandler,
            0x2:  self.linkinfoMsgHandler,
            0x3:  self.datatypeMsgHandler,
            0x4:  self.fillvalueMsgHandler,
            0x6:  self.linkMsgHandler,
            0x8:  self.datalayoutMsgHandler,
            0xC:  self.attributeMsgHandler,
            0x10: self.headercontMsgHandler,
            0x11: self.symboltableMsgHandler,
            0x15: self.attributeinfoMsgHandler
        }
        try:
            return msg_handler_table[msg_type](msg_size, obj_hdr_flags)
        except FatalError:
            if verbose:
                logger.info(f'Skipped Message [{self.datasetLevel}] @{self.pos}: {msg_type}, {msg_size}')
            return msg_size

    #######################
    # dataspaceMsgHandler
    #######################
    def dataspaceMsgHandler(self, msg_size, obj_hdr_flags):
        return msg_size

    #######################
    # linkinfoMsgHandler
    #######################
    def linkinfoMsgHandler(self, msg_size, obj_hdr_flags):
        return msg_size

    #######################
    # datatypeMsgHandler
    #######################
    def datatypeMsgHandler(self, msg_size, obj_hdr_flags):
        return msg_size

    #######################
    # fillvalueMsgHandler
    #######################
    def fillvalueMsgHandler(self, msg_size, obj_hdr_flags):
        return msg_size

    #######################
    # linkMsgHandler
    #######################
    def linkMsgHandler(self, msg_size, obj_hdr_flags):
        return msg_size

    #######################
    # datalayoutMsgHandler
    #######################
    def datalayoutMsgHandler(self, msg_size, obj_hdr_flags):
        return msg_size

    #######################
    # attributeMsgHandler
    #######################
    def attributeMsgHandler(self, msg_size, obj_hdr_flags):
        return msg_size

    #######################
    # headercontMsgHandler
    #######################
    def headercontMsgHandler(self, msg_size, obj_hdr_flags):
        return msg_size

    #######################
    # symboltableMsgHandler
    #######################
    def symboltableMsgHandler(self, msg_size, obj_hdr_flags):
        return msg_size

    #######################
    # attributeinfoMsgHandler
    #######################
    def attributeinfoMsgHandler(self, msg_size, obj_hdr_flags):
        return msg_size


###############################################################################
# H5Coro Class
###############################################################################

class H5Coro:

    #######################
    # Constants
    #######################
    CACHE_LINE_SIZE         =               0x10 #0x400000
    CACHE_LINE_MASK         = 0xFFFFFFFFFFFFFFF0
    H5_SIGNATURE_LE         = 0x0A1A0A0D46444889

    #######################
    # Constructor
    #######################
    def __init__(self, resource, driver_class, datasets=[], credentials={}):
        self.resource = resource
        self.driver = driver_class(resource, credentials)

        self.lock = threading.Lock()
        self.cache = {}

        self.offsetSize = 0
        self.lengthSize = 0
        self.baseAddress = 0
        self.rootAddress = self.readSuperblock()

        workers = []
        for dataset in datasets:
            worker = H5Dataset(self, dataset, credentials)
            thread = threading.Thread(target=worker.readDataset, daemon=True)
            workers.append(thread)
            thread.start()

    #######################
    # ioRequest
    #######################
    def ioRequest(self, pos, size):
        data = None
        # Check if Caching
        if size <= self.CACHE_LINE_SIZE:
            cache_line = (pos + self.baseAddress) & self.CACHE_LINE_MASK
            with self.lock:
                # Populate Cache (if not there already)
                if cache_line not in self.cache:
                    self.cache[cache_line] = self.driver.read(cache_line, self.CACHE_LINE_SIZE)
                # Calculate Start and Stop Indexes into Cache Line
                start_index = (pos + self.baseAddress) - cache_line
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
        else:
            # Direct Read
            data = self.driver.read(pos + self.baseAddress, size)
        # Return Data
        return data


    #######################
    # readSuperblock
    #######################
    def readSuperblock(self):

        # read start of superblock
        block = self.ioRequest(0, 9)
        signature, superblock_version = struct.unpack(f'<QB', block)

        # check file signature
        if signature != self.H5_SIGNATURE_LE:
            raise FatalError(f'invalid file signature: {signature}')

        # check file version
        if superblock_version != 0 and superblock_version != 2:
            raise FatalError(f'unsupported superblock version: {superblock_version}')

        # Super Block Version 0 #
        if superblock_version == 0:
            if errorChecking:
                # read start of superblock
                block = self.ioRequest(9, 2)
                freespace_version, roottable_version = struct.unpack(f'<BB', block)

                # check free space version
                if freespace_version != 0:
                    raise FatalError(f'unsupported free space version: {freespace_version}')

                # check root table version
                if roottable_version != 0:
                    raise FatalError(f'unsupported root table version: {roottable_version}')

            # read sizes
            block = self.ioRequest(13, 2)
            self.offsetSize, self.lengthSize = struct.unpack(f'<BB', block)

            # set base address
            block = self.ioRequest(24, self.offsetSize)
            self.baseAddress = struct.unpack(f'<{SIZE_2_FORMAT[self.offsetSize]}', block)[0]

            # read group offset
            block = self.ioRequest(24 + (5 * self.offsetSize), self.offsetSize)
            root_group_offset = struct.unpack(f'<{SIZE_2_FORMAT[self.offsetSize]}', block)[0]

        # Super Block Version 1 #
        else:
            # read sizes
            block = self.ioRequest(9, 2)
            self.offsetSize, self.lengthSize = struct.unpack(f'<BB', block)

            # set base address
            block = self.ioRequest(12, self.offsetSize)
            self.baseAddress = struct.unpack(f'<{SIZE_2_FORMAT[self.offsetSize]}', block)[0]

            # read group offset
            block = self.ioRequest(12 + (3 * self.offsetSize), self.offsetSize)
            root_group_offset = struct.unpack(f'<{SIZE_2_FORMAT[self.offsetSize]}', block)[0]

        # print file information
        if verbose:
            logger.info(f'File Information @{root_group_offset}')
            logger.info(f'Size of Offsets:      {self.offsetSize}')
            logger.info(f'Size of Lengths:      {self.lengthSize}')
            logger.info(f'Base Address:         {self.baseAddress}')
            logger.info(f'Root Group Offset:    {root_group_offset}')

        # return root group offset
        return root_group_offset
