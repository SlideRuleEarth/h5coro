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

INVALID_VALUE = {
    1: 0xFF,
    2: 0xFFFF,
    4: 0xFFFFFFFF,
    8: 0xFFFFFFFFFFFFFFFF
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
    # local
    H5CORO_CUSTOM_V1_FLAG   = 0x80
    # signatures
    H5_OHDR_SIGNATURE_LE    = 0x5244484F
    H5_FRHP_SIGNATURE_LE    = 0x50485246
    H5_FHDB_SIGNATURE_LE    = 0x42444846
    H5_FHIB_SIGNATURE_LE    = 0x42494846
    H5_OCHK_SIGNATURE_LE    = 0x4B48434F
    H5_TREE_SIGNATURE_LE    = 0x45455254
    H5_HEAP_SIGNATURE_LE    = 0x50414548
    H5_SNOD_SIGNATURE_LE    = 0x444F4E53
    # dimensions
    ALL_ROWS                = -1
    MAX_NDIMS               = 2
    FLAT_NDIMS              = 3
    # datatypes
    FIXED_POINT_TYPE        = 0
    FLOATING_POINT_TYPE     = 1
    TIME_TYPE               = 2
    STRING_TYPE             = 3
    BIT_FIELD_TYPE          = 4
    OPAQUE_TYPE             = 5
    COMPOUND_TYPE           = 6
    REFERENCE_TYPE          = 7
    ENUMERATED_TYPE         = 8
    VARIABLE_LENGTH_TYPE    = 9
    ARRAY_TYPE              = 10
    UNKNOWN_TYPE            = 11
    # layouts
    COMPACT_LAYOUT          = 0
    CONTIGUOUS_LAYOUT       = 1
    CHUNKED_LAYOUT          = 2
    # messages
    DATASPACE_MSG           = 0x1
    LINK_INFO_MSG           = 0x2
    DATATYPE_MSG            = 0x3
    FILL_VALUE_MSG          = 0x5
    LINK_MSG                = 0x6
    DATA_LAYOUT_MSG         = 0x8
    FILTER_MSG              = 0xB
    ATTRIBUTE_MSG           = 0xC
    HEADER_CONT_MSG         = 0x10
    SYMBOL_TABLE_MSG        = 0x11
    ATTRIBUTE_INFO_MSG      = 0x15

    #######################
    # Constructor
    #######################
    def __init__(self, resourceObject, dataset, credentials={}):
        self.resourceObject         = resourceObject
        self.dataset                = dataset
        self.credentials            = credentials
        self.pos                    = self.resourceObject.rootAddress
        self.datasetPath            = dataset.split('/')
        self.datasetLevel           = 0
        self.datasetFound           = False
        self.ndims                  = None
        self.dimensions             = []
        self.typesize               = 0
        self.type                   = None
        self.signedval              = False
        self.fillsize               = 0
        self.fillvalue              = None
        self.layout                 = None
        self.size                   = 0
        self.address                = 0
        self.chunkElements          = 0
        self.chunkDimensions        = []
        self.elementSize            = 0
        self.filter                 = {
            1:  False, # deflate
            2:  False, # shuffle
            3:  False, # fletcher32
            4:  False, # szip
            5:  False, # nbit
            6:  False  # scaleoffset
        }

    #######################
    # readField
    #######################
    def readField(self, size):
        raw = self.resourceObject.ioRequest(self.pos, size)
        self.pos += size
        return struct.unpack(f'<{SIZE_2_FORMAT[size]}', raw)[0]

    #######################
    # readArray
    #######################
    def readArray(self, size):
        raw = self.resourceObject.ioRequest(self.pos, size)
        self.pos += size
        return raw

    #######################
    # readDataset
    #######################
    def readDataset(self):
        self.readObjHdr()

    #######################
    # readObjHdr
    #######################
    def readObjHdr(self):
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

        # skip checksum
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
            if self.datasetFound:
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
        SIZE_OF_V1_PREFIX = 8
        starting_position = self.pos

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
            if self.datasetFound:
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
            self.DATASPACE_MSG:         self.dataspaceMsgHandler,
            self.LINK_INFO_MSG:         self.linkinfoMsgHandler,
            self.DATATYPE_MSG:          self.datatypeMsgHandler,
            self.FILL_VALUE_MSG:        self.fillvalueMsgHandler,
            self.LINK_MSG:              self.linkMsgHandler,
            self.DATA_LAYOUT_MSG:       self.datalayoutMsgHandler,
            self.FILTER_MSG:            self.filterMsgHandler,
            self.ATTRIBUTE_MSG:         self.attributeMsgHandler,
            self.HEADER_CONT_MSG:       self.headercontMsgHandler,
            self.SYMBOL_TABLE_MSG:      self.symboltableMsgHandler,
            self.ATTRIBUTE_INFO_MSG:    self.attributeinfoMsgHandler
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
        MAX_DIM_PRESENT    = 0x1
        PERM_INDEX_PRESENT = 0x2
        starting_position  = self.pos
        version            = self.readField(1)
        dimensionality     = self.readField(1)
        flags              = self.readField(1)
        self.pos          += ((version == 1) and 5 or 1) # go past reserved bytes

        if errorChecking:
            if version != 1 or version != 2:
                raise FatalError(f'unsupported dataspace version: {version}')
            if flags & PERM_INDEX_PRESENT:
                raise FatalError(f'unsupported permutation indexes')
            if dimensionality > self.MAX_NDIMS:
                raise FatalError(f'unsupported number of dimensions: {dimensionality}')

        if verbose:
            logger.info(f'Dataspace Message [{self.datasetLevel}] @{self.pos}')
            logger.info(f'Version:              {version}')
            logger.info(f'Dimensionality:       {dimensionality}')
            logger.info(f'Flags:                {flags}')

        # read and populate data dimensions
        self.ndims = min(dimensionality, self.MAX_NDIMS)
        if self.ndims > 0:
            for x in range(self.ndims):
                dimension = self.readField(self.resourceObject.lengthSize)
                self.dimensions.append(dimension)
                if verbose:
                    logger.info(f'Dimension  {x}:          {dimension}')

            # skip over dimension permutations
            if flags & MAX_DIM_PRESENT:
                skip_bytes = dimensionality * self.resourceObject.lengthSize
                self.pos += skip_bytes

        # return bytes read
        return self.pos - starting_position

    #######################
    # linkinfoMsgHandler
    #######################
    def linkinfoMsgHandler(self, msg_size, obj_hdr_flags):
        MAX_CREATE_PRESENT_BIT      = 0x1
        CREATE_ORDER_PRESENT_BIT    = 0x2
        starting_position           = self.pos
        version                     = self.readField(1)
        flags                       = self.readField(1)

        if errorChecking:
            if version != 0:
                raise FatalError(f'unsupported link info version: {version}')

        if verbose:
            logger.info(f'Link Information Message [{self.datasetLevel}] @{self.pos}')
            logger.info(f'Version:              {version}')
            logger.info(f'Flags:                {flags}')

        # read maximum creation index
        if flags & MAX_CREATE_PRESENT_BIT:
            max_create_index = self.readField(8)
            if verbose:
                logger.info(f'Max Create Index:     {max_create_index}')

        # read heap address and name index
        heap_address = self.readField(self.resourceObject.offsetSize)
        name_index = self.readField(self.resourceObject.offsetSize)
        if verbose:
            logger.info(f'Heap Address:         {heap_address}')
            logger.info(f'Name Index:           {name_index}')

        # read address of v2 B-tree for creation order index
        if flags & CREATE_ORDER_PRESENT_BIT:
            create_order_index = self.readField(self.resourceObject.offsetSize)
            if verbose:
                logger.info(f'Create Order Index:   {create_order_index}')

        # follow heap address if provided
        if heap_address == INVALID_VALUE[self.resourceObject.offsetSize]:
            self.readFractalHeap(self.LINK_MSG, heap_address, obj_hdr_flags)

        # return bytes read
        return self.pos - starting_position

    #######################
    # datatypeMsgHandler
    #######################
    def datatypeMsgHandler(self, msg_size, obj_hdr_flags):
        starting_position           = self.pos
        version_class               = self.readField(4)
        self.typesize               = self.readField(4)
        version                     = (version_class & 0xF0) >> 4
        databits                    = version_class >> 8
        self.type                   = version_class & 0x0F
        self.signedval              = ((databits & 0x08) >> 3) == 1

        if errorChecking and version != 1:
            raise FatalError(f'unsupported datatype version: {version}')

        if verbose:
            logger.info(f'Data Type Message [{self.datasetLevel}] @{self.pos}')
            logger.info(f'Version:              {version}')
            logger.info(f'Type Size:            {self.typesize}')
            logger.info(f'Data Type:            {self.type}')
            logger.info(f'Signed:               {self.signedval}')

        # Fixed Point
        if self.type == self.FIXED_POINT_TYPE:
            if verbose:
                byte_order      = databits & 0x1
                pad_type        = (databits & 0x06) >> 1
                bit_offset      = self.readField(2)
                bit_precision   = self.readField(2)
                logger.info(f'Byte Order:           {byte_order}')
                logger.info(f'Pad Type:             {pad_type}')
                logger.info(f'Bit Offset:           {bit_offset}')
                logger.info(f'Bit Precision:        {bit_precision}')
            else:
                self.pos += 4
        # Floating Point
        elif self.type == self.FLOATING_POINT_TYPE:
            if verbose:
                byte_order      = ((databits & 0x40) >> 5) | (databits & 0x1)
                pad_type        = (databits & 0x0E) >> 1
                mant_norm       = (databits & 0x30) >> 4
                sign_loc        = (databits & 0xFF00) >> 8
                bit_offset      = self.readField(2)
                bit_precision   = self.readField(2)
                exp_location    = self.readField(1)
                exp_size        = self.readField(1)
                mant_location   = self.readField(1)
                mant_size       = self.readField(1)
                exp_bias        = self.readField(4)
                logger.info(f'Byte Order:           {byte_order}')
                logger.info(f'Pad Type:             {pad_type}')
                logger.info(f'Mantissa Norm:        {mant_norm}')
                logger.info(f'Sign Location:        {sign_loc}')
                logger.info(f'Bit Offset:           {bit_offset}')
                logger.info(f'Bit Precision:        {bit_precision}')
                logger.info(f'Exponent Location:    {exp_location}')
                logger.info(f'Exponent Size:        {exp_size}')
                logger.info(f'Mantissa Location:    {mant_location}')
                logger.info(f'Mantissa Size:        {mant_size}')
                logger.info(f'Exponent Bias:        {exp_bias}')
            else:
                self.pos += 12
        # Variable Length
        elif self.type == self.VARIABLE_LENGTH_TYPE:
            if verbose:
                vt_type = databits & 0xF # variable length type
                padding = (databits & 0xF0) >> 4
                charset = (databits & 0xF00) >> 8

                vt_type_str = "unknown"
                if vt_type == 0:
                    vt_type_str = "Sequence"
                elif vt_type == 1:
                    vt_type_str = "String"

                padding_str = "unknown"
                if padding == 0:
                    padding_str = "Null Terminate"
                elif padding == 1:
                    padding_str = "Null Pad"
                elif padding == 2:
                    padding_str = "Space Pad"

                charset_str = "unknown"
                if charset == 0:
                    charset_str = "ASCII"
                elif charset == 1:
                    charset_str = "UTF-8"

                logger.info(f'Variable Type:        {vt_type_str}')
                logger.info(f'Padding Type:         {padding_str}')
                logger.info(f'Character Set:        {charset_str}')

            # unsupported
            raise FatalError(f'variable length data types require reading a global heap, which is not yet supported')
            # self.pos += self.datatypeMsgHandler(msg_size, obj_hdr_flags)
        # String
        elif self.type == self.STRING_TYPE:
            if verbose:
                padding = databits & 0x0F
                charset = (databits & 0xF0) >> 4

                padding_str = "unknown"
                if padding == 0:
                    padding_str = "Null Terminate"
                elif padding == 1:
                    padding_str = "Null Pad"
                elif padding == 2:
                    padding_str = "Space Pad"

                charset_str = "unknown"
                if charset == 0:
                    charset_str = "ASCII"
                elif charset == 1:
                    charset_str = "UTF-8"

                logger.info(f'Padding Type:         {padding_str}')
                logger.info(f'Character Set:        {charset_str}')
        # Default
        elif errorChecking:
            raise FatalError(f'unsupported datatype: {self.type}')

        # return bytes read
        return self.pos - starting_position

    #######################
    # fillvalueMsgHandler
    #######################
    def fillvalueMsgHandler(self, msg_size, obj_hdr_flags):
        FILL_VALUE_DEFINED = 0x20
        starting_position = self.pos

        version = self.readField(1)

        if errorChecking and (version != 2) and (version != 3):
            raise FatalError(f'invalid fill value version: {version}')

        if verbose:
            logger.info(f'Fill Value Message [{self.datasetLevel}] @{self.pos}')
            logger.info(f'Version:              {version}')

        # Version 2
        if version == 2:
            if verbose:
                space_allocation_time = self.readField(1)
                fill_value_write_time = self.readField(1)
                logger.info(f'Space Allocation Time:{space_allocation_time}')
                logger.info(f'Fill Value Write Time:{fill_value_write_time}')
            else:
                self.pos += 2

            fill_value_defined = self.readField(1)
            if fill_value_defined:
                self.fillsize = self.readField(4)
                if self.fillsize > 0:
                    self.fillvalue = self.readField(self.fillsize)
        # Version 3
        else:
            flags = self.readField(1)
            if verbose:
                logger.info(f'Fill Flags:           {flags}')

            if flags & FILL_VALUE_DEFINED:
                self.fillsize = self.readField(4)
                self.fillvalue = self.readField(self.fillsize)

        if verbose:
            logger.info(f'Fill Value Size:      {self.fillsize}')
            logger.info(f'Fill Value:           {self.fillvalue}')

        # return bytes read
        return self.pos - starting_position

    #######################
    # linkMsgHandler
    #######################
    def linkMsgHandler(self, msg_size, obj_hdr_flags):
        SIZE_OF_LEN_OF_NAME_MASK    = 0x03
        CREATE_ORDER_PRESENT_BIT    = 0x04
        LINK_TYPE_PRESENT_BIT       = 0x08
        CHAR_SET_PRESENT_BIT        = 0x10
        HARD_LINK                   = 0
        SOFT_LINK                   = 1
        EXTERNAL_LINK               = 64
        starting_position           = self.pos
        version                     = self.readField(1)
        flags                       = self.readField(1)

        if errorChecking and version != 1:
            raise FatalError(f'unsupported link message version: {version}')

        # read link type
        link_type = 0 # default to hard link
        if flags & LINK_TYPE_PRESENT_BIT:
            link_type = self.readField(1)

        # read creation order
        create_order = None
        if flags & CREATE_ORDER_PRESENT_BIT:
            create_order = self.readField(8)

        # read character set
        char_set = None
        if flags & CHAR_SET_PRESENT_BIT:
            char_set = self.readField(1)

        # read link name
        link_name_len_of_len = 1 << (flags & SIZE_OF_LEN_OF_NAME_MASK)
        if errorChecking and (link_name_len_of_len > 8):
            raise FatalError(f'invalid link name length of length: {link_name_len_of_len}')
        link_name_len = self.readField(link_name_len_of_len)
        link_name = self.readArray(link_name_len).decode('utf-8')

        if verbose:
            logger.info(f'Link Message [{self.datasetLevel}] @{self.pos}')
            logger.info(f'Version:              {version}')
            logger.info(f'Flags:                {flags}')
            logger.info(f'Link Type:            {link_type}')
            logger.info(f'Creation Order:       {create_order}')
            logger.info(f'Character Set:        {char_set}')
            logger.info(f'Link Name:            {link_name}')

        follow_link = False
        if link_name == self.datasetPath[self.datasetLevel]:
            self.datasetLevel += 1
            follow_link = True

        # process link
        if link_type == HARD_LINK:
            obj_hdr_addr = self.readField(self.resourceObject.offsetSize)
            if verbose:
                logger.info(f'Hard Link:            {obj_hdr_addr}')
            if follow_link:
                return_position = self.pos
                self.pos = obj_hdr_addr
                self.readObjHdr()
                self.pos = return_position

        elif link_type == SOFT_LINK:
            soft_link_len = self.readField(2)
            soft_link = self.readArray(soft_link_len).decode('utf-8')
            if verbose:
                logger.info(f'Soft Link:            {soft_link}')
            if errorChecking and follow_link:
                raise FatalError(f'unsupported soft link encountered: {soft_link}')

        elif link_type == EXTERNAL_LINK:
            ext_link_len = self.readField(2)
            ext_link = self.readArray(ext_link_len).decode('utf-8')
            if verbose:
                logger.info(f'External Link:        {ext_link}')
            if errorChecking and follow_link:
                raise FatalError(f'unsupported external link encountered: {ext_link}')

        elif errorChecking:
            raise FatalError(f'unsupported link type: {link_type}')

        # return bytes read
        return self.pos - starting_position

    #######################
    # datalayoutMsgHandler
    #######################
    def datalayoutMsgHandler(self, msg_size, obj_hdr_flags):
        starting_position   = self.pos
        version             = self.readField(1)
        self.layout         = self.readField(1)

        if errorChecking and version != 3:
            raise FatalError(f'invalid data layout version: {version}')

        if verbose:
            logger.info(f'Data Layout Message [{self.datasetLevel}] @{self.pos}')
            logger.info(f'Version:              {version}')
            logger.info(f'Layout:               {self.layout}')

        # read layouts
        if self.layout == self.COMPACT_LAYOUT:
            self.size = self.readField(2)
            self.address = self.pos
            self.pos += self.size
        elif self.layout == self.CONTIGUOUS_LAYOUT:
            self.address = self.readField(self.resourceObject.offsetSize)
            self.size = self.readField(self.resourceObject.lengthSize)
        elif self.layout == self.CHUNKED_LAYOUT:
            # read number of dimensions
            chunk_num_dim = self.readField(1) - 1  # dimensionality is plus one over actual number of dimensions
            chunk_num_dim = min(chunk_num_dim, self.MAX_NDIMS)
            if errorChecking and (self.ndims != None) and (chunk_num_dim != self.ndims):
                raise FatalError(f'number of chunk dimensions does not match dimensionality of data: {chunk_num_dim} != {self.ndims}')
            # read address of B-tree
            self.address = self.readField(self.resourceObject.offsetSize)
            # read chunk dimensions
            if chunk_num_dim > 0:
                self.chunkElements = 1
                for _ in range(chunk_num_dim):
                    chunk_dimension = self.readField(4)
                    self.chunkDimensions.append(chunk_dimension)
                    self.chunkElements *= chunk_dimension
            # read element size
            self.elementSize = self.readField(4)
            # verbose
            if verbose:
                logger.info(f'Element Size:         {self.elementSize}')
                logger.info(f'# Chunked Dimensions: {chunk_num_dim}')
                for d in range(chunk_num_dim):
                    logger.info(f'Chunk Dimension {d}:    {self.chunkDimensions[d]}')
        elif errorChecking:
            raise FatalError(f'unsupported data layout: {self.layout}')

        # verbose
        if verbose:
            logger.info(f'Dataset Size:         {self.size}')
            logger.info(f'Dataset Address:      {self.address}')

        # return bytes read
        return self.pos - starting_position

    #######################
    # filterMsgHandler
    #######################
    def filterMsgHandler(self, msg_size, obj_hdr_flags):
        starting_position   = self.pos
        version             = self.readField(1)
        num_filters         = self.readField(1)
        if errorChecking and (version != 1) and (version != 2):
            raise FatalError(f'invalid filter version: {version}')

        if verbose:
            logger.info(f'Filter Message [{self.datasetLevel}] @{self.pos}')
            logger.info(f'Version:              {version}')
            logger.info(f'Num Filters:          {num_filters}')

        # move past reserved bytes in version 1
        if version == 1:
            self.pos += 6

        # read filters
        for f in range(num_filters):
            # read filter id
            filter = self.readField(2)

            # read filter name length
            name_len = 0
            if (version == 1) or (filter >= 256):
                name_len = self.readField(2)

            # read Filter parameters
            flags     = self.readField(2)
            num_parms = self.readField(2)

            # consistency check flags
            if errorChecking and (flags != 0) and (flags != 1):
                raise FatalError(f'invalid flags in filter message: {flags}')

            # read name
            filter_name = ""
            if name_len > 0:
                filter_name = self.readArray(name_len).decode('utf-8')
                name_padding = (8 - (name_len % 8)) % 8
                self.pos += name_padding

            # display
            if verbose:
                logger.info(f'Filter ID:            {filter}')
                logger.info(f'Flags:                {flags}')
                logger.info(f'# Parameters:         {num_parms}')
                logger.info(f'Filter Name:          {filter_name}')

            # set filter
            try:
                self.filter[filter] = True
            except Exception:
                raise FatalError(f'unsupported filter specified: {filter}')

            # read client data
            self.pos += num_parms * 4

            # handle padding (version 1 only)
            if (version == 1) and (num_parms % 2 == 1):
                self.pos += 4

        # return bytes read
        return self.pos - starting_position

    #######################
    # attributeMsgHandler
    #######################
    def attributeMsgHandler(self, msg_size, obj_hdr_flags):
        starting_position   = self.pos
        version             = self.readField(1)
        self.pos           += 1
        name_size           = self.readField(2)
        datatype_size       = self.readField(2)
        dataspace_size      = self.readField(2)

        if errorChecking and (version != 1):
            raise FatalError(f'invalid attribute version: {version}')

        # read attribute name
        attr_name = self.readArray(name_size).decode('utf-8')
        self.pos += (8 - (name_size % 8)) % 8; # align to next 8-byte boundary

        # display
        if verbose:
            logger.info(f'Attribute Message [{self.datasetLevel}] @{self.pos}')
            logger.info(f'Version:              {version}')
            logger.info(f'Name:                 {attr_name}')
            logger.info(f'Message Size:         {msg_size}')
            logger.info(f'Datatype Size:        {datatype_size}')
            logger.info(f'Dataspace Size:       {dataspace_size}')

        # check if desired attribute
        if( ((self.datasetLevel + 1) == self.datasetPath.length()) and
            (attr_name == self.datasetPath[self.datasetLevel]) ):
            self.datasetFound = True

            # read datatype message
            datatype_bytes_read = self.datatypeMsgHandler(datatype_size, obj_hdr_flags)
            if errorChecking and (datatype_bytes_read > datatype_size):
                raise FatalError(f'failed to read expected bytes for datatype message: {datatype_bytes_read} > {datatype_size}')
            self.pos += datatype_bytes_read
            self.pos += (8 - (datatype_bytes_read % 8)) % 8 # align to next 8-byte boundary

            # read dataspace message
            dataspace_bytes_read = self.dataspaceMsgHandler(dataspace_size, obj_hdr_flags)
            if errorChecking and (dataspace_bytes_read > dataspace_size):
                raise FatalError(f'failed to read expected bytes for dataspace message: {dataspace_bytes_read} > {dataspace_size}')
            self.pos += dataspace_bytes_read
            self.pos += (8 - (dataspace_bytes_read % 8)) % 8 # align to next 8-byte boundary

            # set meta data
            self.layout = self.CONTIGUOUS_LAYOUT
            for f in filter.keys():
                filter[f] = False
            self.address = self.pos
            self.size = msg_size - (self.pos - starting_position)

            # move to end of data
            self.pos += self.size

            # return bytes read
            return self.pos - starting_position
        else:
            # skip processing message
            return msg_size

    #######################
    # headercontMsgHandler
    #######################
    def headercontMsgHandler(self, msg_size, obj_hdr_flags):
        starting_position   = self.pos
        hc_offset           = self.readField(self.resourceObject.offsetSize)
        hc_length           = self.readField(self.resourceObject.lengthSize)

        if verbose:
            logger.info(f'Header Continuation Message [{self.datasetLevel}] @{self.pos}')
            logger.info(f'Offset:               {hc_offset}')
            logger.info(f'Length:               {hc_length}')

        # go to continuation block
        return_position = self.pos
        self.pos = hc_offset

        # read continuation block
        if obj_hdr_flags & self.H5CORO_CUSTOM_V1_FLAG:
            end_of_chdr = hc_offset + hc_length
            self.pos += self.readMessagesV1 (end_of_chdr, obj_hdr_flags)
        else:
            # read signature
            if errorChecking:
                signature = self.readField(4)
                if signature != self.H5_OCHK_SIGNATURE_LE:
                    raise FatalError(f'invalid header continuation signature: {signature}')
            else:
                self.pos += 4

            # read continuation header messages
            end_of_chdr = hc_offset + hc_length - 4 # leave 4 bytes for checksum below
            self.pos += self.readMessages (end_of_chdr, obj_hdr_flags)

            # skip checksum
            self.pos += 4

        # return bytes read
        bytes_read = self.resourceObject.offsetSize + self.resourceObject.lengthSize
        self.pos = return_position + bytes_read
        return bytes_read

    #######################
    # symboltableMsgHandler
    #######################
    def symboltableMsgHandler(self, msg_size, obj_hdr_flags):
        starting_position   = self.pos
        btree_addr          = self.readField(self.resourceObject.offsetSize)
        heap_addr           = self.readField(self.resourceObject.offsetSize)
        return_position     = self.pos

        if verbose:
            logger.info(f'Symbol Table Message [{self.datasetLevel}] @{self.pos}')
            logger.info(f'B-Tree Address:       {btree_addr}')
            logger.info(f'Heap Address:         {heap_addr}')

        # read heap info
        self.pos = heap_addr
        if errorChecking:
            signature = self.readField(4)
            version = self.readField(1)
            if signature != self.H5_HEAP_SIGNATURE_LE:
                raise FatalError(f'invalid heap signature: {signature}')
            if version != 0:
                raise FatalError(f'unsupported version of heap: {version}')
            self.pos += 19
        else:
            self.pos += 24
        head_data_addr = self.readField(self.resourceObject.offsetSize)

        # go to left-most node
        self.pos = btree_addr
        while True:
            # read header info
            if errorChecking:
                signature = self.readField(4)
                node_type = self.readField(1)
                if signature != self.H5_TREE_SIGNATURE_LE:
                    raise FatalError(f'invalid group b-tree signature: {signature}')
                if node_type != 0:
                    raise FatalError(f'only group b-trees supported: {node_type}')
            else:
                self.pos += 5

            # read branch info
            node_level = self.readField(1)
            if node_level == 0:
                break
            else:
                self.pos += 2 + (2 * self.resourceObject.offsetSize) + self.resourceObject.lengthSize # skip entries used, sibling addresses, and first key
                self.pos = self.readField(self.resourceObject.offsetSize) # read and go to first child

        # traverse children left to right */
        while True:
            entries_used    = self.readField(2)
            left_sibling    = self.readField(self.resourceObject.offsetSize)
            right_sibling   = self.readField(self.resourceObject.offsetSize)
            key0            = self.readField(self.resourceObject.lengthSize)
            if verbose:
                logger.info(f'Entries Used:         {entries_used}')
                logger.info(f'Left Sibling:         {left_sibling}')
                logger.info(f'Right Sibling:        {right_sibling}')
                logger.info(f'First Key:            {key0}')

            # loop through entries in current node
            for _ in range(entries_used):
                symbol_table_addr = self.readField(self.resourceObject.offsetSize)
                current_node_pos = self.pos
                self.pos = symbol_table_addr
                self.readSymbolTable(head_data_addr)
                self.pos = current_node_pos
                self.pos += self.resourceObject.lengthSize # skip next key
                if self.datasetFound:
                    break

            # exit loop or go to next node
            if (right_sibling == INVALID_VALUE[self.resourceObject.offsetSize]) or self.datasetFound:
                break
            else:
                self.pos = right_sibling

            # read header info
            if errorChecking:
                signature = self.readField(4)
                node_type = self.readField(1)
                node_level = self.readField(1)
                if signature != self.H5_TREE_SIGNATURE_LE:
                    raise FatalError(f'invalid group b-tree signature: {signature}')
                if node_type != 0:
                    raise FatalError(f'only group b-trees supported: {node_type}')
                if node_level != 0:
                    raise FatalError(f'traversed to non-leaf node: {node_level}')
            else:
                self.pos += 6

        # return bytes read
        bytes_read = self.resourceObject.offsetSize + self.resourceObject.lengthSize
        self.pos = return_position + bytes_read
        return bytes_read

    #######################
    # attributeinfoMsgHandler
    #######################
    def attributeinfoMsgHandler(self, msg_size, obj_hdr_flags):
        MAX_CREATE_PRESENT_BIT      = 0x01
        CREATE_ORDER_PRESENT_BIT    = 0x02
        starting_position           = self.pos
        version                     = self.readField(1)
        flags                       = self.readField(1)

        if errorChecking and (version != 0):
            raise FatalError(f'unsupported link info version: {version}')

        if verbose:
            logger.info(f'Attribute Info [{self.datasetLevel}] @{self.pos}')
            logger.info(f'Version:              {version}')
            logger.info(f'Flags:                {flags}')

        # read maximum creation index (number of elements in group)
        if flags & MAX_CREATE_PRESENT_BIT:
            max_create_index = self.readField(2)
            if verbose:
                logger.info(f'Max Creation Index:   {max_create_index}')

        # read heap and name offsets
        heap_address    = self.readField(self.resourceObject.offsetSize)
        name_index      = self.readField(self.resourceObject.offsetSize)
        if verbose:
            logger.info(f'Heap Address:         {heap_address}')
            logger.info(f'Name Index:           {name_index}')

        # read creation order index
        if flags & CREATE_ORDER_PRESENT_BIT:
            create_order_index = self.readField(self.resourceObject.offsetSize)
            if verbose:
                logger.info(f'Creation Order Index: {create_order_index}')

        # follow heap address if provided */
        if heap_address == INVALID_VALUE[self.resourceObject.offsetSize]:
            self.readFractalHeap(self.ATTRIBUTE_MSG, heap_address, obj_hdr_flags)

        # return bytes read
        return self.pos - starting_position

    #######################
    # readFractalHeap
    #######################
    def readFractalHeap(self, msg_type, heap_address, obj_hdr_flags):
        pass

    #######################
    # readSymbolTable
    #######################
    def readSymbolTable(self, heap_address):
        pass

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
