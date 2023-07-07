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

from datetime import datetime
import concurrent.futures
import threading
import struct
import logging
import zlib
import sys
import numpy

###############################################################################
# CONSTANTS
###############################################################################

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

###############################################################################
# GLOBALS
###############################################################################

logger = logging.getLogger(__name__)

###############################################################################
# OPTIONS
###############################################################################

errorCheckingOption = True
verboseOption = False
enableAttributesOption = False
logLevelOption = None
logFormatOption = '%(created)f %(levelname)-5s [%(filename)s:%(lineno)5d] %(message)s'
cacheLineSizeOption = 0x400000
enablePrefetchOption = False
def config( errorChecking=errorCheckingOption,
            verbose=verboseOption,
            enableAttributes=enableAttributesOption,
            logLevel=logLevelOption,
            logFormat=logFormatOption,
            cacheLineSize=cacheLineSizeOption,
            enablePrefetch=enablePrefetchOption ):
    global errorCheckingOption, verboseOption, enableAttributesOption, cacheLineSizeOption, enablePrefetchOption
    errorCheckingOption = errorChecking
    verboseOption = verbose
    enableAttributesOption = enableAttributes
    cacheLineSizeOption = cacheLineSize
    enablePrefetchOption = enablePrefetch
    if logLevel != None:
        logging.basicConfig(stream=sys.stdout, level=logLevel, format=logFormat)

###############################################################################
# EXCEPTIONS
###############################################################################

class FatalError(RuntimeError):
    pass

###############################################################################
# H5Values Class
###############################################################################

class H5Values:

    #######################
    # Constructor
    #######################
    def __init__(self, _elements, _datasize, _numrows, _numcols, _datatype, _values):
        self.elements   = _elements
        self.datasize   = _datasize
        self.numrows    = _numrows
        self.numcols    = _numcols
        self.datatype   = _datatype
        self.values     = _values

    #######################
    # operator: []
    #######################
    def __getitem__(self, key):
        return self.values[key]

    #######################
    # length
    #######################
    def __len__(self):
        return len(self.values)

    #######################
    # representation
    #######################
    def __repr__(self):
        return f'{{"elements": {self.elements}, "datasize": {self.datasize}, "numrows": {self.numrows}, "numcols": {self.numcols}, "datatype": {self.datatype}, "values": {self.values}}}'

    #######################
    # print
    #######################
    def __str__(self):
        return self.__repr__()

    #######################
    # tolist
    #######################
    def tolist(self):
        return list(self.values)

###############################################################################
# H5Dataset Class
###############################################################################

class H5Dataset:

    #######################
    # Constants
    #######################
    # local
    CUSTOM_V1_FLAG          = 0x80
    ALL_ROWS                = -1
    MAX_NDIMS               = 2
    FLAT_NDIMS              = 3
    # signatures
    H5_OHDR_SIGNATURE_LE    = 0x5244484F
    H5_FRHP_SIGNATURE_LE    = 0x50485246
    H5_FHDB_SIGNATURE_LE    = 0x42444846
    H5_FHIB_SIGNATURE_LE    = 0x42494846
    H5_OCHK_SIGNATURE_LE    = 0x4B48434F
    H5_TREE_SIGNATURE_LE    = 0x45455254
    H5_HEAP_SIGNATURE_LE    = 0x50414548
    H5_SNOD_SIGNATURE_LE    = 0x444F4E53
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
    # filters
    DEFLATE_FILTER          = 1
    SHUFFLE_FILTER          = 2
    FLETCHER32_FILTER       = 3
    SZIP_FILTER             = 4
    NBIT_FILTER             = 5
    SCALEOFFSET_FILTER      = 6
    # data type conversion
    TO_DATATYPE = {
        FIXED_POINT_TYPE: {
            True: {
                1:  numpy.int8,
                2:  numpy.int16,
                4:  numpy.int32,
                8:  numpy.int64
            },
            False: {
                1:  numpy.uint8,
                2:  numpy.uint16,
                4:  numpy.uint32,
                8:  numpy.uint64
            }
        },
        FLOATING_POINT_TYPE: {
            True: {
                4:  numpy.single,
                8:  numpy.double
            }
        },
        STRING_TYPE: {
            True: {
                1:  numpy.byte
            }
        }
    }

    #######################
    # Constructor
    #######################
    def __init__(self, resourceObject, dataset, startRow=0, numRows=ALL_ROWS, metaOnly=False):
        self.resourceObject         = resourceObject
        self.metaOnly               = metaOnly
        self.pos                    = self.resourceObject.rootAddress
        self.dataset                = dataset
        self.datasetStartRow        = startRow
        self.datasetNumRows         = numRows
        self.datasetPath            = list(filter(('').__ne__, self.dataset.split('/')))
        self.datasetPathLevels      = len(self.datasetPath)
        self.datasetFound           = False
        self.ndims                  = None
        self.dimensions             = []
        self.typeSize               = 0
        self.type                   = None
        self.signedval              = True
        self.fillsize               = 0
        self.fillvalue              = None
        self.layout                 = None
        self.size                   = 0
        self.address                = 0
        self.chunkElements          = 0
        self.chunkDimensions        = []
        self.elementSize            = 0
        self.dataChunkBufferSize    = []
        self.filter                 = {
            self.DEFLATE_FILTER:        False,
            self.SHUFFLE_FILTER:        False,
            self.FLETCHER32_FILTER:     False,
            self.SZIP_FILTER:           False,
            self.NBIT_FILTER:           False,
            self.SCALEOFFSET_FILTER:    False
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
        # traverse file for dataset
        dataset_level = 0
        self.readObjHdr(dataset_level)

        # sanity check data attrbutes
        if self.typeSize <= 0:
            raise FatalError(f'missing data type information')
        elif self.ndims == None:
            raise FatalError(f'missing data dimension information')
        elif self.address == INVALID_VALUE[self.resourceObject.offsetSize]:
            raise FatalError(f'invalid data address')

        # calculate size of data row (note dimension starts at 1)
        row_size = self.typeSize
        for d in range(1, self.ndims):
            row_size *= self.dimensions[d]

        # get number of rows
        first_dimension = (self.ndims > 0) and self.dimensions[0] or 1
        self.datasetNumRows = (self.datasetNumRows == self.ALL_ROWS) and first_dimension or self.datasetNumRows
        if (self.datasetStartRow + self.datasetNumRows) > first_dimension:
            raise FatalError(f'read exceeds number of rows: {self.datasetStartRow} + {self.datasetNumRows} > {first_dimension}')

        # calculate size of buffer
        buffer_size = row_size * self.datasetNumRows

        # calculate buffer start */
        buffer_offset = row_size * self.datasetStartRow

        # check if data address and data size is valid
        if errorCheckingOption:
            if (self.size != 0) and (self.size < (buffer_offset + buffer_size)):
                raise FatalError(f'read exceeds available data: {self.size} < {buffer_offset} + {buffer_size}')
            if (self.filter[self.DEFLATE_FILTER] or self.filter[self.SHUFFLE_FILTER]) and \
               ((self.layout == self.COMPACT_LAYOUT) or (self.layout == self.CONTIGUOUS_LAYOUT)):
                raise FatalError(f'filters unsupported on non-chunked layouts')

        # read dataset
        if not self.metaOnly and (buffer_size > 0):
            if (self.layout == self.COMPACT_LAYOUT) or (self.layout == self.CONTIGUOUS_LAYOUT):
                data_addr = self.address + buffer_offset
                buffer = self.resourceObject.ioRequest(data_addr, buffer_size, caching=False)
            elif self.layout == self.CHUNKED_LAYOUT:
                # chunk layout specific error checks
                if errorCheckingOption:
                    if self.elementSize != self.typeSize:
                        raise FatalError(f'chunk element size does not match data element size: {self.elementSize} !=  {self.typeSize}')
                    elif self.chunkElements <= 0:
                        raise FatalError(f'invalid number of chunk elements: {self.chunkElements}')

                # allocate and initialize buffer
                buffer = bytearray(buffer_size)

                # fill buffer with fill value (if provided)
                if self.fillsize > 0:
                    fill_values = struct.pack('Q', self.fillvalue)[:self.fillsize]
                    for i in range(0, buffer_size, self.fillsize):
                        buffer[i:i+self.fillsize] = fill_values

                # calculate data chunk buffer size
                self.dataChunkBufferSize = self.chunkElements * self.typeSize

                # perform prefetch
                if enablePrefetchOption:
                    if buffer_offset < buffer_size:
                        self.resourceObject.ioRequest(self.address, buffer_offset + buffer_size, caching=False, prefetch=True)
                    else:
                        self.resourceObject.ioRequest(self.address + buffer_offset, buffer_size, caching=False, prefetch=True)

                # read b-tree
                self.pos = self.address
                self.readBTreeV1(buffer, buffer_offset, dataset_level)

                # check need to flatten chunks
                flatten = False
                for d in range(1, self.ndims):
                    if self.chunkDimensions[d] != self.dimensions[d]:
                        flatten = True
                        break

                # flatten chunks - place dataset in row order
                if flatten:
                    # new flattened buffer
                    fbuf = numpy.empty(buffer_size, dtype=numpy.byte)
                    bi = 0 # index into source buffer

                    # build number of each chunk per dimension
                    cdimnum = [0 for _ in range(self.MAX_NDIMS * 2)]
                    for i in range(self.ndims):
                        cdimnum[i] = self.dimensions[i] / self.chunkDimensions[i]
                        cdimnum[i + self.ndims] = self.chunkDimensions[i]

                    # build size of each chunk per flattened dimension
                    cdimsizes = [0 for _ in range(self.FLAT_NDIMS)]
                    cdimsizes[0] = self.chunkDimensions[0] * self.typeSize  # number of chunk rows
                    for i in range(1, self.ndims):
                        cdimsizes[0] *= cdimnum[i]                          # number of columns of chunks
                        cdimsizes[0] *= self.chunkDimensions[i]             # number of columns in chunks
                    cdimsizes[1] = self.typeSize
                    for i in range(1, self.ndims):
                        cdimsizes[1] *= self.chunkDimensions[i]             # number of columns in chunks
                    cdimsizes[2] = self.typeSize
                    for i in range(1, self.ndims):
                        cdimsizes[2] *= cdimnum[i]                          # number of columns of chunks
                        cdimsizes[2] *= self.chunkDimensions[i]             # number of columns in chunks

                    # initialize loop variables
                    ci = self.FLAT_NDIMS - 1;                               # chunk dimension index
                    dimi = [0 for _ in range(self.MAX_NDIMS * 2)]           # chunk dimension indices

                    # loop through each chunk
                    while True:
                        # calculate start position
                        start = 0
                        for i in range(self.FLAT_NDIMS):
                            start += dimi[i] * cdimsizes[i]

                        # copy into new buffer
                        for k in range(cdimsizes[1]):
                            fbuf[start + k] = buffer[bi]
                            bi += 1

                        # update indices
                        dimi[ci] += 1
                        while dimi[ci] == cdimnum[ci]:
                            dimi[ci] = 0
                            ci -= 1
                            if ci < 0:
                                break
                            else:
                                dimi[ci] += 1

                        # check exit condition
                        if ci < 0:
                            break
                        else:
                            ci = self.FLAT_NDIMS - 1

                    # replace buffer
                    buffer = fbuf

            elif errorCheckingOption:
                raise FatalError(f'invalid data layout: {self.layout}')

        try:
            # set results
            numcols = 0
            if self.ndims == 0:
                numcols = 0
            elif self.ndims == 1:
                numcols = 1
            elif self.ndims >= 2:
                numcols = self.dimensions[1]
            elements = int(buffer_size / self.typeSize)
            datatype = self.TO_DATATYPE[self.type][self.signedval][self.typeSize]

            # fulfill h5 future
            h5values = H5Values(elements,
                                buffer_size,
                                self.datasetNumRows,
                                numcols,
                                datatype,
                                numpy.frombuffer(buffer, dtype=datatype, count=elements))
        except Exception as e:
            raise FatalError(f'unable to populate results for {self.resourceObject.resource}/{self.dataset}: {e}')

        # return result of reading dataset back to caller
        return self.dataset, h5values

    #######################
    # readObjHdr
    #######################
    def readObjHdr(self, dlvl):
        # check mata data table
        for lvl in range(self.datasetPathLevels, dlvl, -1):
            group_path = '/'.join(self.datasetPath[:lvl])
            if group_path in self.resourceObject.metaDataTable:
                self.pos = self.resourceObject.metaDataTable[group_path]
                self.resourceObject.metaDataHits += 1
                dlvl = lvl
        # process header
        version_peek = self.readField(1)
        self.pos -= 1
        if version_peek == 1:
            self.readObjHdrV1(dlvl)
        else:
            self.readObjHdrV0(dlvl)

    #######################
    # readObjHdrV0
    #######################
    def readObjHdrV0(self, dlvl):
        FILE_STATS_BIT          = 0x20
        STORE_CHANGE_PHASE_BIT  = 0x10
        SIZE_OF_CHUNK_0_MASK    = 0x3
        starting_position       = self.pos

        # display
        if verboseOption:
            logger.info(f'<<Object Information V0 - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')

        # check signature and version
        if errorCheckingOption:
            signature = self.readField(4)
            version = self.readField(1)
            if signature != self.H5_OHDR_SIGNATURE_LE:
                raise FatalError(f'invalid version 0 object header signature: 0x{signature:x}')
            if version != 2:
                raise FatalError(f'unsupported header version: {version}')
        else:
            self.pos += 5

        # file stats
        obj_hdr_flags = self.readField(1)
        if obj_hdr_flags & FILE_STATS_BIT:
            if verboseOption:
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
        if obj_hdr_flags & STORE_CHANGE_PHASE_BIT:
            if verboseOption:
                max_compact_attr = self.readField(2)
                max_dense_attr = self.readField(2)
                logger.info(f'Max Compact Attr:     {max_compact_attr}')
                logger.info(f'Max Dense Attr:       {max_dense_attr}')
            else:
                self.pos += 4

        # read header messages
        size_of_chunk0 = self.readField(1 << (obj_hdr_flags & SIZE_OF_CHUNK_0_MASK))
        end_of_hdr = self.pos + size_of_chunk0
        self.readMessagesV0(end_of_hdr, obj_hdr_flags, dlvl)

        # skip checksum
        self.pos += 4

        # return bytes read
        return self.pos - starting_position

    #######################
    # readMessagesV0
    #######################
    def readMessagesV0(self, end_of_hdr, obj_hdr_flags, dlvl):
        ATTR_CREATION_TRACK_BIT = 0x4
        starting_position       = self.pos

        while self.pos < end_of_hdr:
            # read message info
            msg_type = self.readField(1)
            msg_size = self.readField(2)
            msg_flags = self.readField(1)

            # read messag order
            if obj_hdr_flags & ATTR_CREATION_TRACK_BIT:
                msg_order = self.readField(2)

            # read message
            bytes_read = self.readMessage(msg_type, msg_size, obj_hdr_flags, dlvl)
            if errorCheckingOption and (bytes_read != msg_size):
                raise FatalError(f'header v0 message different size than specified: {bytes_read} != {msg_size}')

            # check if dataset found
            if not self.metaOnly and self.datasetFound:
                self.pos = end_of_hdr # go directory to end of header
                break # exit loop because dataset is found

        # check bytes read
        if errorCheckingOption and (self.pos < end_of_hdr):
            raise FatalError(f'did not read enough v0 bytes: 0x{self.pos:x} < 0x{end_of_hdr:x}')

        # return bytes read
        return self.pos - starting_position

    #######################
    # readObjHdrV1
    #######################
    def readObjHdrV1(self, dlvl):
        starting_position = self.pos
        self.pos += 2 # version and reserved field

        if verboseOption:
            # read number of header messages
            num_hdr_msgs = self.readField(2)
            logger.info(f'<<Object Information V1 - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
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
        self.readMessagesV1(end_of_hdr, self.CUSTOM_V1_FLAG, dlvl)

        # return bytes read
        return self.pos - starting_position

    #######################
    # readMessagesV1
    #######################
    def readMessagesV1(self, end_of_hdr, obj_hdr_flags, dlvl):
        SIZE_OF_V1_PREFIX = 8
        starting_position = self.pos

        while self.pos < (end_of_hdr - SIZE_OF_V1_PREFIX):
            # read message info
            msg_type = self.readField(2)
            msg_size = self.readField(2)
            msg_flags = self.readField(1)

            # read reserved fields
            if errorCheckingOption:
                reserved1 = self.readField(1)
                reserved2 = self.readField(2)
                if reserved1 != 0 and reserved2 != 0:
                    raise FatalError(f'invalid reserved fields: {reserved1},{reserved2}')
            else:
                self.pos += 3

            # read message
            bytes_read = self.readMessage(msg_type, msg_size, obj_hdr_flags, dlvl)
            alignment_padding = ((8 - (bytes_read % 8)) % 8) # align to 8-byte boundary
            self.pos += alignment_padding
            bytes_read += alignment_padding
            if errorCheckingOption and (bytes_read != msg_size):
                raise FatalError(f'header v1 message different size than specified: {bytes_read} != {msg_size}')

            # check if dataset found
            if not self.metaOnly and self.datasetFound:
                self.pos = end_of_hdr # go directory to end of header
                break # exit loop because dataset is found

        # move past gap
        if self.pos < end_of_hdr:
            self.pos = end_of_hdr

        # check bytes read
        if errorCheckingOption and (self.pos < end_of_hdr):
            raise FatalError(f'did not read enough v1 bytes: 0x{self.pos:x} < 0x{end_of_hdr:x}')

        # return bytes read
        return self.pos - starting_position

    #######################
    # readMessage
    #######################
    def readMessage(self, msg_type, msg_size, obj_hdr_flags, dlvl):
        # default handlers
        msg_handler_table = {
            self.DATASPACE_MSG:         self.dataspaceMsgHandler,
            self.LINK_INFO_MSG:         self.linkinfoMsgHandler,
            self.DATATYPE_MSG:          self.datatypeMsgHandler,
            self.FILL_VALUE_MSG:        self.fillvalueMsgHandler,
            self.LINK_MSG:              self.linkMsgHandler,
            self.DATA_LAYOUT_MSG:       self.datalayoutMsgHandler,
            self.FILTER_MSG:            self.filterMsgHandler,
            self.HEADER_CONT_MSG:       self.headercontMsgHandler,
            self.SYMBOL_TABLE_MSG:      self.symboltableMsgHandler,
        }
        # attrubite handlers
        if enableAttributesOption:
            msg_handler_table[self.ATTRIBUTE_MSG] = self.attributeMsgHandler
            msg_handler_table[self.ATTRIBUTE_INFO_MSG] = self.attributeinfoMsgHandler
        # process message
        try:
            return msg_handler_table[msg_type](msg_size, obj_hdr_flags, dlvl)
        except KeyError:
            if verboseOption:
                logger.info(f'<<Skipped Message - {self.dataset}[{dlvl}] @0x{self.pos:x}: 0x{msg_type:x}, {msg_size}>>')
            self.pos += msg_size
            return msg_size

    #######################
    # dataspaceMsgHandler
    #######################
    def dataspaceMsgHandler(self, msg_size, obj_hdr_flags, dlvl):
        MAX_DIM_PRESENT    = 0x1
        PERM_INDEX_PRESENT = 0x2
        starting_position  = self.pos
        version            = self.readField(1)
        dimensionality     = self.readField(1)
        flags              = self.readField(1)
        self.pos          += ((version == 1) and 5 or 1) # go past reserved bytes

        if verboseOption:
            logger.info(f'<<Dataspace Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            logger.info(f'Version:              {version}')
            logger.info(f'Dimensionality:       {dimensionality}')
            logger.info(f'Flags:                {flags}')

        # check version and flags and dimenstionality
        if errorCheckingOption:
            if version != 1 and version != 2:
                raise FatalError(f'unsupported dataspace version: {version}')
            if flags & PERM_INDEX_PRESENT:
                raise FatalError(f'unsupported permutation indexes')
            if dimensionality > self.MAX_NDIMS:
                raise FatalError(f'unsupported number of dimensions: {dimensionality}')

        # read and populate data dimensions
        self.ndims = min(dimensionality, self.MAX_NDIMS)
        if self.ndims > 0:
            for x in range(self.ndims):
                dimension = self.readField(self.resourceObject.lengthSize)
                self.dimensions.append(dimension)
                if verboseOption:
                    logger.info(f'Dimension {x}:          {dimension}')

            # skip over dimension permutations
            if flags & MAX_DIM_PRESENT:
                skip_bytes = dimensionality * self.resourceObject.lengthSize
                self.pos += skip_bytes

        # return bytes read
        return self.pos - starting_position

    #######################
    # linkinfoMsgHandler
    #######################
    def linkinfoMsgHandler(self, msg_size, obj_hdr_flags, dlvl):
        MAX_CREATE_PRESENT_BIT      = 0x1
        CREATE_ORDER_PRESENT_BIT    = 0x2
        starting_position           = self.pos
        version                     = self.readField(1)
        flags                       = self.readField(1)

        # display
        if verboseOption:
            logger.info(f'<<Link Information Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            logger.info(f'Version:              {version}')
            logger.info(f'Flags:                {flags}')

        # check version
        if errorCheckingOption and (version != 0):
            raise FatalError(f'unsupported link info version: {version}')

        # read maximum creation index
        if flags & MAX_CREATE_PRESENT_BIT:
            max_create_index = self.readField(8)
            if verboseOption:
                logger.info(f'Max Create Index:     {max_create_index}')

        # read heap address and name index
        heap_address = self.readField(self.resourceObject.offsetSize)
        name_index = self.readField(self.resourceObject.offsetSize)
        if verboseOption:
            logger.info(f'Heap Address:         0x{heap_address:x}')
            logger.info(f'Name Index:           0x{name_index:x}')

        # read address of v2 B-tree for creation order index
        if flags & CREATE_ORDER_PRESENT_BIT:
            create_order_index = self.readField(self.resourceObject.offsetSize)
            if verboseOption:
                logger.info(f'Create Order Index:   0x{create_order_index:x}')

        # follow heap address if provided
        if heap_address != INVALID_VALUE[self.resourceObject.offsetSize]:
            return_position = self.pos
            self.pos = heap_address
            self.readFractalHeap(self.LINK_MSG, obj_hdr_flags, dlvl)
            self.pos = return_position

        # return bytes read
        return self.pos - starting_position

    #######################
    # datatypeMsgHandler
    #######################
    def datatypeMsgHandler(self, msg_size, obj_hdr_flags, dlvl):
        starting_position           = self.pos
        version_class               = self.readField(4)
        self.typeSize               = self.readField(4)
        version                     = (version_class & 0xF0) >> 4
        databits                    = version_class >> 8
        self.type                   = version_class & 0x0F
        self.signedval              = ((databits & 0x08) >> 3) == 1

        # display
        if verboseOption:
            logger.info(f'<<Data Type Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            logger.info(f'Version:              {version}')
            logger.info(f'Type Size:            {self.typeSize}')
            logger.info(f'Data Type:            {self.type}')
            logger.info(f'Signed:               {self.signedval}')

        # check version
        if errorCheckingOption and version != 1:
            raise FatalError(f'unsupported datatype version: {version}')

        # Fixed Point
        if self.type == self.FIXED_POINT_TYPE:
            if verboseOption:
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
            self.signedval = True
            if verboseOption:
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
            if verboseOption:
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
            self.typeSize = 1
            self.signedval = True
            if verboseOption:
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
        elif errorCheckingOption:
            raise FatalError(f'unsupported datatype: {self.type}')

        # return bytes read
        return self.pos - starting_position

    #######################
    # fillvalueMsgHandler
    #######################
    def fillvalueMsgHandler(self, msg_size, obj_hdr_flags, dlvl):
        FILL_VALUE_DEFINED  = 0x20
        starting_position   = self.pos
        version             = self.readField(1)

        # display
        if verboseOption:
            logger.info(f'<<Fill Value Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            logger.info(f'Version:              {version}')

        # check version
        if errorCheckingOption and (version != 2) and (version != 3):
            raise FatalError(f'invalid fill value version: {version}')

        # version 2
        if version == 2:
            if verboseOption:
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
        # version 3
        else:
            flags = self.readField(1)
            if verboseOption:
                logger.info(f'Fill Flags:           {flags}')

            if flags & FILL_VALUE_DEFINED:
                self.fillsize = self.readField(4)
                self.fillvalue = self.readField(self.fillsize)

        # display
        if verboseOption:
            logger.info(f'Fill Value Size:      {self.fillsize}')
            logger.info(f'Fill Value:           {self.fillvalue}')

        # return bytes read
        return self.pos - starting_position

    #######################
    # linkMsgHandler
    #######################
    def linkMsgHandler(self, msg_size, obj_hdr_flags, dlvl):
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

        # display
        if verboseOption:
            logger.info(f'<<Link Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            logger.info(f'Version:              {version}')
            logger.info(f'Flags:                {flags}')

        # check version
        if errorCheckingOption and version != 1:
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
        if errorCheckingOption and (link_name_len_of_len > 8):
            raise FatalError(f'invalid link name length of length: {link_name_len_of_len}')
        link_name_len = self.readField(link_name_len_of_len)
        link_name = self.readArray(link_name_len).tobytes().decode('utf-8')

        # display
        if verboseOption:
            logger.info(f'Link Type:            {link_type}')
            logger.info(f'Creation Order:       {create_order}')
            logger.info(f'Character Set:        {char_set}')
            logger.info(f'Link Name:            {link_name}')

        # check if follow link
        follow_link = False
        if dlvl < len(self.datasetPath) and link_name == self.datasetPath[dlvl]:
            follow_link = True

        # process link
        if link_type == HARD_LINK:
            obj_hdr_addr = self.readField(self.resourceObject.offsetSize)
            if verboseOption:
                logger.info(f'Hard Link:            0x{obj_hdr_addr:x}')
            # update meta data table
            group_path = '/'.join(self.datasetPath[:dlvl] + [link_name])
            self.resourceObject.metaDataTable[group_path] = obj_hdr_addr
            # follow link
            if follow_link:
                return_position = self.pos
                self.pos = obj_hdr_addr
                self.readObjHdr(dlvl + 1)
                self.pos = return_position
                # dataset found
                if (dlvl + 1) == len(self.datasetPath):
                    self.datasetFound = True

        elif link_type == SOFT_LINK:
            soft_link_len = self.readField(2)
            soft_link = self.readArray(soft_link_len).tobytes().decode('utf-8')
            if verboseOption:
                logger.info(f'Soft Link:            {soft_link}')
            if errorCheckingOption and follow_link:
                raise FatalError(f'unsupported soft link encountered: {soft_link}')

        elif link_type == EXTERNAL_LINK:
            ext_link_len = self.readField(2)
            ext_link = self.readArray(ext_link_len).tobytes().decode('utf-8')
            if verboseOption:
                logger.info(f'External Link:        {ext_link}')
            if errorCheckingOption and follow_link:
                raise FatalError(f'unsupported external link encountered: {ext_link}')

        elif errorCheckingOption:
            raise FatalError(f'unsupported link type: {link_type}')

        # return bytes read
        return self.pos - starting_position

    #######################
    # datalayoutMsgHandler
    #######################
    def datalayoutMsgHandler(self, msg_size, obj_hdr_flags, dlvl):
        starting_position   = self.pos
        version             = self.readField(1)
        self.layout         = self.readField(1)

        # display
        if verboseOption:
            logger.info(f'<<Data Layout Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            logger.info(f'Version:              {version}')
            logger.info(f'Layout:               {self.layout}')

        # check version
        if errorCheckingOption and version != 3:
            raise FatalError(f'invalid data layout version: {version}')

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
            if errorCheckingOption and (self.ndims != None) and (chunk_num_dim != self.ndims):
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
            # display
            if verboseOption:
                logger.info(f'Element Size:         {self.elementSize}')
                logger.info(f'# Chunked Dimensions: {chunk_num_dim}')
                for d in range(chunk_num_dim):
                    logger.info(f'Chunk Dimension {d}:    {self.chunkDimensions[d]}')
        elif errorCheckingOption:
            raise FatalError(f'unsupported data layout: {self.layout}')

        # display
        if verboseOption:
            logger.info(f'Dataset Size:         {self.size}')
            logger.info(f'Dataset Address:      {self.address}')

        # return bytes read
        return self.pos - starting_position

    #######################
    # filterMsgHandler
    #######################
    def filterMsgHandler(self, msg_size, obj_hdr_flags, dlvl):
        starting_position   = self.pos
        version             = self.readField(1)
        num_filters         = self.readField(1)

        # display
        if verboseOption:
            logger.info(f'<<Filter Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            logger.info(f'Version:              {version}')
            logger.info(f'Num Filters:          {num_filters}')

        # check version
        if errorCheckingOption and (version != 1) and (version != 2):
            raise FatalError(f'invalid filter version: {version}')

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
            if errorCheckingOption and (flags != 0) and (flags != 1):
                raise FatalError(f'invalid flags in filter message: {flags}')

            # read name
            filter_name = ""
            if name_len > 0:
                filter_name = self.readArray(name_len).tobytes().decode('utf-8')
                name_padding = (8 - (name_len % 8)) % 8
                self.pos += name_padding

            # display
            if verboseOption:
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
    def attributeMsgHandler(self, msg_size, obj_hdr_flags, dlvl):
        starting_position   = self.pos
        version             = self.readField(1)
        self.pos           += 1
        name_size           = self.readField(2)
        datatype_size       = self.readField(2)
        dataspace_size      = self.readField(2)

        # display
        if verboseOption:
            logger.info(f'<<Attribute Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            logger.info(f'Version:              {version}')

        # check version
        if errorCheckingOption and (version != 1):
            raise FatalError(f'invalid attribute version: {version}')

        # read attribute name
        attr_name = self.readArray(name_size).tobytes().decode('utf-8')
        self.pos += (8 - (name_size % 8)) % 8; # align to next 8-byte boundary

        # display
        if verboseOption:
            logger.info(f'Name:                 {attr_name}')
            logger.info(f'Message Size:         {msg_size}')
            logger.info(f'Datatype Size:        {datatype_size}')
            logger.info(f'Dataspace Size:       {dataspace_size}')

        # check if desired attribute
        if( ((dlvl + 1) == len(self.datasetPath)) and
            (attr_name == self.datasetPath[dlvl]) ):
            self.datasetFound = True

            # read datatype message
            datatype_bytes_read = self.datatypeMsgHandler(datatype_size, obj_hdr_flags, dlvl)
            if errorCheckingOption and (datatype_bytes_read > datatype_size):
                raise FatalError(f'failed to read expected bytes for datatype message: {datatype_bytes_read} > {datatype_size}')
            self.pos += datatype_bytes_read
            self.pos += (8 - (datatype_bytes_read % 8)) % 8 # align to next 8-byte boundary

            # read dataspace message
            dataspace_bytes_read = self.dataspaceMsgHandler(dataspace_size, obj_hdr_flags, dlvl)
            if errorCheckingOption and (dataspace_bytes_read > dataspace_size):
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
            self.pos = starting_position + msg_size
            return msg_size

    #######################
    # headercontMsgHandler
    #######################
    def headercontMsgHandler(self, msg_size, obj_hdr_flags, dlvl):
        starting_position   = self.pos
        hc_offset           = self.readField(self.resourceObject.offsetSize)
        hc_length           = self.readField(self.resourceObject.lengthSize)

        # display
        if verboseOption:
            logger.info(f'<<Header Continuation Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            logger.info(f'Offset:               0x{hc_offset:x}')
            logger.info(f'Length:               {hc_length}')

        # go to continuation block
        return_position = self.pos
        self.pos = hc_offset

        # read continuation block
        if obj_hdr_flags & self.CUSTOM_V1_FLAG:
            end_of_chdr = hc_offset + hc_length
            self.readMessagesV1(end_of_chdr, obj_hdr_flags, dlvl)
        else:
            # read signature
            if errorCheckingOption:
                signature = self.readField(4)
                if signature != self.H5_OCHK_SIGNATURE_LE:
                    raise FatalError(f'invalid header continuation signature: 0x{signature:x}')
            else:
                self.pos += 4

            # read continuation header messages
            end_of_chdr = hc_offset + hc_length - 4 # leave 4 bytes for checksum below
            self.readMessagesV0(end_of_chdr, obj_hdr_flags, dlvl)

            # skip checksum
            self.pos += 4

        # return bytes read
        bytes_read = self.resourceObject.offsetSize + self.resourceObject.lengthSize
        self.pos = return_position
        return bytes_read

    #######################
    # symboltableMsgHandler
    #######################
    def symboltableMsgHandler(self, msg_size, obj_hdr_flags, dlvl):
        starting_position   = self.pos
        btree_addr          = self.readField(self.resourceObject.offsetSize)
        heap_addr           = self.readField(self.resourceObject.offsetSize)
        return_position     = self.pos

        # display
        if verboseOption:
            logger.info(f'<<Symbol Table Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            logger.info(f'B-Tree Address:       {btree_addr}')
            logger.info(f'Heap Address:         {heap_addr}')

        # read heap info
        self.pos = heap_addr
        if errorCheckingOption:
            signature = self.readField(4)
            version = self.readField(1)
            if signature != self.H5_HEAP_SIGNATURE_LE:
                raise FatalError(f'invalid heap signature: 0x{signature:x}')
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
            if errorCheckingOption:
                signature = self.readField(4)
                node_type = self.readField(1)
                if signature != self.H5_TREE_SIGNATURE_LE:
                    raise FatalError(f'invalid group b-tree signature: 0x{signature:x}')
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
            if verboseOption:
                logger.info(f'Entries Used:         {entries_used}')
                logger.info(f'Left Sibling:         {left_sibling}')
                logger.info(f'Right Sibling:        {right_sibling}')
                logger.info(f'First Key:            {key0}')

            # loop through entries in current node
            for _ in range(entries_used):
                symbol_table_addr = self.readField(self.resourceObject.offsetSize)
                current_node_pos = self.pos
                self.pos = symbol_table_addr
                self.readSymbolTable(head_data_addr, dlvl)
                self.pos = current_node_pos
                self.pos += self.resourceObject.lengthSize # skip next key
                if not self.metaOnly and self.datasetFound:
                    break

            # exit loop or go to next node
            if (right_sibling == INVALID_VALUE[self.resourceObject.offsetSize]) or (not self.metaOnly and self.datasetFound):
                break
            else:
                self.pos = right_sibling

            # read header info
            if errorCheckingOption:
                signature = self.readField(4)
                node_type = self.readField(1)
                node_level = self.readField(1)
                if signature != self.H5_TREE_SIGNATURE_LE:
                    raise FatalError(f'invalid group b-tree signature: 0x{signature:x}')
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
    def attributeinfoMsgHandler(self, msg_size, obj_hdr_flags, dlvl):
        MAX_CREATE_PRESENT_BIT      = 0x01
        CREATE_ORDER_PRESENT_BIT    = 0x02
        starting_position           = self.pos
        version                     = self.readField(1)
        flags                       = self.readField(1)

        # display
        if verboseOption:
            logger.info(f'<<Attribute Info - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            logger.info(f'Version:              {version}')
            logger.info(f'Flags:                {flags}')

        # check version
        if errorCheckingOption and (version != 0):
            raise FatalError(f'unsupported link info version: {version}')

        # read maximum creation index (number of elements in group)
        if flags & MAX_CREATE_PRESENT_BIT:
            max_create_index = self.readField(2)
            if verboseOption:
                logger.info(f'Max Creation Index:   {max_create_index}')

        # read heap and name offsets
        heap_address    = self.readField(self.resourceObject.offsetSize)
        name_index      = self.readField(self.resourceObject.offsetSize)
        if verboseOption:
            logger.info(f'Heap Address:         {heap_address}')
            logger.info(f'Name Index:           {name_index}')

        # read creation order index
        if flags & CREATE_ORDER_PRESENT_BIT:
            create_order_index = self.readField(self.resourceObject.offsetSize)
            if verboseOption:
                logger.info(f'Creation Order Index: {create_order_index}')

        # follow heap address if provided */
        if heap_address != INVALID_VALUE[self.resourceObject.offsetSize]:
            return_position = self.pos
            self.pos = heap_address
            self.readFractalHeap(self.ATTRIBUTE_MSG, obj_hdr_flags, dlvl)
            self.pos = return_position

        # return bytes read
        return self.pos - starting_position

    #######################
    # readSymbolTable
    #######################
    def readSymbolTable(self, heap_address, dlvl):
        starting_position = self.pos

        # display
        if verboseOption:
            logger.info(f'<<Symbol Table - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')

        # check signature and version
        if errorCheckingOption:
            signature = self.readField(4)
            version = self.readField(1)
            if signature != self.H5_SNOD_SIGNATURE_LE:
                raise FatalError(f'invalid symbol table signature: 0x{signature:x}')
            if version != 1:
                raise FatalError(f'incorrect version of symbole table: {version}')
            self.pos += 1
        else:
            self.pos += 6

        # read symbols
        num_symbols = self.readField(2)
        for _ in range(num_symbols):
            # read symbol entry
            link_name_offset    = self.readField(self.resourceObject.offsetsize)
            obj_hdr_addr        = self.readField(self.resourceObject.offsetsize)
            cache_type          = self.readField(4)
            self.pos += 20 # reserved + scratch pad

            # read link name
            return_position = self.pos
            link_name_addr = heap_address + link_name_offset
            self.pos = link_name_addr
            link_name_chars = []
            while True:
                c = self.readArray(1).tobytes().decode('utf-8')
                if c == '\0':
                    break
                link_name_chars.append(c)
            link_name = ''.join(link_name_chars)
            self.pos = return_position

            # display
            if verboseOption:
                logger.info(f'Link Name:            {link_name}')
                logger.info(f'Obj Hdr Addr:         {obj_hdr_addr}')

            # update meta data table
            group_path = '/'.join(self.datasetPath[:dlvl] + [link_name])
            self.resourceObject.metaDataTable[group_path] = obj_hdr_addr

            # process link
            return_position = self.pos
            if dlvl < len(self.datasetPath) and link_name == self.datasetPath[dlvl]:
                if cache_type == 2:
                    raise FatalError(f'symbolic links are unsupported: {link_name}')
                self.readObjHdr(obj_hdr_addr, dlvl + 1)
                self.pos = return_position
                if not self.metaOnly:
                    break # datasetFound

        # return bytes read
        return self.pos - starting_position

    #######################
    # readFractalHeap
    #######################
    def readFractalHeap(self, msg_type, obj_hdr_flags, dlvl):
        FRHP_CHECKSUM_DIRECT_BLOCKS = 0x02
        starting_position           = self.pos

        # read fractal heap header
        signature           = self.readField(4)
        version             = self.readField(1)
        heap_obj_id_len     = self.readField(2) # Heap ID Length
        io_filter_len       = self.readField(2) # I/O Filters' Encoded Length
        flags               = self.readField(1) # Flags
        max_size_mg_obj     = self.readField(4) # Maximum Size of Managed Objects
        next_huge_obj_id    = self.readField(self.resourceObject.lengthSize) # Next Huge Object ID
        btree_addr_huge_obj = self.readField(self.resourceObject.offsetSize) # v2 B-tree Address of Huge Objects
        free_space_mg_blks  = self.readField(self.resourceObject.lengthSize) # Amount of Free Space in Managed Blocks
        addr_free_space_mg  = self.readField(self.resourceObject.offsetSize) # Address of Managed Block Free Space Manager
        mg_space            = self.readField(self.resourceObject.lengthSize) # Amount of Manged Space in Heap
        alloc_mg_space      = self.readField(self.resourceObject.lengthSize) # Amount of Allocated Managed Space in Heap
        dblk_alloc_iter     = self.readField(self.resourceObject.lengthSize) # Offset of Direct Block Allocation Iterator in Managed Space
        mg_objs             = self.readField(self.resourceObject.lengthSize) # Number of Managed Objects in Heap
        huge_obj_size       = self.readField(self.resourceObject.lengthSize) # Size of Huge Objects in Heap
        huge_objs           = self.readField(self.resourceObject.lengthSize) # Number of Huge Objects in Heap
        tiny_obj_size       = self.readField(self.resourceObject.lengthSize) # Size of Tiny Objects in Heap
        tiny_objs           = self.readField(self.resourceObject.lengthSize) # Number of Tiny Objects in Heap
        table_width         = self.readField(2) # Table Width
        starting_blk_size   = self.readField(self.resourceObject.lengthSize) # Starting Block Size
        max_dblk_size       = self.readField(self.resourceObject.lengthSize) # Maximum Direct Block Size
        max_heap_size       = self.readField(2) # Maximum Heap Size
        start_num_rows      = self.readField(2) # Starting # of Rows in Root Indirect Block
        root_blk_addr       = self.readField(self.resourceObject.offsetSize) # Address of Root Block
        curr_num_rows       = self.readField(2) # Current # of Rows in Root Indirect Block

        # display
        if verboseOption:
            logger.info(f'<<Fractal Heap - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            logger.info(f'Heap ID Length:       {heap_obj_id_len}')
            logger.info(f'I/O Filters Length:   {io_filter_len}')
            logger.info(f'Flags:                {flags}')
            logger.info(f'Max Size of Objects:  {max_size_mg_obj}')
            logger.info(f'Next Huge Object ID:  {next_huge_obj_id}')
            logger.info(f'v2 B-tree Address:    0x{btree_addr_huge_obj:x}')
            logger.info(f'Free Space in Blocks: {free_space_mg_blks}')
            logger.info(f'Address Free Space:   0x{addr_free_space_mg:x}')
            logger.info(f'Managed Space:        {mg_space}')
            logger.info(f'Allocated Heap Space: {alloc_mg_space}')
            logger.info(f'Direct Block Offset:  0x{dblk_alloc_iter:x}')
            logger.info(f'Managed Heap Objects: {mg_objs}')
            logger.info(f'Size of Huge Objects: {huge_obj_size}')
            logger.info(f'Huge Objects in Heap: {huge_objs}')
            logger.info(f'Size of Tiny Objects: {tiny_obj_size}')
            logger.info(f'Tiny Objects in Heap: {tiny_objs}')
            logger.info(f'Table Width:          {table_width}')
            logger.info(f'Starting Block Size:  {starting_blk_size}')
            logger.info(f'Max Direct Block Size:{max_dblk_size}')
            logger.info(f'Max Heap Size:        {max_heap_size}')
            logger.info(f'Starting # of Rows:   {start_num_rows}')
            logger.info(f'Address of Root Block:0x{root_blk_addr:x}')
            logger.info(f'Current # of Rows:    {curr_num_rows}')

        # check signature and version
        if errorCheckingOption:
            if signature != self.H5_FRHP_SIGNATURE_LE:
                raise FatalError(f'invalid heap signature: 0x{signature:x}')
            if version != 0:
                raise FatalError(f'unsupported heap version: {version}')

        # read filter information
        if io_filter_len > 0:
            filter_root_dblk   = self.readField(self.resourceObject.lengthSize) # Size of Filtered Root Direct Block
            filter_mask        = self.readField(4) # I/O Filter Mask
            logger.info(f'Filtered Direct Block:{filter_root_dblk}')
            logger.info(f'I/O Filter Mask:      {filter_mask}')
            raise FatalError(f'Filtering unsupported on fractal heap: {io_filter_len}')
            # self.readMessage(FILTER_MSG, io_filter_len, obj_hdr_flags) # this currently populates filter for dataset

        # skip checksum
        self.pos += 4

        # build heap info object
        heap_info = {
            'table_width': table_width,
            'curr_num_rows': curr_num_rows,
            'starting_blk_size': starting_blk_size,
            'max_dblk_size': max_dblk_size,
            'blk_offset_size': int((max_heap_size + 7) / 8),
            'dblk_checksum': ((flags & FRHP_CHECKSUM_DIRECT_BLOCKS) != 0),
            'msg_type': msg_type,
            'num_objects': mg_objs,
            'cur_objects': 0 # updated as objects are read
        }

        # move to root block
        return_position = self.pos
        self.pos = root_blk_addr

        # process blocks
        if heap_info['curr_num_rows'] == 0:
            # direct blocks
            bytes_read = self.readDirectBlock(heap_info, heap_info['starting_blk_size'], obj_hdr_flags, dlvl)
            self.pos = return_position + heap_info['starting_blk_size']
        else:
            # indirect blocks
            bytes_read = self.readIndirectBlock(heap_info, 0, obj_hdr_flags, dlvl)
            self.pos = return_position + bytes_read

        # check bytes read
        if errorCheckingOption and (bytes_read > heap_info['starting_blk_size']):
            raise FatalError(f'block contianed more bytes than specified: {bytes_read} > {heap_info["starting_blk_size"]}')

        # return bytes read
        return self.pos - starting_position

    #######################
    # readDirectBlock
    #######################
    def readDirectBlock(self, heap_info, block_size, obj_hdr_flags, dlvl):
        starting_position = self.pos

        # display
        if verboseOption:
            logger.info(f'<<Direct Block - {self.dataset}[{dlvl}] @0x{starting_position:x}: {heap_info["msg_type"]}, {block_size}>>')

        # check signature and version
        if errorCheckingOption:
            signature = self.readField(4)
            version = self.readField(1)
            if signature != self.H5_FHDB_SIGNATURE_LE:
                raise FatalError(f'invalid direct block signature: 0x{signature:x}')
            if version != 0:
                raise FatalError(f'invalid direct block version: {version}')
        else:
            self.pos += 5

        # read block header
        if verboseOption:
            heap_hdr_addr = self.readField(self.resourceObject.offsetSize) # Heap Header Address
            logger.info(f'Heap Header Address:  {heap_hdr_addr}')
        else:
            self.pos += self.resourceObject.offsetSize
        self.pos += heap_info['blk_offset_size'] # block offset reading is not supported because size can be non-standard integer size (like 3, 5, 6, 7)

        # skip checksum
        if heap_info['dblk_checksum']:
            self.pos += 4

        # read block data
        data_left = block_size - (5 + self.resourceObject.offsetSize + heap_info['blk_offset_size'] + (heap_info['dblk_checksum'] * 4))
        while data_left > 0:
            # peak if more messages
            early_exit = False
            peak_size = min((1 << self.highestBit(data_left)), 8)
            peak_addr = self.pos
            if self.readField(peak_size) == 0:
                early_exit = True
            self.pos = peak_addr
            if early_exit:
                if verboseOption:
                    logger.info(f'exiting direct block 0x{starting_position:x} early at 0x{self.pos:x}')
                break

            # read message
            data_read = self.readMessage(heap_info['msg_type'], data_left, obj_hdr_flags, dlvl)
            data_left -= data_read

            # update number of objects read
            #   there are often more links in a heap than managed objects;
            #   therefore, the number of objects cannot be used to know when
            #   to stop reading links
            heap_info['cur_objects'] += 1

            # check reading past block
            if errorCheckingOption and data_left < 0:
                raise FatalError(f'reading message exceeded end of direct block: {starting_position}')

            # check if dataset found
            if not self.metaOnly and self.datasetFound:
                break

        # skip to end of block (useful only if exited loop above early)
        self.pos += data_left

        # return bytes read
        return self.pos - starting_position

    #######################
    # readIndirectBlock
    #######################
    def readIndirectBlock(self, heap_info, block_size, obj_hdr_flags, dlvl):
        starting_position = self.pos

        # display
        if verboseOption:
            logger.info(f'<<Indirect Block - {self.dataset}[{dlvl}] @0x{starting_position:x}: {heap_info["msg_type"]}, {block_size}>>')

        # check signature and version
        if errorCheckingOption:
            signature = self.readField(4)
            version = self.readField(1)
            if signature != self.H5_FHIB_SIGNATURE_LE:
                raise FatalError(f'invalid direct block signature: 0x{signature:x}')
            if version != 0:
                raise FatalError(f'unsupported direct block version: {version}')
        else:
            self.pos += 5

        # read block header
        if verboseOption:
            heap_hdr_addr = self.readField(self.resourceObject.offsetSize) # Heap Header Address
            logger.info(f'Heap Header Address:  {heap_hdr_addr}')
        else:
            self.pos += self.resourceObject.offsetSize
        self.pos += heap_info['blk_offset_size'] # block offset reading is not supported because size can be non-standard integer size (like 3, 5, 6, 7)

        # calculate number of direct and indirect blocks (see III.G. Disk Format: Level 1G - Fractal Heap)
        nrows = heap_info['curr_num_rows'] # used for "root" indirect block only
        curr_size = heap_info['starting_blk_size'] * heap_info['table_width']
        if block_size > 0:
            nrows = (self.highestBit(block_size) - self.highestBit(curr_size)) + 1
        max_dblock_rows = (self.highestBit(heap_info['max_dblk_size']) - self.highestBit(heap_info['starting_blk_size'])) + 2
        K = min(nrows, max_dblock_rows) * heap_info['table_width']
        N = K - (max_dblock_rows * heap_info['table_width'])
        if verboseOption:
            logger.info(f'Number of Rows:       {nrows}')
            logger.info(f'Max Direct Block Rows:{max_dblock_rows}')
            logger.info(f'# Direct Blocks (K):  {K}')
            logger.info(f'# Indirect Blocks (N):{N}')

        # read direct child blocks
        for row in range(nrows):
            # calculate row's block size
            if row == 0:
                row_block_size = heap_info['starting_blk_size']
            elif row == 1:
                row_block_size = heap_info['starting_blk_size']
            else:
                row_block_size = heap_info['starting_blk_size'] * (0x2 << (row - 2))

            # process entries in row
            for entry in range(heap_info['table_width']):
                # direct block entry
                if row_block_size <= heap_info['max_dblk_size']:
                    if errorCheckingOption and (row >= K):
                        raise FatalError(f'unexpected direct block row: {row_block_size}, {row} >= {K}')

                    # read direct block address
                    direct_block_addr = self.readField(self.resourceObject.offsetSize)
                    # note: filters are unsupported, but if present would be read here
                    if direct_block_addr != INVALID_VALUE[self.resourceObject.offsetSize] and not self.datasetFound:
                        # read direct block
                        return_position = self.pos
                        self.pos = direct_block_addr
                        bytes_read = self.readDirectBlock(heap_info, row_block_size, obj_hdr_flags, dlvl)
                        self.pos = return_position
                        if errorCheckingOption and (bytes_read > row_block_size):
                            raise FatalError(f'direct block contained more bytes than specified: {bytes_read} > {row_block_size}')
                elif errorCheckingOption and ((row < K) or (row >= N)):
                    raise FatalError(f'unexpected indirect block row: {row_block_size}, {row}, {N}')
                else:
                    # read indirect block address
                    indirect_block_addr = self.readField(self.resourceObject.offsetSize)
                    if indirect_block_addr != INVALID_VALUE[self.resourceObject.offsetSize] and not self.datasetFound:
                        # read indirect block
                        return_position = self.pos
                        self.pos = indirect_block_addr
                        bytes_read = self.readIndirectBlock(heap_info, row_block_size, obj_hdr_flags, dlvl)
                        self.pos = return_position
                        if errorCheckingOption and (bytes_read > row_block_size):
                            raise FatalError(f'indirect block contained more bytes than specified: {bytes_read} > {row_block_size}')

        # skip checksum
        self.pos += 4

        # return bytes read
        return self.pos - starting_position

    #######################
    # readBTreeV1
    #######################
    def readBTreeV1(self, buffer, buffer_offset, dlvl):
        starting_position = self.pos
        data_key1 = self.datasetStartRow
        data_key2 = self.datasetStartRow + self.datasetNumRows - 1

        # display
        if verboseOption:
            logger.info(f'<<B-Tree Node - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')

        # check signature and node type
        if errorCheckingOption:
            signature = self.readField(4)
            node_type = self.readField(1)
            if signature != self.H5_TREE_SIGNATURE_LE:
                raise FatalError(f'invalid b-tree signature: 0x{signature:x}')
            if node_type != 1:
                raise FatalError(f'only raw data chunk b-trees supported: {node_type}')
        else:
            self.pos += 5

        # read node level and number of entries
        node_level = self.readField(1)
        entries_used = self.readField(2)

        # display
        if verboseOption:
            logger.info(f'Node Level:           {node_level}')
            logger.info(f'Entries Used:         {entries_used}')

        # skip sibling addresses
        self.pos += self.resourceObject.offsetSize * 2

        # read first key
        curr_node = self.readBTreeNodeV1(self.ndims)

        # read children
        for e in range(entries_used):
            child_addr  = self.readField(self.resourceObject.offsetSize)
            next_node   = self.readBTreeNodeV1(self.ndims)
            child_key1  = curr_node['row_key']
            child_key2  = next_node['row_key'] # there is always +1 keys
            if (next_node['chunk_size'] == 0) and (self.ndims > 0):
                child_key2 = self.dimensions[0]

            # display
            if verboseOption:
                logger.debug(f'Entry <{node_level}>:            {e}')
                logger.debug(f'Chunk Size:           {curr_node["chunk_size"]} | {next_node["chunk_size"]}')
                logger.debug(f'Filter Mask:          {curr_node["filter_mask"]} | {next_node["filter_mask"]}')
                logger.debug(f'Chunk Key:            {child_key1} | {child_key2}')
                logger.debug(f'Data Key:             {data_key1} | {data_key2}')
                logger.debug(f'Slice:                {" ".join([str(d) for d in curr_node["slice"]])}')
                logger.debug(f'Child Address:        0x{child_addr:x}')

            # check inclusion
            if  (data_key1  >= child_key1 and data_key1  <  child_key2) or \
                (data_key2  >= child_key1 and data_key2  <  child_key2) or \
                (child_key1 >= data_key1  and child_key1 <= data_key2)  or \
                (child_key2 >  data_key1  and child_key2 <  data_key2):
                # process child entry
                if node_level > 0:
                    return_position = self.pos
                    self.pos = child_addr
                    self.readBTreeV1(buffer, buffer_offset, dlvl)
                    self.pos = return_position
                else:
                    # calculate chunk location
                    chunk_offset = 0
                    for i in range(self.ndims):
                        slice_size = curr_node['slice'][i] * self.typeSize
                        for k in range(i):
                            slice_size *= self.chunkDimensions[k]
                        for j in range(i + 1, self.ndims):
                            slice_size *= self.dimensions[j]
                        chunk_offset += slice_size

                    # calculate buffer index - offset into data buffer to put chunked data
                    buffer_index = 0
                    if chunk_offset > buffer_offset:
                        buffer_index = chunk_offset - buffer_offset
                        if buffer_index >= len(buffer):
                            raise FatalError(f'invalid location to read data: {chunk_offset}, {buffer_offset}')

                    # calculate chunk index - offset into chunk buffer to read from
                    chunk_index = 0
                    if buffer_offset > chunk_offset:
                        chunk_index = buffer_offset - chunk_offset
                        if chunk_index >= self.dataChunkBufferSize:
                            raise FatalError (f'invalid location to read chunk: {chunk_offset}, {buffer_offset}')

                    # calculate chunk bytes - number of bytes to read from chunk buffer
                    chunk_bytes = self.dataChunkBufferSize - chunk_index
                    if chunk_bytes < 0:
                        raise FatalError(f'no bytes of chunk data to read: {chunk_bytes}, {chunk_index}')
                    elif (buffer_index + chunk_bytes) > len(buffer):
                        chunk_bytes = len(buffer) - buffer_index

                    # display
                    if verboseOption:
                        logger.debug(f'Chunk Offset:         {chunk_offset} ({int(chunk_offset/self.typeSize)})')
                        logger.debug(f'Buffer Index:         {buffer_index} ({int(buffer_index/self.typeSize)})')
                        logger.debug(f'Chunk Bytes:          {chunk_bytes} ({int(chunk_bytes/self.typeSize)})')

                    # read chunk
                    if self.filter[self.DEFLATE_FILTER]:

                        # read data into chunk filter buffer (holds the compressed data)
                        self.dataChunkFilterBuffer = self.resourceObject.ioRequest(child_addr, curr_node['chunk_size'])

                        # inflate directly into data buffer
                        if (chunk_bytes == self.dataChunkBufferSize) and (not self.filter[self.SHUFFLE_FILTER]):
                            buffer[buffer_index:buffer_index+chunk_bytes] = self.inflateChunk(self.dataChunkFilterBuffer)

                        # inflate into data chunk buffer */
                        else:
                            dataChunkBuffer = self.inflateChunk(self.dataChunkFilterBuffer)

                            # shuffle data chunk buffer into data buffer
                            if self.filter[self.SHUFFLE_FILTER]:
                                buffer[buffer_index:buffer_index+chunk_bytes] = self.shuffleChunk(dataChunkBuffer, chunk_index, chunk_bytes, self.typeSize)

                            # copy data chunk buffer into data buffer
                            else:
                                buffer[buffer_index:buffer_index+chunk_bytes] = dataChunkBuffer[chunk_index:chunk_index+chunk_bytes]

                    # check filter options
                    elif errorCheckingOption and self.filter[self.SHUFFLE_FILTER]:
                        raise FatalError(f'shuffle filter unsupported on uncompressed chunk')

                    # check buffer sizes
                    elif errorCheckingOption and (self.dataChunkBufferSize != curr_node['chunk_size']):
                        raise FatalError(f'mismatch in chunk size: {curr_node["chunk_size"]}, {self.dataChunkBufferSize}')

                    # read data into data buffer
                    else:
                        chunk_offset_addr = child_addr + chunk_index
                        buffer[buffer_index:buffer_index+chunk_bytes] = self.resourceObject.ioRequest(chunk_offset_addr, chunk_bytes)

            # goto next key
            curr_node = next_node

    #######################
    # readBTreeNodeV1
    #######################
    def readBTreeNodeV1(self, ndims):
        node = {}

        # read key
        node['chunk_size'] = self.readField(4)
        node['filter_mask'] = self.readField(4)
        node['slice'] = []
        for _ in range(ndims):
            node['slice'].append(self.readField(8))

        # read trailing zero
        trailing_zero = self.readField(8)
        if errorCheckingOption and (trailing_zero % self.typeSize != 0):
            raise FatalError(f'key did not include a trailing zero: {trailing_zero}')

        # set node key
        if ndims > 0:
            node['row_key'] = node['slice'][0]
        else:
            node['row_key'] = 0

        # return copy of node
        return node

    #######################
    # inflateChunk
    #######################
    def inflateChunk(self, input):
        return zlib.decompress(input)

    #######################
    # shuffleChunk
    #######################
    def shuffleChunk(self, input, output_offset, output_size, type_size):
        if errorCheckingOption and (type_size < 0 or type_size > 8):
            raise FatalError(f'invalid data size to perform shuffle on: {type_size}')
        output = bytearray(output_size)
        dst_index = 0
        shuffle_block_size = int(len(input) / type_size)
        num_elements = int(output_size / type_size)
        start_element = int(output_offset / type_size)
        for element_index in range(start_element, start_element + num_elements):
            for val_index in range(0, type_size):
                src_index = (val_index * shuffle_block_size) + element_index
                output[dst_index] = input[src_index]
                dst_index += 1
        return output

    #######################
    # highestBit
    #######################
    def highestBit(self, value):
        bit = 0
        value >>= 1
        while value != 0:
            value >>= 1
            bit += 1
        return bit

###############################################################################
# H5Coro Functions
###############################################################################

def workerThread(dataset_worker):
    try:
        return dataset_worker.readDataset()
    except FatalError as e:
        logger.error(f'H5Coro encountered an error processing {dataset_worker.resourceObject.resource}/{dataset_worker.dataset}: {e}')
        return dataset_worker.dataset,{}

def resultThread(resourceObject):
    for future in concurrent.futures.as_completed(resourceObject.futures):
        dataset, values = future.result()
        resourceObject.conditions[dataset].acquire()
        resourceObject.results[dataset] = values
        resourceObject.conditions[dataset].notify()
        resourceObject.conditions[dataset].release()

###############################################################################
# H5Coro Class
###############################################################################

class H5Coro:

    #######################
    # Constants
    #######################
    CACHE_LINE_SIZE         = cacheLineSizeOption
    CACHE_LINE_MASK         = (0xFFFFFFFFFFFFFFFF - (CACHE_LINE_SIZE-1))
    H5_SIGNATURE_LE         = 0x0A1A0A0D46444889

    #######################
    # Constructor
    #######################
    def __init__(self, resource, driver_class, credentials={}, datasets=[], block=True):
        self.resource = resource
        self.driver = driver_class(resource, credentials)

        self.cache = {}
        self.metaDataTable = {}
        self.metaDataHits = 0

        self.offsetSize = 0
        self.lengthSize = 0
        self.baseAddress = 0
        self.rootAddress = self.readSuperblock()

        self.futures = []
        self.results = {}
        self.conditions = {}

        self.readDatasets(datasets, block)

    #######################
    # readDatasets
    #######################
    def readDatasets(self, datasets, block=True):

        # check if datasets supplied
        if len(datasets) <= 0:
            return

        # make into dictionary
        dataset_table = {}
        for dataset in datasets:
            if type(dataset) == str:
                dataset_table[dataset] = {"dataset": dataset, "startrow": 0, "numrows": H5Dataset.ALL_ROWS}
            else:
                dataset_table[dataset["dataset"]] = dataset

        # start threads working on each dataset
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(dataset_table)) as executor:
            dataset_workers = [H5Dataset(self, dataset["dataset"], dataset["startrow"], dataset["numrows"]) for dataset in dataset_table.values()]
            self.futures = [executor.submit(workerThread, dataset_worker) for dataset_worker in dataset_workers]

        # initialize results and conditionals for each dataset being read
        for dataset in dataset_table.keys():
            self.results[dataset] = None
            self.conditions[dataset] = threading.Condition()

        # wait for results to be populated OR populate results in the background
        if block:
            resultThread(self)
        else:
            threading.Thread(target=resultThread, args=(self,), daemon=True).start()

    #######################
    # listDirectory
    #######################
    def listDirectory(self, directory):
        try:
            dataset_worker = H5Dataset(self, directory, metaOnly=True)
            dataset_worker.readDataset()
        except FatalError as e:
            logger.debug(f'H5Coro exited listing the directory {directory}: {e}')
        if len(directory) > 0 and directory[0] == '/':
            directory = directory[1:]
        if len(directory) > 0 and directory[-1] == '/':
            directory = directory[:-1]
        elements = {}
        for entry in self.metaDataTable:
            if entry.startswith(directory):
                if len(directory) > 0:
                    element = entry.split(directory)[1]
                else:
                    element = entry
                if len(element) > 0 and element[0] == '/':
                    element = element[1:]
                if len(element) > 0:
                    element = element.split('/')[0]
                    elements[element] = True
        return list(elements.keys())

    #######################
    # operator: []
    #######################
    def __getitem__(self, key):
        self.waitOnResult(key)
        return self.results[key]

    #######################
    # representation
    #######################
    def __repr__(self):
        rstr = '{ '
        total_count = len(self.keys())
        count = 1
        for dataset in self.keys():
            separator = count == total_count and ' ' or ','
            rstr += f'"{dataset}": {self.results[dataset]}{separator}'
            count += 1
        rstr += '}'
        return rstr

    #######################
    # string
    #######################
    def __str__(self):
        return self.__repr__()

    #######################
    # iterate
    #######################
    def __iter__(self):
        for key in self.results.keys():
            yield key

    #######################
    # keys
    #######################
    def keys(self):
        for key in self.results.keys():
            self.waitOnResult(key)
        return self.results.keys()

    #######################
    # waitOnResult
    #######################
    def waitOnResult(self, dataset):
        if dataset in self.conditions:
            self.conditions[dataset].acquire()
            while self.results[dataset] == None:
                self.conditions[dataset].wait(1)
            self.conditions[dataset].release()

    #######################
    # ioRequest
    #######################
    def ioRequest(self, pos, size, caching=True, prefetch=False):
        # Check if Caching
        if caching:
            data_blocks = []
            data_to_read = size
            while data_to_read > 0:
                # Calculate Cache Line
                cache_line = (pos + self.baseAddress) & self.CACHE_LINE_MASK
                # Populate Cache (if not there already)
                if cache_line not in self.cache:
                    self.cache[cache_line] = memoryview(self.driver.read(cache_line, self.CACHE_LINE_SIZE))
                # Update Indexes
                start_index = (pos + self.baseAddress) - cache_line
                stop_index = min(start_index + size, self.CACHE_LINE_SIZE)
                data_read = stop_index - start_index
                data_to_read -= data_read
                pos += data_read
                # Grab slice of memory from cache
                data_blocks += self.cache[cache_line][start_index:stop_index],

            if len(data_blocks) == 1:
                return data_blocks[0]
            else:
                return b''.join(data_blocks)
        # Prefetch
        elif prefetch:
            block_size = size + ((self.CACHE_LINE_SIZE - (size % self.CACHE_LINE_SIZE)) % self.CACHE_LINE_SIZE) # align to cache line boundary
            cache_line = (pos + self.baseAddress) & self.CACHE_LINE_MASK
            data_block = memoryview(self.driver.read(cache_line, block_size))
            data_index = 0
            while data_index < block_size:
                # Cache the Line
                self.cache[cache_line] = data_block[data_index:data_index+self.CACHE_LINE_SIZE]
                cache_line += self.CACHE_LINE_SIZE
                data_index += self.CACHE_LINE_SIZE
            return None
        # Direct Read
        else:
            return self.driver.read(pos + self.baseAddress, size)

    #######################
    # readSuperblock
    #######################
    def readSuperblock(self):
        # read start of superblock
        block = self.ioRequest(0, 9)
        signature, superblock_version = struct.unpack(f'<QB', block)

        # check file signature
        if signature != self.H5_SIGNATURE_LE:
            raise FatalError(f'invalid file signature: 0x{signature:x}')

        # check file version
        if superblock_version != 0 and superblock_version != 2:
            raise FatalError(f'unsupported superblock version: {superblock_version}')

        # Super Block Version 0 #
        if superblock_version == 0:
            if errorCheckingOption:
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
        if verboseOption:
            logger.info(f'File Information @0x{root_group_offset:x}')
            logger.info(f'Size of Offsets:      {self.offsetSize}')
            logger.info(f'Size of Lengths:      {self.lengthSize}')
            logger.info(f'Base Address:         {self.baseAddress}')
            logger.info(f'Root Group Offset:    0x{root_group_offset:x}')

        # return root group offset
        return root_group_offset
