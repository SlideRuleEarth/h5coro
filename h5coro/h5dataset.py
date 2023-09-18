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

from h5coro.h5metadata import H5Metadata
from h5coro.h5values import H5Values
from datetime import datetime
import struct
import logging
import zlib
import ctypes
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
    CUSTOM_V1_FLAG          = 0x80
    ALL_ROWS                = -1
    MAX_NDIMS               = 2
    FLAT_NDIMS              = 3
    # signatures
    H5_SIGNATURE_LE         = 0x0A1A0A0D46444889
    H5_OHDR_SIGNATURE_LE    = 0x5244484F
    H5_FRHP_SIGNATURE_LE    = 0x50485246
    H5_FHDB_SIGNATURE_LE    = 0x42444846
    H5_FHIB_SIGNATURE_LE    = 0x42494846
    H5_OCHK_SIGNATURE_LE    = 0x4B48434F
    H5_TREE_SIGNATURE_LE    = 0x45455254
    H5_HEAP_SIGNATURE_LE    = 0x50414548
    H5_SNOD_SIGNATURE_LE    = 0x444F4E53
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
    def __init__(self, resourceObject, dataset, startRow=0, numRows=ALL_ROWS, makeNull=False, *, earlyExit, metaOnly, enableAttributes):
        # initialize object
        self.resourceObject         = resourceObject
        self.earlyExit              = earlyExit
        self.metaOnly               = metaOnly
        self.enableAttributes       = enableAttributes
        self.pos                    = self.resourceObject.rootAddress
        self.dataset                = dataset
        self.datasetStartRow        = startRow
        self.datasetNumRows         = numRows
        self.datasetPath            = list(filter(('').__ne__, self.dataset.split('/')))
        self.datasetPathLevels      = len(self.datasetPath)
        self.datasetFound           = False
        self.dataChunkBufferSize    = 0
        self.meta                   = H5Metadata()
        self.values                 = None

        # check for null dataset
        # (to handle datasets that error out and cannot be read)
        if makeNull:
            return

        # initialize local variables
        dataset_level = 0

        # get metadata for dataset
        if self.dataset in self.resourceObject.metadataTable:
            self.meta = self.resourceObject.metadataTable[self.dataset]

        # metadata not available
        if self.meta.typeSize == 0 or not earlyExit:
            # traverse file for dataset
            self.readObjHdr(dataset_level)
            # update metadata table
            if self.meta.typeSize != 0:
                self.resourceObject.metadataTable[self.dataset] = self.meta

        # exit early if only reading metadata
        if self.metaOnly:
            return

        # sanity check data attrbutes
        if self.meta.typeSize <= 0:
            raise FatalError(f'missing data type information for {self.dataset}')
        elif self.meta.ndims == None:
            raise FatalError(f'missing data dimension information for {self.dataset}')
        elif self.meta.address == INVALID_VALUE[self.resourceObject.offsetSize]:
            raise FatalError(f'invalid data address for {self.dataset}')

        # calculate size of data row (note dimension starts at 1)
        row_size = self.meta.typeSize
        for d in range(1, self.meta.ndims):
            row_size *= self.meta.dimensions[d]

        # get number of rows
        first_dimension = (self.meta.ndims > 0) and self.meta.dimensions[0] or 1
        self.datasetNumRows = (self.datasetNumRows == self.ALL_ROWS) and first_dimension or self.datasetNumRows
        if (self.datasetStartRow + self.datasetNumRows) > first_dimension:
            raise FatalError(f'read exceeds number of rows: {self.datasetStartRow} + {self.datasetNumRows} > {first_dimension}')

        # calculate size of buffer
        buffer_size = row_size * self.datasetNumRows

        # calculate buffer start */
        buffer_offset = row_size * self.datasetStartRow

        # check if data address and data size is valid
        if self.resourceObject.errorChecking:
            if (self.meta.size != 0) and (self.meta.size < (buffer_offset + buffer_size)):
                raise FatalError(f'read exceeds available data: {self.meta.size} < {buffer_offset} + {buffer_size}')
            if (self.meta.filter[self.meta.DEFLATE_FILTER] or self.meta.filter[self.meta.SHUFFLE_FILTER]) and \
               ((self.meta.layout == self.COMPACT_LAYOUT) or (self.meta.layout == self.CONTIGUOUS_LAYOUT)):
                raise FatalError(f'filters unsupported on non-chunked layouts')

        # read dataset
        if buffer_size > 0:
            if (self.meta.layout == self.COMPACT_LAYOUT) or (self.meta.layout == self.CONTIGUOUS_LAYOUT):
                data_addr = self.meta.address + buffer_offset
                buffer = self.resourceObject.ioRequest(data_addr, buffer_size, caching=False)
            elif self.meta.layout == self.CHUNKED_LAYOUT:
                # chunk layout specific error checks
                if self.resourceObject.errorChecking:
                    if self.meta.elementSize != self.meta.typeSize:
                        raise FatalError(f'chunk element size does not match data element size: {self.meta.elementSize} !=  {self.meta.typeSize}')
                    elif self.meta.chunkElements <= 0:
                        raise FatalError(f'invalid number of chunk elements: {self.meta.chunkElements}')

                # allocate and initialize buffer
                buffer = bytearray(buffer_size)

                # fill buffer with fill value (if provided)
                if self.meta.fillsize > 0:
                    fill_values = struct.pack('Q', self.meta.fillvalue)[:self.meta.fillsize]
                    for i in range(0, buffer_size, self.meta.fillsize):
                        buffer[i:i+self.meta.fillsize] = fill_values

                # calculate data chunk buffer size
                self.dataChunkBufferSize = self.meta.chunkElements * self.meta.typeSize

                # perform prefetch
                if self.resourceObject.enablePrefetch:
                    if buffer_offset < buffer_size:
                        self.resourceObject.ioRequest(self.meta.address, buffer_offset + buffer_size, caching=False, prefetch=True)
                    else:
                        self.resourceObject.ioRequest(self.meta.address + buffer_offset, buffer_size, caching=False, prefetch=True)

                # read b-tree
                self.pos = self.meta.address
                self.readBTreeV1(buffer, buffer_offset, dataset_level)

                # check need to flatten chunks
                flatten = False
                for d in range(1, self.meta.ndims):
                    if self.meta.chunkDimensions[d] != self.meta.dimensions[d]:
                        flatten = True
                        break

                # flatten chunks - place dataset in row order
                if flatten:
                    # new flattened buffer
                    fbuf = numpy.empty(buffer_size, dtype=numpy.byte)
                    bi = 0 # index into source buffer

                    # build number of each chunk per dimension
                    cdimnum = [0 for _ in range(self.MAX_NDIMS * 2)]
                    for i in range(self.meta.ndims):
                        cdimnum[i] = self.meta.dimensions[i] / self.meta.chunkDimensions[i]
                        cdimnum[i + self.meta.ndims] = self.meta.chunkDimensions[i]

                    # build size of each chunk per flattened dimension
                    cdimsizes = [0 for _ in range(self.FLAT_NDIMS)]
                    cdimsizes[0] = self.meta.chunkDimensions[0] * self.meta.typeSize  # number of chunk rows
                    for i in range(1, self.meta.ndims):
                        cdimsizes[0] *= cdimnum[i]                          # number of columns of chunks
                        cdimsizes[0] *= self.meta.chunkDimensions[i]             # number of columns in chunks
                    cdimsizes[1] = self.meta.typeSize
                    for i in range(1, self.meta.ndims):
                        cdimsizes[1] *= self.meta.chunkDimensions[i]             # number of columns in chunks
                    cdimsizes[2] = self.meta.typeSize
                    for i in range(1, self.meta.ndims):
                        cdimsizes[2] *= cdimnum[i]                          # number of columns of chunks
                        cdimsizes[2] *= self.meta.chunkDimensions[i]             # number of columns in chunks

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

            elif self.resourceObject.errorChecking:
                raise FatalError(f'invalid data layout: {self.meta.layout}')

        try:
            # set dimensions
            numcols = 0
            if self.meta.ndims == 0:
                numcols = 0
            elif self.meta.ndims == 1:
                numcols = 1
            elif self.meta.ndims >= 2:
                numcols = self.meta.dimensions[1]
            elements = int(buffer_size / self.meta.typeSize)

            # populate values
            if self.meta.type == H5Metadata.FIXED_POINT_TYPE or self.meta.type == H5Metadata.FLOATING_POINT_TYPE:
                datatype = H5Metadata.TO_NUMPY_TYPE[self.meta.type][self.meta.signedval][self.meta.typeSize]
                values = numpy.frombuffer(buffer, dtype=datatype, count=elements)
            else:
                datatype = str
                values = ctypes.create_string_buffer(buffer).value.decode('ascii')
            # fulfill h5 future
            self.values = H5Values(elements,
                                   buffer_size,
                                   self.datasetNumRows,
                                   numcols,
                                   datatype,
                                   values)
        except Exception as e:
            raise FatalError(f'unable to populate datasets for {self.resourceObject.resource}/{self.dataset}: {e}')

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
    # readSuperblock
    #
    #   Note: this is NOT a class method as it takes a 'resourceObject'
    #   and populates critical attributes of that object; this should
    #   be called in h5coro and passed the self member
    #######################
    def readSuperblock(resourceObject):
        # read start of superblock
        block = resourceObject.ioRequest(0, 9)
        signature, superblock_version = struct.unpack(f'<QB', block)

        # check file signature
        if signature != H5Dataset.H5_SIGNATURE_LE:
            raise FatalError(f'invalid file signature: 0x{signature:x}')

        # check file version
        if superblock_version != 0 and superblock_version != 2:
            raise FatalError(f'unsupported superblock version: {superblock_version}')

        # Super Block Version 0 #
        if superblock_version == 0:
            if resourceObject.errorChecking:
                # read start of superblock
                block = resourceObject.ioRequest(9, 2)
                freespace_version, roottable_version = struct.unpack(f'<BB', block)

                # check free space version
                if freespace_version != 0:
                    raise FatalError(f'unsupported free space version: {freespace_version}')

                # check root table version
                if roottable_version != 0:
                    raise FatalError(f'unsupported root table version: {roottable_version}')

            # read sizes
            block = resourceObject.ioRequest(13, 2)
            resourceObject.offsetSize, resourceObject.lengthSize = struct.unpack(f'<BB', block)

            # set base address
            block = resourceObject.ioRequest(24, resourceObject.offsetSize)
            resourceObject.baseAddress = struct.unpack(f'<{SIZE_2_FORMAT[resourceObject.offsetSize]}', block)[0]

            # read group offset
            block = resourceObject.ioRequest(24 + (5 * resourceObject.offsetSize), resourceObject.offsetSize)
            root_group_offset = struct.unpack(f'<{SIZE_2_FORMAT[resourceObject.offsetSize]}', block)[0]

        # Super Block Version 1 #
        else:
            # read sizes
            block = resourceObject.ioRequest(9, 2)
            resourceObject.offsetSize, resourceObject.lengthSize = struct.unpack(f'<BB', block)

            # set base address
            block = resourceObject.ioRequest(12, resourceObject.offsetSize)
            resourceObject.baseAddress = struct.unpack(f'<{SIZE_2_FORMAT[resourceObject.offsetSize]}', block)[0]

            # read group offset
            block = resourceObject.ioRequest(12 + (3 * resourceObject.offsetSize), resourceObject.offsetSize)
            root_group_offset = struct.unpack(f'<{SIZE_2_FORMAT[resourceObject.offsetSize]}', block)[0]

        # print file information
        if resourceObject.verbose:
            logger.info(f'File Information @0x{root_group_offset:x}')
            logger.info(f'Size of Offsets:      {resourceObject.offsetSize}')
            logger.info(f'Size of Lengths:      {resourceObject.lengthSize}')
            logger.info(f'Base Address:         {resourceObject.baseAddress}')
            logger.info(f'Root Group Offset:    0x{root_group_offset:x}')

        # return root group offset
        return root_group_offset

    #######################
    # readObjHdr
    #######################
    def readObjHdr(self, dlvl):
        # check mata data table
        for lvl in range(self.datasetPathLevels, dlvl, -1):
            group_path = '/'.join(self.datasetPath[:lvl])
            if group_path in self.resourceObject.metadataTable:
                self.pos = self.resourceObject.metadataTable[group_path].address
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
        if self.resourceObject.verbose:
            logger.info(f'<<Object Information V0 - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')

        # check signature and version
        if self.resourceObject.errorChecking:
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
            if self.resourceObject.verbose:
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
            if self.resourceObject.verbose:
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
            if self.resourceObject.errorChecking and (bytes_read != msg_size):
                raise FatalError(f'header v0 message different size than specified: {bytes_read} != {msg_size}')

            # check if dataset found
            if self.earlyExit and self.datasetFound:
                self.pos = end_of_hdr # go directly to end of header
                break # exit loop because dataset is found

        # check bytes read
        if self.resourceObject.errorChecking and (self.pos < end_of_hdr):
            raise FatalError(f'did not read enough v0 bytes: 0x{self.pos:x} < 0x{end_of_hdr:x}')

        # return bytes read
        return self.pos - starting_position

    #######################
    # readObjHdrV1
    #######################
    def readObjHdrV1(self, dlvl):
        starting_position = self.pos
        self.pos += 2 # version and reserved field

        if self.resourceObject.verbose:
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
            if self.resourceObject.errorChecking:
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
            if self.resourceObject.errorChecking and (bytes_read != msg_size):
                raise FatalError(f'header v1 message different size than specified: {bytes_read} != {msg_size}')

            # check if dataset found
            if self.earlyExit and self.datasetFound:
                self.pos = end_of_hdr # go directly to end of header
                break # exit loop because dataset is found

        # move past gap
        if self.pos < end_of_hdr:
            self.pos = end_of_hdr

        # check bytes read
        if self.resourceObject.errorChecking and (self.pos < end_of_hdr):
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
        # attribute handlers
        if self.enableAttributes:
            msg_handler_table[self.ATTRIBUTE_MSG] = self.attributeMsgHandler
            msg_handler_table[self.ATTRIBUTE_INFO_MSG] = self.attributeinfoMsgHandler
        # process message
        try:
            return msg_handler_table[msg_type](msg_size, obj_hdr_flags, dlvl)
        except KeyError:
            if self.resourceObject.verbose:
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

        if self.resourceObject.verbose:
            logger.info(f'<<Dataspace Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            logger.info(f'Version:              {version}')
            logger.info(f'Dimensionality:       {dimensionality}')
            logger.info(f'Flags:                {flags}')

        # check version and flags and dimenstionality
        if self.resourceObject.errorChecking:
            if version != 1 and version != 2:
                raise FatalError(f'unsupported dataspace version: {version}')
            if flags & PERM_INDEX_PRESENT:
                raise FatalError(f'unsupported permutation indexes')
            if dimensionality > self.MAX_NDIMS:
                raise FatalError(f'unsupported number of dimensions: {dimensionality}')

        # read and populate data dimensions
        self.meta.dimensions = []
        self.meta.ndims = min(dimensionality, self.MAX_NDIMS)
        if self.meta.ndims > 0:
            for x in range(self.meta.ndims):
                dimension = self.readField(self.resourceObject.lengthSize)
                self.meta.dimensions.append(dimension)
                if self.resourceObject.verbose:
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
        if self.resourceObject.verbose:
            logger.info(f'<<Link Information Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            logger.info(f'Version:              {version}')
            logger.info(f'Flags:                {flags}')

        # check version
        if self.resourceObject.errorChecking and (version != 0):
            raise FatalError(f'unsupported link info version: {version}')

        # read maximum creation index
        if flags & MAX_CREATE_PRESENT_BIT:
            max_create_index = self.readField(8)
            if self.resourceObject.verbose:
                logger.info(f'Max Create Index:     {max_create_index}')

        # read heap address and name index
        heap_address = self.readField(self.resourceObject.offsetSize)
        name_index = self.readField(self.resourceObject.offsetSize)
        if self.resourceObject.verbose:
            logger.info(f'Heap Address:         0x{heap_address:x}')
            logger.info(f'Name Index:           0x{name_index:x}')

        # read address of v2 B-tree for creation order index
        if flags & CREATE_ORDER_PRESENT_BIT:
            create_order_index = self.readField(self.resourceObject.offsetSize)
            if self.resourceObject.verbose:
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
        self.meta.typeSize          = self.readField(4)
        version                     = (version_class & 0xF0) >> 4
        databits                    = version_class >> 8
        self.meta.type              = version_class & 0x0F
        self.meta.signedval         = ((databits & 0x08) >> 3) == 1

        # display
        if self.resourceObject.verbose:
            logger.info(f'<<Data Type Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            logger.info(f'Version:              {version}')
            logger.info(f'Type Size:            {self.meta.typeSize}')
            logger.info(f'Data Type:            {self.meta.type}')
            logger.info(f'Signed:               {self.meta.signedval}')

        # check version
        if self.resourceObject.errorChecking and version != 1:
            raise FatalError(f'unsupported datatype version: {version}')

        # Fixed Point
        if self.meta.type == H5Metadata.FIXED_POINT_TYPE:
            if self.resourceObject.verbose:
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
        elif self.meta.type == H5Metadata.FLOATING_POINT_TYPE:
            self.meta.signedval = True
            if self.resourceObject.verbose:
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
        elif self.meta.type == H5Metadata.VARIABLE_LENGTH_TYPE:
            if self.resourceObject.verbose:
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
        elif self.meta.type == H5Metadata.STRING_TYPE:
            self.meta.signedval = True
            if self.resourceObject.verbose:
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
        elif self.resourceObject.errorChecking:
            raise FatalError(f'unsupported datatype: {self.meta.type}')

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
        if self.resourceObject.verbose:
            logger.info(f'<<Fill Value Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            logger.info(f'Version:              {version}')

        # check version
        if self.resourceObject.errorChecking and (version != 2) and (version != 3):
            raise FatalError(f'invalid fill value version: {version}')

        # version 2
        if version == 2:
            if self.resourceObject.verbose:
                space_allocation_time = self.readField(1)
                fill_value_write_time = self.readField(1)
                logger.info(f'Space Allocation Time:{space_allocation_time}')
                logger.info(f'Fill Value Write Time:{fill_value_write_time}')
            else:
                self.pos += 2

            fill_value_defined = self.readField(1)
            if fill_value_defined:
                self.meta.fillsize = self.readField(4)
                if self.meta.fillsize > 0:
                    self.meta.fillvalue = self.readField(self.meta.fillsize)
        # version 3
        else:
            flags = self.readField(1)
            if self.resourceObject.verbose:
                logger.info(f'Fill Flags:           {flags}')

            if flags & FILL_VALUE_DEFINED:
                self.meta.fillsize = self.readField(4)
                self.meta.fillvalue = self.readField(self.meta.fillsize)

        # display
        if self.resourceObject.verbose:
            logger.info(f'Fill Value Size:      {self.meta.fillsize}')
            logger.info(f'Fill Value:           {self.meta.fillvalue}')

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
        if self.resourceObject.verbose:
            logger.info(f'<<Link Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            logger.info(f'Version:              {version}')
            logger.info(f'Flags:                {flags}')

        # check version
        if self.resourceObject.errorChecking and version != 1:
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
        if self.resourceObject.errorChecking and (link_name_len_of_len > 8):
            raise FatalError(f'invalid link name length of length: {link_name_len_of_len}')
        link_name_len = self.readField(link_name_len_of_len)
        link_name = self.readArray(link_name_len).tobytes().decode('utf-8')

        # display
        if self.resourceObject.verbose:
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
            if self.resourceObject.verbose:
                logger.info(f'Hard Link:            0x{obj_hdr_addr:x}')
            # update meta data table
            group_path = '/'.join(self.datasetPath[:dlvl] + [link_name])
            self.resourceObject.metadataTable[group_path] = H5Metadata(obj_hdr_addr)
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
            if self.resourceObject.verbose:
                logger.info(f'Soft Link:            {soft_link}')
            if self.resourceObject.errorChecking and follow_link:
                raise FatalError(f'unsupported soft link encountered: {soft_link}')

        elif link_type == EXTERNAL_LINK:
            ext_link_len = self.readField(2)
            ext_link = self.readArray(ext_link_len).tobytes().decode('utf-8')
            if self.resourceObject.verbose:
                logger.info(f'External Link:        {ext_link}')
            if self.resourceObject.errorChecking and follow_link:
                raise FatalError(f'unsupported external link encountered: {ext_link}')

        elif self.resourceObject.errorChecking:
            raise FatalError(f'unsupported link type: {link_type}')

        # return bytes read
        return self.pos - starting_position

    #######################
    # datalayoutMsgHandler
    #######################
    def datalayoutMsgHandler(self, msg_size, obj_hdr_flags, dlvl):
        starting_position   = self.pos
        version             = self.readField(1)
        self.meta.layout         = self.readField(1)

        # display
        if self.resourceObject.verbose:
            logger.info(f'<<Data Layout Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            logger.info(f'Version:              {version}')
            logger.info(f'Layout:               {self.meta.layout}')

        # check version
        if self.resourceObject.errorChecking and version != 3:
            raise FatalError(f'invalid data layout version: {version}')

        # read layouts
        if self.meta.layout == self.COMPACT_LAYOUT:
            self.meta.size = self.readField(2)
            self.meta.address = self.pos
            self.pos += self.meta.size
        elif self.meta.layout == self.CONTIGUOUS_LAYOUT:
            self.meta.address = self.readField(self.resourceObject.offsetSize)
            self.meta.size = self.readField(self.resourceObject.lengthSize)
        elif self.meta.layout == self.CHUNKED_LAYOUT:
            # read number of dimensions
            chunk_num_dim = self.readField(1) - 1  # dimensionality is plus one over actual number of dimensions
            chunk_num_dim = min(chunk_num_dim, self.MAX_NDIMS)
            if self.resourceObject.errorChecking and (self.meta.ndims != None) and (chunk_num_dim != self.meta.ndims):
                raise FatalError(f'number of chunk dimensions does not match dimensionality of data: {chunk_num_dim} != {self.meta.ndims}')
            # read address of B-tree
            self.meta.address = self.readField(self.resourceObject.offsetSize)
            # read chunk dimensions
            if chunk_num_dim > 0:
                self.meta.chunkElements = 1
                for _ in range(chunk_num_dim):
                    chunk_dimension = self.readField(4)
                    self.meta.chunkDimensions.append(chunk_dimension)
                    self.meta.chunkElements *= chunk_dimension
            # read element size
            self.meta.elementSize = self.readField(4)
            # display
            if self.resourceObject.verbose:
                logger.info(f'Element Size:         {self.meta.elementSize}')
                logger.info(f'# Chunked Dimensions: {chunk_num_dim}')
                for d in range(chunk_num_dim):
                    logger.info(f'Chunk Dimension {d}:    {self.meta.chunkDimensions[d]}')
        elif self.resourceObject.errorChecking:
            raise FatalError(f'unsupported data layout: {self.meta.layout}')

        # display
        if self.resourceObject.verbose:
            logger.info(f'Dataset Size:         {self.meta.size}')
            logger.info(f'Dataset Address:      {self.meta.address}')

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
        if self.resourceObject.verbose:
            logger.info(f'<<Filter Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            logger.info(f'Version:              {version}')
            logger.info(f'Num Filters:          {num_filters}')

        # check version
        if self.resourceObject.errorChecking and (version != 1) and (version != 2):
            raise FatalError(f'invalid filter version: {version}')

        # move past reserved bytes in version 1
        if version == 1:
            self.pos += 6

        # read filters
        for f in range(num_filters):
            # read filter id
            filter_id = self.readField(2)

            # read filter name length
            name_len = 0
            if (version == 1) or (filter >= 256):
                name_len = self.readField(2)

            # read Filter parameters
            flags     = self.readField(2)
            num_parms = self.readField(2)

            # consistency check flags
            if self.resourceObject.errorChecking and (flags != 0) and (flags != 1):
                raise FatalError(f'invalid flags in filter message: {flags}')

            # read name
            filter_name = ""
            if name_len > 0:
                filter_name = self.readArray(name_len).tobytes().decode('utf-8')
                name_padding = (8 - (name_len % 8)) % 8
                self.pos += name_padding

            # display
            if self.resourceObject.verbose:
                logger.info(f'Filter ID:            {filter_id}')
                logger.info(f'Flags:                {flags}')
                logger.info(f'# Parameters:         {num_parms}')
                logger.info(f'Filter Name:          {filter_name}')

            # set filter
            try:
                self.meta.filter[filter_id] = True
            except Exception:
                raise FatalError(f'unsupported filter specified: {filter_id}')

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
        PAD_SIZE            = 8
        starting_position   = self.pos
        version             = self.readField(1)
        self.pos           += 1
        name_size           = self.readField(2)
        datatype_size       = self.readField(2)
        dataspace_size      = self.readField(2)

        # display
        if self.resourceObject.verbose:
            logger.info(f'<<Attribute Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            logger.info(f'Version:              {version}')

        # check version
        if self.resourceObject.errorChecking and (version != 1):
            raise FatalError(f'invalid attribute version: {version}')

        # update message sizes
        datatype_size += ((PAD_SIZE - (datatype_size % PAD_SIZE)) % PAD_SIZE)
        dataspace_size += ((PAD_SIZE - (dataspace_size % PAD_SIZE)) % PAD_SIZE)

        # read attribute name
        attr_name = self.readArray(name_size).tobytes().decode('utf-8')[:-1]
        self.pos += (PAD_SIZE - (name_size % PAD_SIZE)) % PAD_SIZE; # align to next x-byte boundary
        attr_path = '/'.join(self.datasetPath[:dlvl] + [attr_name])

        # display
        if self.resourceObject.verbose:
            logger.info(f'Name:                 {attr_name}')
            logger.info(f'Message Size:         {msg_size}')
            logger.info(f'Datatype Size:        {datatype_size}')
            logger.info(f'Dataspace Size:       {dataspace_size}')

        # check if desired attribute
        if( ((dlvl + 1) == len(self.datasetPath)) and
            (attr_name == self.datasetPath[dlvl]) ):
            self.datasetFound = True
            self.meta.isattribute = True

            # read datatype message
            datatype_bytes_read = self.datatypeMsgHandler(datatype_size, obj_hdr_flags, dlvl)
            pad_bytes = (PAD_SIZE - (datatype_bytes_read % PAD_SIZE)) % PAD_SIZE # align to next x-byte boundary
            if self.resourceObject.errorChecking and ((datatype_bytes_read + pad_bytes) != datatype_size):
                raise FatalError(f'failed to read expected bytes for datatype message: {datatype_bytes_read + pad_bytes} != {datatype_size}')
            self.pos += pad_bytes

            # read dataspace message
            dataspace_bytes_read = self.dataspaceMsgHandler(dataspace_size, obj_hdr_flags, dlvl)
            pad_bytes = (PAD_SIZE - (dataspace_bytes_read % PAD_SIZE)) % PAD_SIZE # align to next x-byte boundary
            if self.resourceObject.errorChecking and ((dataspace_bytes_read + pad_bytes) != dataspace_size):
                raise FatalError(f'failed to read expected bytes for dataspace message: {dataspace_bytes_read + pad_bytes} != {dataspace_size}')
            self.pos += pad_bytes

            # set meta data
            self.meta.layout = self.CONTIGUOUS_LAYOUT
            for f in self.meta.filter.keys():
                self.meta.filter[f] = False
            self.meta.address = self.pos
            self.meta.size = msg_size - (self.pos - starting_position)

            # move to end of data
            self.pos += self.meta.size

            # update metadata table
            self.resourceObject.metadataTable[attr_path] = self.meta

            # return bytes read
            return self.pos - starting_position
        else:
            # update metadata table
            self.resourceObject.metadataTable[attr_path] = H5Metadata(self.pos + datatype_size + dataspace_size)
            self.resourceObject.metadataTable[attr_path].isattribute = True

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
        if self.resourceObject.verbose:
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
            if self.resourceObject.errorChecking:
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
        if self.resourceObject.verbose:
            logger.info(f'<<Symbol Table Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            logger.info(f'B-Tree Address:       {btree_addr}')
            logger.info(f'Heap Address:         {heap_addr}')

        # read heap info
        self.pos = heap_addr
        if self.resourceObject.errorChecking:
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
            if self.resourceObject.errorChecking:
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
            if self.resourceObject.verbose:
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
                if self.earlyExit and self.datasetFound:
                    break

            # exit loop or go to next node
            if (right_sibling == INVALID_VALUE[self.resourceObject.offsetSize]) or (self.earlyExit and self.datasetFound):
                break
            else:
                self.pos = right_sibling

            # read header info
            if self.resourceObject.errorChecking:
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
        if self.resourceObject.verbose:
            logger.info(f'<<Attribute Info - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            logger.info(f'Version:              {version}')
            logger.info(f'Flags:                {flags}')

        # check version
        if self.resourceObject.errorChecking and (version != 0):
            raise FatalError(f'unsupported link info version: {version}')

        # read maximum creation index (number of elements in group)
        if flags & MAX_CREATE_PRESENT_BIT:
            max_create_index = self.readField(2)
            if self.resourceObject.verbose:
                logger.info(f'Max Creation Index:   {max_create_index}')

        # read heap and name offsets
        heap_address    = self.readField(self.resourceObject.offsetSize)
        name_index      = self.readField(self.resourceObject.offsetSize)
        if self.resourceObject.verbose:
            logger.info(f'Heap Address:         {heap_address}')
            logger.info(f'Name Index:           {name_index}')

        # read creation order index
        if flags & CREATE_ORDER_PRESENT_BIT:
            create_order_index = self.readField(self.resourceObject.offsetSize)
            if self.resourceObject.verbose:
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
        if self.resourceObject.verbose:
            logger.info(f'<<Symbol Table - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')

        # check signature and version
        if self.resourceObject.errorChecking:
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
            if self.resourceObject.verbose:
                logger.info(f'Link Name:            {link_name}')
                logger.info(f'Obj Hdr Addr:         {obj_hdr_addr}')

            # update meta data table
            group_path = '/'.join(self.datasetPath[:dlvl] + [link_name])
            self.resourceObject.metadataTable[group_path] = H5Metadata(obj_hdr_addr)

            # process link
            return_position = self.pos
            if dlvl < len(self.datasetPath) and link_name == self.datasetPath[dlvl]:
                if cache_type == 2:
                    raise FatalError(f'symbolic links are unsupported: {link_name}')
                self.readObjHdr(obj_hdr_addr, dlvl + 1)
                self.pos = return_position
                if self.earlyExit:
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
        if self.resourceObject.verbose:
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
        if self.resourceObject.errorChecking:
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
        if self.resourceObject.errorChecking and (bytes_read > heap_info['starting_blk_size']):
            raise FatalError(f'block contianed more bytes than specified: {bytes_read} > {heap_info["starting_blk_size"]}')

        # return bytes read
        return self.pos - starting_position

    #######################
    # readDirectBlock
    #######################
    def readDirectBlock(self, heap_info, block_size, obj_hdr_flags, dlvl):
        starting_position = self.pos

        # display
        if self.resourceObject.verbose:
            logger.info(f'<<Direct Block - {self.dataset}[{dlvl}] @0x{starting_position:x}: {heap_info["msg_type"]}, {block_size}>>')

        # check signature and version
        if self.resourceObject.errorChecking:
            signature = self.readField(4)
            version = self.readField(1)
            if signature != self.H5_FHDB_SIGNATURE_LE:
                raise FatalError(f'invalid direct block signature: 0x{signature:x}')
            if version != 0:
                raise FatalError(f'invalid direct block version: {version}')
        else:
            self.pos += 5

        # read block header
        if self.resourceObject.verbose:
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
                if self.resourceObject.verbose:
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
            if self.resourceObject.errorChecking and data_left < 0:
                raise FatalError(f'reading message exceeded end of direct block: {starting_position}')

            # check if dataset found
            if self.earlyExit and self.datasetFound:
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
        if self.resourceObject.verbose:
            logger.info(f'<<Indirect Block - {self.dataset}[{dlvl}] @0x{starting_position:x}: {heap_info["msg_type"]}, {block_size}>>')

        # check signature and version
        if self.resourceObject.errorChecking:
            signature = self.readField(4)
            version = self.readField(1)
            if signature != self.H5_FHIB_SIGNATURE_LE:
                raise FatalError(f'invalid direct block signature: 0x{signature:x}')
            if version != 0:
                raise FatalError(f'unsupported direct block version: {version}')
        else:
            self.pos += 5

        # read block header
        if self.resourceObject.verbose:
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
        if self.resourceObject.verbose:
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
                    if self.resourceObject.errorChecking and (row >= K):
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
                        if self.resourceObject.errorChecking and (bytes_read > row_block_size):
                            raise FatalError(f'direct block contained more bytes than specified: {bytes_read} > {row_block_size}')
                elif self.resourceObject.errorChecking and ((row < K) or (row >= N)):
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
                        if self.resourceObject.errorChecking and (bytes_read > row_block_size):
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
        if self.resourceObject.verbose:
            logger.info(f'<<B-Tree Node - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')

        # check signature and node type
        if self.resourceObject.errorChecking:
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
        if self.resourceObject.verbose:
            logger.info(f'Node Level:           {node_level}')
            logger.info(f'Entries Used:         {entries_used}')

        # skip sibling addresses
        self.pos += self.resourceObject.offsetSize * 2

        # read first key
        curr_node = self.readBTreeNodeV1(self.meta.ndims)

        # read children
        for e in range(entries_used):
            child_addr  = self.readField(self.resourceObject.offsetSize)
            next_node   = self.readBTreeNodeV1(self.meta.ndims)
            child_key1  = curr_node['row_key']
            child_key2  = next_node['row_key'] # there is always +1 keys
            if (next_node['chunk_size'] == 0) and (self.meta.ndims > 0):
                child_key2 = self.meta.dimensions[0]

            # display
            if self.resourceObject.verbose:
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
                    for i in range(self.meta.ndims):
                        slice_size = curr_node['slice'][i] * self.meta.typeSize
                        for k in range(i):
                            slice_size *= self.meta.chunkDimensions[k]
                        for j in range(i + 1, self.meta.ndims):
                            slice_size *= self.meta.dimensions[j]
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
                    if self.resourceObject.verbose:
                        logger.debug(f'Chunk Offset:         {chunk_offset} ({int(chunk_offset/self.meta.typeSize)})')
                        logger.debug(f'Buffer Index:         {buffer_index} ({int(buffer_index/self.meta.typeSize)})')
                        logger.debug(f'Chunk Bytes:          {chunk_bytes} ({int(chunk_bytes/self.meta.typeSize)})')

                    # read chunk
                    if self.meta.filter[self.meta.DEFLATE_FILTER]:

                        # read data into chunk filter buffer (holds the compressed data)
                        self.dataChunkFilterBuffer = self.resourceObject.ioRequest(child_addr, curr_node['chunk_size'])

                        # inflate directly into data buffer
                        if (chunk_bytes == self.dataChunkBufferSize) and (not self.meta.filter[self.meta.SHUFFLE_FILTER]):
                            buffer[buffer_index:buffer_index+chunk_bytes] = self.inflateChunk(self.dataChunkFilterBuffer)

                        # inflate into data chunk buffer */
                        else:
                            dataChunkBuffer = self.inflateChunk(self.dataChunkFilterBuffer)

                            # shuffle data chunk buffer into data buffer
                            if self.meta.filter[self.meta.SHUFFLE_FILTER]:
                                buffer[buffer_index:buffer_index+chunk_bytes] = self.shuffleChunk(dataChunkBuffer, chunk_index, chunk_bytes, self.meta.typeSize)

                            # copy data chunk buffer into data buffer
                            else:
                                buffer[buffer_index:buffer_index+chunk_bytes] = dataChunkBuffer[chunk_index:chunk_index+chunk_bytes]

                    # check filter options
                    elif self.resourceObject.errorChecking and self.meta.filter[self.meta.SHUFFLE_FILTER]:
                        raise FatalError(f'shuffle filter unsupported on uncompressed chunk')

                    # check buffer sizes
                    elif self.resourceObject.errorChecking and (self.dataChunkBufferSize != curr_node['chunk_size']):
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
        if self.resourceObject.errorChecking and (trailing_zero % self.meta.typeSize != 0):
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
        if self.resourceObject.errorChecking and (type_size < 0 or type_size > 8):
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
