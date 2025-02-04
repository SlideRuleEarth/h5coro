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
from h5coro.logger import log
from datetime import datetime
from multiprocessing import shared_memory, Process
import struct
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
# EXCEPTIONS
###############################################################################

class FatalError(RuntimeError):
    pass

###############################################################################
# LOCAL FUNCTIONS
###############################################################################

def BTreeReader(dataset, buffer, level):
    # Create new io driver for this process
    dataset.resourceObject.driver = dataset.resourceObject.driver.copy()
    dataset.readBTreeV1(buffer, level)

###############################################################################
# H5DATASET CLASS
###############################################################################

class H5Dataset:

    #######################
    # Constants
    #######################
    # local
    CUSTOM_V1_FLAG          = 0x80
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
    def __init__(self, resourceObject, dataset, hyperslice=None, makeNull=False, enableFill=True, *, earlyExit, metaOnly, enableAttributes):
        # initialize object
        self.resourceObject         = resourceObject
        self.earlyExit              = earlyExit
        self.metaOnly               = metaOnly
        self.enableAttributes       = enableAttributes
        self.enableFill             = enableFill
        self.pos                    = self.resourceObject.rootAddress
        self.currObjHdrPos          = 0
        self.numElements            = 1 # recalculated below
        self.shape                  = [] # recalculated below
        self.dataset                = dataset
        self.hyperslice             = hyperslice
        self.datasetPath            = list(filter(('').__ne__, self.dataset.split('/')))
        self.datasetPathLevels      = len(self.datasetPath)
        self.datasetFound           = False
        self.dataChunkBufferSize    = 0
        self.meta                   = H5Metadata()
        self.sharedBuffer           = None
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
        elif len(self.hyperslice) > self.meta.ndims:
            raise FatalError(f'cannot provide hyperslice with more dimensions that dataset: {len(self.hyperslice)} > {self.meta.ndims}')
        elif (self.meta.filter[self.meta.DEFLATE_FILTER] or self.meta.filter[self.meta.SHUFFLE_FILTER]) and \
             ((self.meta.layout == self.COMPACT_LAYOUT) or (self.meta.layout == self.CONTIGUOUS_LAYOUT)):
            raise FatalError(f'filters unsupported on non-chunked layouts')

        # massage hyperslice
        for d in range(self.meta.ndims):
            if d < len(self.hyperslice):
                if self.hyperslice[d] == None:
                    self.hyperslice[d] = (0, self.meta.dimensions[d])
                elif len(self.hyperslice[d]) == 2:
                    if self.hyperslice[d][0] == None:
                        self.hyperslice[d][0] = 0
                    if self.hyperslice[d][1] == None:
                        self.hyperslice[d][1] = self.meta.dimensions[d]
                else:
                    raise FatalError(f'invalid hyperslice, must provide as list of ranges [x,y), got {self.hyperslice}')
            else:
                self.hyperslice.append((0, self.meta.dimensions[d]))

            # check for valid hyperslice
            if (self.hyperslice[d][1] < self.hyperslice[d][0]) or \
               (self.hyperslice[d][1] > self.meta.dimensions[d]) or \
               (self.hyperslice[d][0] < 0) or (self.hyperslice[d][1] < 0):
                    raise FatalError(f'invalid hyperslice, must provide as list of valid ranges, got {self.hyperslice}')

        # calculate shape and number of elements
        for d in range(self.meta.ndims):
            elements_in_dimension = self.hyperslice[d][1] - self.hyperslice[d][0]
            if elements_in_dimension > 0:
                self.numElements *= elements_in_dimension
            self.shape.append(elements_in_dimension)

        # calculate size of buffer
        buffer_size = self.meta.typeSize * self.numElements
        if buffer_size <= 0:
            log.warn(f'empty read: typeSize={self.meta.typeSize}, numElements={self.numElements}')
            return

        # allocate buffer
        if self.resourceObject.multiProcess:
            if self.meta.ndims > 0:
                self.sharedBuffer = shared_memory.SharedMemory(create=True, size=buffer_size)
                buffer = self.sharedBuffer.buf
        else:
            buffer = bytearray(buffer_size)

        # ###################################
        # read compact and contiguous layouts
        # ###################################
        if (self.meta.layout == self.COMPACT_LAYOUT) or (self.meta.layout == self.CONTIGUOUS_LAYOUT):
            if self.meta.ndims == 0:
                buffer = self.resourceObject.ioRequest(self.meta.address, buffer_size, caching=False)
            else:
                compact_buffer = self.resourceObject.ioRequest(self.meta.address, buffer_size, caching=False)
                self.readSlice(buffer, self.shape, self.hyperslice, compact_buffer, self.meta.dimensions, self.hyperslice)

        # ###################################
        # read chunked layouts
        # ###################################
        elif self.meta.layout == self.CHUNKED_LAYOUT:
            # chunk layout specific error checks
            if self.resourceObject.errorChecking:
                if self.meta.elementSize != self.meta.typeSize:
                    raise FatalError(f'chunk element size does not match data element size: {self.meta.elementSize} !=  {self.meta.typeSize}')
                elif self.meta.chunkElements <= 0:
                    raise FatalError(f'invalid number of chunk elements: {self.meta.chunkElements}')

            # fill buffer with fill value (if provided and if enabled)
            if self.enableFill and self.meta.fillsize > 0:
                fill_values = struct.pack('Q', self.meta.fillvalue)[:self.meta.fillsize]
                for i in range(0, buffer_size, self.meta.fillsize):
                    buffer[i:i+self.meta.fillsize] = fill_values

            # calculate data chunk buffer size
            self.dataChunkBufferSize = self.meta.chunkElements * self.meta.typeSize

            # calculate step size of each dimension in chunks
            # ... for example a 12x12x12 cube of unsigned chars
            # ... with a chunk size of 3x3x3 would be have 4x4x4 chunks
            # ... the step size in chunks is then 16x4x1
            self.dimensionsInChunks = [int(self.meta.dimensions[d] / self.meta.chunkDimensions[d]) for d in range(self.meta.ndims)]
            self.chunkStepSize = [1 for _ in range(self.meta.ndims)]
            for d in range(self.meta.ndims-1, 0, -1):
                self.chunkStepSize[d-1] = self.dimensionsInChunks[d] * self.chunkStepSize[d]

            # calculate position of first and last element in hyperslice
            hyperslice_in_chunks = [(int(self.hyperslice[d][0] / self.meta.chunkDimensions[d]), int(self.hyperslice[d][1] / self.meta.chunkDimensions[d])) for d in range(self.meta.ndims)]
            self.hypersliceChunkStart = sum([hyperslice_in_chunks[d][0] * self.chunkStepSize[d] for d in range(self.meta.ndims)])
            self.hypersliceChunkEnd = sum([hyperslice_in_chunks[d][1] * self.chunkStepSize[d] for d in range(self.meta.ndims)])

            # read b-tree
            self.pos = self.meta.address
            if self.resourceObject.multiProcess:
                reader = Process(target=BTreeReader, args=(self, buffer, dataset_level))
                reader.start()
                reader.join()
            else:
                self.readBTreeV1(buffer, dataset_level)

        elif self.resourceObject.errorChecking:
            raise FatalError(f'invalid data layout: {self.meta.layout}')

        # populate data
        if self.meta.type == H5Metadata.FIXED_POINT_TYPE or self.meta.type == H5Metadata.FLOATING_POINT_TYPE:
            elements = int(buffer_size / self.meta.typeSize)
            datatype = H5Metadata.TO_NUMPY_TYPE[self.meta.type][self.meta.signedval][self.meta.typeSize]
            self.values = numpy.frombuffer(buffer, dtype=datatype, count=elements)
            if self.meta.ndims > 1:
                self.values = self.values.reshape(self.shape)
        elif self.meta.type == H5Metadata.STRING_TYPE:
            self.values = ctypes.create_string_buffer(buffer).value.decode('ascii')
        else:
            log.warn(f'{self.dataset} is an unsupported datatype {self.meta.type}: unable to populate values')

    #######################
    # Destructor
    #######################
    def __del__(self):
        self.values = None
        if self.sharedBuffer != None:
            self.sharedBuffer.close()
            self.sharedBuffer.unlink()

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

        # display file information
        if resourceObject.verbose:
            log.info(f'File Information @0x{root_group_offset:x}')
            log.info(f'Size of Offsets:      {resourceObject.offsetSize}')
            log.info(f'Size of Lengths:      {resourceObject.lengthSize}')
            log.info(f'Base Address:         {resourceObject.baseAddress}')
            log.info(f'Root Group Offset:    0x{root_group_offset:x}')

        # return root group offset
        return root_group_offset

    #######################
    # readObjHdr
    #######################
    def readObjHdr(self, dlvl):
        # check mata data table
        for lvl in range(self.datasetPathLevels, dlvl, -1):
            group_path = '/'.join(self.datasetPath[:lvl])
            if group_path in self.resourceObject.pathAddresses:
                self.pos = self.resourceObject.pathAddresses[group_path]
                dlvl = lvl
                break
        # process header
        self.currObjHdrPos = self.pos
        version_peek = self.readField(1)
        self.pos = self.currObjHdrPos
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
            log.info(f'<<Object Information V0 - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')

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
                log.info(f'Access Time:          {datetime.fromtimestamp(access_time)}')
                log.info(f'Modification Time:    {datetime.fromtimestamp(modification_time)}')
                log.info(f'Change Time:          {datetime.fromtimestamp(change_time)}')
                log.info(f'Birth Time:           {datetime.fromtimestamp(birth_time)}')
            else:
                self.pos += 16

        # phase attributes
        if obj_hdr_flags & STORE_CHANGE_PHASE_BIT:
            if self.resourceObject.verbose:
                max_compact_attr = self.readField(2)
                max_dense_attr = self.readField(2)
                log.info(f'Max Compact Attr:     {max_compact_attr}')
                log.info(f'Max Dense Attr:       {max_dense_attr}')
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
            log.info(f'<<Object Information V1 - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            log.info(f'# Header Messages:    {num_hdr_msgs}')

            # read object reference count
            obj_ref_count = self.readField(4)
            log.info(f'Obj Reference Count:  {obj_ref_count}')
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
                raise FatalError(f'header v1 message of type {msg_type} different size than specified: {bytes_read} != {msg_size}')

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
            # It is unclear why messages of size zero appear in the h5 files,
            # but in cases where it appears, the code has been able to continue
            # to successfully parse the file when the msg_type is zero; but
            # when there are zero length messages of a non-nill type, it is possible
            # a previous message has been padded and this code is not correctly picking
            # up the padding.  Regardless, something has gone wrong and we must abort
            if msg_type != 0 and msg_size == 0:
                raise FatalError(f'Invalid Message - {self.dataset}[{dlvl}] @0x{self.pos:x}: 0x{msg_type:x}, {msg_size}')
            if self.resourceObject.verbose:
                log.info(f'<<Skipped Message - {self.dataset}[{dlvl}] @0x{self.pos:x}: 0x{msg_type:x}, {msg_size}>>')
            self.pos += msg_size
            return msg_size

    #######################
    # dataspaceMsgHandler
    #######################
    def dataspaceMsgHandler(self, msg_size, obj_hdr_flags, dlvl, meta=None):
        if meta == None:
            meta = self.meta

        MAX_DIM_PRESENT    = 0x1
        PERM_INDEX_PRESENT = 0x2
        starting_position  = self.pos
        version            = self.readField(1)
        meta.ndims         = self.readField(1)
        flags              = self.readField(1)
        self.pos          += ((version == 1) and 5 or 1) # go past reserved bytes

        if self.resourceObject.verbose:
            log.info(f'<<Dataspace Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            log.info(f'Version:              {version}')
            log.info(f'Dimensionality:       {meta.ndims}')
            log.info(f'Flags:                {flags}')

        # check version and flags and dimenstionality
        if self.resourceObject.errorChecking:
            if version != 1 and version != 2:
                raise FatalError(f'unsupported dataspace version: {version}')
            if flags & PERM_INDEX_PRESENT:
                raise FatalError(f'unsupported permutation indexes')

        # read and populate data dimensions
        meta.dimensions = []
        if meta.ndims > 0:
            for x in range(meta.ndims):
                dimension = self.readField(self.resourceObject.lengthSize)
                meta.dimensions.append(dimension)
                if self.resourceObject.verbose:
                    log.info(f'Dimension {x}:          {dimension}')

            # skip over dimension permutations
            if flags & MAX_DIM_PRESENT:
                skip_bytes = meta.ndims * self.resourceObject.lengthSize
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
            log.info(f'<<Link Information Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            log.info(f'Version:              {version}')
            log.info(f'Flags:                {flags}')

        # check version
        if self.resourceObject.errorChecking and (version != 0):
            raise FatalError(f'unsupported link info version: {version}')

        # read maximum creation index
        if flags & MAX_CREATE_PRESENT_BIT:
            max_create_index = self.readField(8)
            if self.resourceObject.verbose:
                log.info(f'Max Create Index:     {max_create_index}')

        # read heap address and name index
        heap_address = self.readField(self.resourceObject.offsetSize)
        name_index = self.readField(self.resourceObject.offsetSize)
        if self.resourceObject.verbose:
            log.info(f'Heap Address:         0x{heap_address:x}')
            log.info(f'Name Index:           0x{name_index:x}')

        # read address of v2 B-tree for creation order index
        if flags & CREATE_ORDER_PRESENT_BIT:
            create_order_index = self.readField(self.resourceObject.offsetSize)
            if self.resourceObject.verbose:
                log.info(f'Create Order Index:   0x{create_order_index:x}')

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
    def datatypeMsgHandler(self, msg_size, obj_hdr_flags, dlvl, meta=None):
        if meta == None:
            meta = self.meta

        starting_position   = self.pos
        version_class       = self.readField(4)
        meta.typeSize       = self.readField(4)
        version             = (version_class & 0xF0) >> 4
        databits            = version_class >> 8
        meta.type           = version_class & 0x0F
        meta.signedval      = ((databits & 0x08) >> 3) == 1

        # display
        if self.resourceObject.verbose:
            log.info(f'<<Data Type Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            log.info(f'Version:              {version}')
            log.info(f'Type Size:            {meta.typeSize}')
            log.info(f'Data Type:            {meta.type}')
            log.info(f'Signed:               {meta.signedval}')

        # check version
        if self.resourceObject.errorChecking and version != 1:
            raise FatalError(f'unsupported datatype version {version}')

        # Fixed Point
        if meta.type == H5Metadata.FIXED_POINT_TYPE:
            if self.resourceObject.verbose:
                byte_order      = databits & 0x1
                pad_type        = (databits & 0x06) >> 1
                bit_offset      = self.readField(2)
                bit_precision   = self.readField(2)
                log.info(f'Byte Order:           {byte_order}')
                log.info(f'Pad Type:             {pad_type}')
                log.info(f'Bit Offset:           {bit_offset}')
                log.info(f'Bit Precision:        {bit_precision}')
            else:
                self.pos += 4
        # Floating Point
        elif meta.type == H5Metadata.FLOATING_POINT_TYPE:
            meta.signedval = True
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
                log.info(f'Byte Order:           {byte_order}')
                log.info(f'Pad Type:             {pad_type}')
                log.info(f'Mantissa Norm:        {mant_norm}')
                log.info(f'Sign Location:        {sign_loc}')
                log.info(f'Bit Offset:           {bit_offset}')
                log.info(f'Bit Precision:        {bit_precision}')
                log.info(f'Exponent Location:    {exp_location}')
                log.info(f'Exponent Size:        {exp_size}')
                log.info(f'Mantissa Location:    {mant_location}')
                log.info(f'Mantissa Size:        {mant_size}')
                log.info(f'Exponent Bias:        {exp_bias}')
            else:
                self.pos += 12
        # Reference
        elif meta.type == H5Metadata.COMPOUND_TYPE:
            meta.signedval = True
            log.warn(f'Compound datatype is not currently supported: unable to fully inspect {self.dataset}')
            self.pos = starting_position + msg_size
        # Reference
        elif meta.type == H5Metadata.REFERENCE_TYPE:
            meta.signedval = True
            if self.resourceObject.verbose:
                ref_type = databits & 0xF
                ref_ver  = (databits >> 4) & 0xF
                try:
                    ref_str  = {
                        0: "H5R_OBJECT1",
                        1: "H5R_DATASET_REGION1",
                        2: "H5R_OBJECT2",
                        3: "H5R_DATASET_REGION2",
                        4: "H5R_ATTR"
                    }[ref_type]
                except:
                    if self.resourceObject.errorChecking:
                        raise FatalError(f'unrecognized reference type: {ref_type}')
                    ref_str = "unrecognized"
                log.info(f'Reference Type:       {ref_str}')
        # Variable Length
        elif meta.type == H5Metadata.VARIABLE_LENGTH_TYPE:
            if self.resourceObject.verbose:
                vl_type = databits & 0xF # variable length type
                padding = (databits & 0xF0) >> 4
                charset = (databits & 0xF00) >> 8

                vl_type_str = "unknown"
                if vl_type == 0:
                    vl_type_str = "Sequence"
                elif vl_type == 1:
                    vl_type_str = "String"

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

                log.info(f'Variable Type:        {vl_type_str}')
                log.info(f'Padding Type:         {padding_str}')
                log.info(f'Character Set:        {charset_str}')

            # save off
            vlen_type_size  = meta.typeSize
            vlen_type       = meta.type
            vlen_signedval  = meta.signedval

            # recursively call datatype message
            self.datatypeMsgHandler(0, obj_hdr_flags, dlvl, meta)
        # String
        elif meta.type == H5Metadata.STRING_TYPE:
            meta.signedval = True
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

                log.info(f'Padding Type:         {padding_str}')
                log.info(f'Character Set:        {charset_str}')
        # Default
        elif self.resourceObject.errorChecking:
            raise FatalError(f'unsupported datatype {meta.type}')

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
            log.info(f'<<Fill Value Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            log.info(f'Version:              {version}')

        # check version
        if self.resourceObject.errorChecking and (version != 2) and (version != 3):
            raise FatalError(f'invalid fill value version: {version}')

        # version 2
        if version == 2:
            if self.resourceObject.verbose:
                space_allocation_time = self.readField(1)
                fill_value_write_time = self.readField(1)
                log.info(f'Space Allocation Time:{space_allocation_time}')
                log.info(f'Fill Value Write Time:{fill_value_write_time}')
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
                log.info(f'Fill Flags:           {flags}')

            if flags & FILL_VALUE_DEFINED:
                self.meta.fillsize = self.readField(4)
                self.meta.fillvalue = self.readField(self.meta.fillsize)

        # display
        if self.resourceObject.verbose:
            log.info(f'Fill Value Size:      {self.meta.fillsize}')
            log.info(f'Fill Value:           {self.meta.fillvalue}')

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
            log.info(f'<<Link Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            log.info(f'Version:              {version}')
            log.info(f'Flags:                {flags}')

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
            log.info(f'Link Type:            {link_type}')
            log.info(f'Creation Order:       {create_order}')
            log.info(f'Character Set:        {char_set}')
            log.info(f'Link Name:            {link_name}')

        # check if follow link
        follow_link = False
        if dlvl < len(self.datasetPath) and link_name == self.datasetPath[dlvl]:
            follow_link = True

        # process link
        if link_type == HARD_LINK:
            obj_hdr_addr = self.readField(self.resourceObject.offsetSize)
            if self.resourceObject.verbose:
                log.info(f'Hard Link:            0x{obj_hdr_addr:x}')
            # update meta data table
            group_path = '/'.join(self.datasetPath[:dlvl] + [link_name])
            self.resourceObject.pathAddresses[group_path] = obj_hdr_addr
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
                log.info(f'Soft Link:            {soft_link}')
            if self.resourceObject.errorChecking and follow_link:
                raise FatalError(f'unsupported soft link encountered: {soft_link}')

        elif link_type == EXTERNAL_LINK:
            ext_link_len = self.readField(2)
            ext_link = self.readArray(ext_link_len).tobytes().decode('utf-8')
            if self.resourceObject.verbose:
                log.info(f'External Link:        {ext_link}')
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
            log.info(f'<<Data Layout Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            log.info(f'Version:              {version}')
            log.info(f'Layout:               {self.meta.layout}')

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
                log.info(f'Element Size:         {self.meta.elementSize}')
                log.info(f'# Chunked Dimensions: {chunk_num_dim}')
                for d in range(chunk_num_dim):
                    log.info(f'Chunk Dimension {d}:    {self.meta.chunkDimensions[d]}')
        elif self.resourceObject.errorChecking:
            raise FatalError(f'unsupported data layout: {self.meta.layout}')

        # display
        if self.resourceObject.verbose:
            log.info(f'Dataset Size:         {self.meta.size}')
            log.info(f'Dataset Address:      {self.meta.address}')

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
            log.info(f'<<Filter Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            log.info(f'Version:              {version}')
            log.info(f'Num Filters:          {num_filters}')

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
            if (version == 1) or (filter_id >= 256):
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
                log.info(f'Filter ID:            {filter_id}')
                log.info(f'Flags:                {flags}')
                log.info(f'# Parameters:         {num_parms}')
                log.info(f'Filter Name:          {filter_name}')

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
        v2_3_flags          = self.readField(1)
        name_size           = self.readField(2)
        datatype_size       = self.readField(2)
        dataspace_size      = self.readField(2)

        # display
        if self.resourceObject.verbose:
            log.info(f'<<Attribute Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            log.info(f'Version:              {version}')

        # check version
        if self.resourceObject.errorChecking and (version != 1) and (version != 2) and (version != 3):
            raise FatalError(f'invalid attribute version: {version}')

        # check shared messages
        if self.resourceObject.errorChecking and (version != 1) and (v2_3_flags != 0):
            raise FatalError(f'unsupported shared messages for attribute: {v2_3_flags}')

        # character encoding
        char_encoding = 'utf-8'
        if version == 3:
            name_character_set_encoding = self.readField(1)
            if name_character_set_encoding == 0:
                char_encoding = 'ascii'
            elif name_character_set_encoding == 1:
                char_encoding = 'utf-8'
            elif self.resourceObject.errorChecking:
                raise FatalError(f'invalid character set encoding: {name_character_set_encoding}')

        # pad out message sizes (version 1 only)
        if version == 1:
            datatype_size += ((PAD_SIZE - (datatype_size % PAD_SIZE)) % PAD_SIZE)
            dataspace_size += ((PAD_SIZE - (dataspace_size % PAD_SIZE)) % PAD_SIZE)

        # read attribute name
        attr_name = self.readArray(name_size).tobytes().decode(char_encoding).strip('\0')
        attr_path = '/'.join(self.datasetPath[:dlvl] + [attr_name])

        # pad out name size (version 1 only)
        if version == 1:
            self.pos += (PAD_SIZE - (name_size % PAD_SIZE)) % PAD_SIZE; # align to next x-byte boundary

        # display
        if self.resourceObject.verbose:
            log.info(f'Name:                 {attr_name}')
            log.info(f'Message Size:         {msg_size}')
            log.info(f'Datatype Size:        {datatype_size}')
            log.info(f'Dataspace Size:       {dataspace_size}')

        # initialize local meta structure
        meta = H5Metadata()

        # read datatype message
        datatype_bytes_read = self.datatypeMsgHandler(datatype_size, obj_hdr_flags, dlvl, meta)
        pad_bytes = (PAD_SIZE - (datatype_bytes_read % PAD_SIZE)) % PAD_SIZE # align to next x-byte boundary
        if self.resourceObject.errorChecking and ((datatype_bytes_read + pad_bytes) != datatype_size):
            raise FatalError(f'failed to read expected bytes for datatype message: {datatype_bytes_read + pad_bytes} != {datatype_size}')
        self.pos += pad_bytes

        # read dataspace message
        dataspace_bytes_read = self.dataspaceMsgHandler(dataspace_size, obj_hdr_flags, dlvl, meta)
        pad_bytes = (PAD_SIZE - (dataspace_bytes_read % PAD_SIZE)) % PAD_SIZE # align to next x-byte boundary
        if self.resourceObject.errorChecking and ((dataspace_bytes_read + pad_bytes) != dataspace_size):
            raise FatalError(f'failed to read expected bytes for dataspace message: {dataspace_bytes_read + pad_bytes} != {dataspace_size}')
        self.pos += pad_bytes

        # set meta data
        meta.isattribute = True
        meta.layout = self.CONTIGUOUS_LAYOUT
        for f in meta.filter.keys():
            meta.filter[f] = False
        meta.address = self.pos
        meta.size = msg_size - (self.pos - starting_position)
        self.resourceObject.metadataTable[attr_path] = meta
        self.resourceObject.pathAddresses[attr_path] = self.currObjHdrPos

        # move to end of data
        self.pos += meta.size

        # check if desired attribute
        if( ((dlvl + 1) == len(self.datasetPath)) and
            (attr_name == self.datasetPath[dlvl]) ):
            self.datasetFound = True
            self.meta = meta

        # return bytes read
        return self.pos - starting_position

    #######################
    # headercontMsgHandler
    #######################
    def headercontMsgHandler(self, msg_size, obj_hdr_flags, dlvl):
        starting_position   = self.pos
        hc_offset           = self.readField(self.resourceObject.offsetSize)
        hc_length           = self.readField(self.resourceObject.lengthSize)

        # display
        if self.resourceObject.verbose:
            log.info(f'<<Header Continuation Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            log.info(f'Offset:               0x{hc_offset:x}')
            log.info(f'Length:               {hc_length}')

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
            log.info(f'<<Symbol Table Message - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            log.info(f'B-Tree Address:       {btree_addr}')
            log.info(f'Heap Address:         {heap_addr}')

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
                log.info(f'Entries Used:         {entries_used}')
                log.info(f'Left Sibling:         {left_sibling}')
                log.info(f'Right Sibling:        {right_sibling}')
                log.info(f'First Key:            {key0}')

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

        # reset position and return bytes read
        self.pos = return_position
        return return_position - starting_position

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
            log.info(f'<<Attribute Info - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            log.info(f'Version:              {version}')
            log.info(f'Flags:                {flags}')

        # check version
        if self.resourceObject.errorChecking and (version != 0):
            raise FatalError(f'unsupported link info version: {version}')

        # read maximum creation index (number of elements in group)
        if flags & MAX_CREATE_PRESENT_BIT:
            max_create_index = self.readField(2)
            if self.resourceObject.verbose:
                log.info(f'Max Creation Index:   {max_create_index}')

        # read heap and name offsets
        heap_address    = self.readField(self.resourceObject.offsetSize)
        name_index      = self.readField(self.resourceObject.offsetSize)
        if self.resourceObject.verbose:
            log.info(f'Heap Address:         {heap_address}')
            log.info(f'Name Index:           {name_index}')

        # read creation order index
        if flags & CREATE_ORDER_PRESENT_BIT:
            create_order_index = self.readField(self.resourceObject.offsetSize)
            if self.resourceObject.verbose:
                log.info(f'Creation Order Index: {create_order_index}')

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
            log.info(f'<<Symbol Table - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')

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
            link_name_offset    = self.readField(self.resourceObject.offsetSize)
            obj_hdr_addr        = self.readField(self.resourceObject.offsetSize)
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
                log.info(f'Link Name:            {link_name}')
                log.info(f'Obj Hdr Addr:         {obj_hdr_addr}')

            # update path address table
            group_path = '/'.join(self.datasetPath[:dlvl] + [link_name])
            self.resourceObject.pathAddresses[group_path] = obj_hdr_addr

            # process link
            if dlvl < len(self.datasetPath) and link_name == self.datasetPath[dlvl]:
                if cache_type == 2:
                    raise FatalError(f'symbolic links are unsupported: {link_name}')
                return_position = self.pos
                self.pos = obj_hdr_addr
                self.readObjHdr(dlvl + 1)
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
            log.info(f'<<Fractal Heap - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')
            log.info(f'Heap ID Length:       {heap_obj_id_len}')
            log.info(f'I/O Filters Length:   {io_filter_len}')
            log.info(f'Flags:                {flags}')
            log.info(f'Max Size of Objects:  {max_size_mg_obj}')
            log.info(f'Next Huge Object ID:  {next_huge_obj_id}')
            log.info(f'v2 B-tree Address:    0x{btree_addr_huge_obj:x}')
            log.info(f'Free Space in Blocks: {free_space_mg_blks}')
            log.info(f'Address Free Space:   0x{addr_free_space_mg:x}')
            log.info(f'Managed Space:        {mg_space}')
            log.info(f'Allocated Heap Space: {alloc_mg_space}')
            log.info(f'Direct Block Offset:  0x{dblk_alloc_iter:x}')
            log.info(f'Managed Heap Objects: {mg_objs}')
            log.info(f'Size of Huge Objects: {huge_obj_size}')
            log.info(f'Huge Objects in Heap: {huge_objs}')
            log.info(f'Size of Tiny Objects: {tiny_obj_size}')
            log.info(f'Tiny Objects in Heap: {tiny_objs}')
            log.info(f'Table Width:          {table_width}')
            log.info(f'Starting Block Size:  {starting_blk_size}')
            log.info(f'Max Direct Block Size:{max_dblk_size}')
            log.info(f'Max Heap Size:        {max_heap_size}')
            log.info(f'Starting # of Rows:   {start_num_rows}')
            log.info(f'Address of Root Block:0x{root_blk_addr:x}')
            log.info(f'Current # of Rows:    {curr_num_rows}')

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
            if self.resourceObject.verbose:
                log.info(f'Filtered Direct Block:{filter_root_dblk}')
                log.info(f'I/O Filter Mask:      {filter_mask}')
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
            log.info(f'<<Direct Block - {self.dataset}[{dlvl}] @0x{starting_position:x}: {heap_info["msg_type"]}, {block_size}>>')

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
            log.info(f'Heap Header Address:  {heap_hdr_addr}')
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
                    log.info(f'exiting direct block 0x{starting_position:x} early at 0x{self.pos:x}')
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
            log.info(f'<<Indirect Block - {self.dataset}[{dlvl}] @0x{starting_position:x}: {heap_info["msg_type"]}, {block_size}>>')

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
            log.info(f'Heap Header Address:  {heap_hdr_addr}')
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
            log.info(f'Number of Rows:       {nrows}')
            log.info(f'Max Direct Block Rows:{max_dblock_rows}')
            log.info(f'# Direct Blocks (K):  {K}')
            log.info(f'# Indirect Blocks (N):{N}')

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
    # hypersliceIntersection
    #######################
    def hypersliceIntersection(self, node_slice, node_level):
        if node_level == 0:
            # check for intersection for all dimensions
            for d in range(self.meta.ndims):
                if node_slice[d][1] < self.hyperslice[d][0] or node_slice[d][0] >= self.hyperslice[d][1]:
                    return False
            return True
        else: # node_level > 0
            # calculate chunk of node slice
            node_slice_in_chunks = [(int(node_slice[d][0] / self.meta.chunkDimensions[d]), int(node_slice[d][1] / self.meta.chunkDimensions[d])) for d in range(self.meta.ndims)]
            # calculate element position
            node_start = sum([node_slice_in_chunks[d][0] * self.chunkStepSize[d] for d in range(self.meta.ndims)])
            node_end = sum([node_slice_in_chunks[d][1] * self.chunkStepSize[d] for d in range(self.meta.ndims)])
            # check for intersection of position
            if node_end < self.hypersliceChunkStart or node_start > self.hypersliceChunkEnd:
                return False
            return True

    #######################
    # hypersliceSubset
    #######################
    def hypersliceSubset(self, chunk_slice):
        subset = []
        for d in range(self.meta.ndims):
            subset.append((max(chunk_slice[d][0], self.hyperslice[d][0]), min(chunk_slice[d][1], self.hyperslice[d][1])))
        return subset

    #######################
    # readSlice
    #######################
    def readSlice(self, output_buffer, output_dimensions, output_slice, input_buffer, input_dimensions, input_slice):

        # get number of dimensions
        ndims = len(input_dimensions)

        # build serialized size of each input and output dimension
        # ... for example a 4x4x4 cube of unsigned chars would be 16,4,1
        input_dim_step = [self.meta.typeSize for _ in range(ndims)]
        output_dim_step = [self.meta.typeSize for _ in range(ndims)]
        for d in range(ndims-1, 0, -1):
            input_dim_step[d-1] = input_dimensions[d] * input_dim_step[d]
            output_dim_step[d-1] = output_dimensions[d] * output_dim_step[d]

        # initialize dimension indices
        input_dim_index = [i[0] for i in input_slice] # initialize to the start index of each input_slice
        output_dim_index = [i[0] for i in output_slice] # initialize to the start index of each output_slice

        # calculate amount to read each time
        read_slice = input_slice[-1][1] - input_slice[-1][0]
        read_size = input_dim_step[-1] * read_slice # size of data to read each time

        # read each input_slice
        while input_dim_index[0] < input_slice[0][1]: # while the first dimension index has not traversed its range

            # calculate source offset
            src_offset = 0
            for d in range(ndims):
                src_offset += (input_dim_index[d] * input_dim_step[d])

            # calculate destination offset
            dst_offset = 0
            for d in range(ndims):
                dst_offset += (output_dim_index[d] * output_dim_step[d])

            # copy data from input buffer to output buffer
            output_buffer[dst_offset:dst_offset + read_size] = input_buffer[src_offset:src_offset + read_size]

            # go to next set of input indices
            input_dim_index[-1] += read_slice
            i = len(input_dim_index) - 1
            while i > 0 and input_dim_index[i] == input_slice[i][1]:    # while the level being examined is at the last index
                input_dim_index[i] = input_slice[i][0]                  # set index back to the beginning of hyperslice
                input_dim_index[i - 1] += 1                             # bump the previous index to the next element in dimension
                i -= 1                                                  # go to previous dimension

            # update output indices
            output_dim_index[-1] += read_slice
            i = len(output_dim_index) - 1
            while i > 0 and output_dim_index[i] == output_slice[i][1]:  # while the level being examined is at the last index
                output_dim_index[i] = output_slice[i][0]                # set index back to the beginning of hyperslice
                output_dim_index[i - 1] += 1                            # bump the previous index to the next element in dimension
                i -= 1                                                  # go to previous dimension

    #######################
    # readBTreeV1
    #######################
    def readBTreeV1(self, buffer, dlvl):
        starting_position = self.pos

        # display
        if self.resourceObject.verbose:
            log.info(f'<<B-Tree Node - {self.dataset}[{dlvl}] @0x{starting_position:x}>>')

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
            log.debug(f'Node Level:           {node_level}')
            log.debug(f'Entries Used:         {entries_used}')

        # skip sibling addresses
        self.pos += self.resourceObject.offsetSize * 2

        # read first key
        curr_node = self.readBTreeNodeV1(self.meta.ndims)

        # read children
        for e in range(entries_used):
            child_addr  = self.readField(self.resourceObject.offsetSize)
            next_node   = self.readBTreeNodeV1(self.meta.ndims)

            # construct node slice
            if node_level > 0:
                node_slice = [(start, stop) for start, stop in zip(curr_node['slice'], next_node['slice'])]
            else:
                node_slice = [(start, min(start + extent, dimension)) for start, extent, dimension in zip(curr_node['slice'], self.meta.chunkDimensions, self.meta.dimensions)]

            # display
            if self.resourceObject.verbose:
                log.info(f'level={node_level}, entry={e+1} of {entries_used}, node_slice={node_slice}, curr_node={curr_node["slice"]}, next_node={next_node["slice"]}')
                log.debug(f'Entry <{node_level}>:            {e}')
                log.debug(f'Chunk Size:           {curr_node["chunk_size"]} | {next_node["chunk_size"]}')
                log.debug(f'Filter Mask:          {curr_node["filter_mask"]} | {next_node["filter_mask"]}')
                log.debug(f'Node Slice:           {curr_node["slice"]} | {next_node["slice"]}')
                log.debug(f'Hyperslice:           {self.hyperslice}')
                log.debug(f'Chunk Dimensions:     {self.meta.chunkDimensions}')
                log.debug(f'Child Address:        0x{child_addr:x}')

            # check for short-cutting
            if self.meta.ndims <= 1 and self.hyperslice[0][1] < node_slice[0][0]:
                break

            # check inclusion
            if self.hypersliceIntersection(node_slice, node_level):
                # display
                if self.resourceObject.verbose:
                    log.info(f'entry {node_level}.{e+1} of selected')

                # process child entry
                if node_level > 0:
                    return_position = self.pos
                    self.pos = child_addr
                    self.readBTreeV1(buffer, dlvl)
                    self.pos = return_position

                elif self.meta.ndims == 0:
                    log.warn(f'Unexpected chunked read of a zero dimensional dataset')
                    pass # NOT SURE WHAT TO DO HERE, IS THIS POSSIBLE?

                elif self.meta.ndims == 1:
                    # calculate offsets
                    buffer_offset = self.meta.typeSize * self.hyperslice[0][0]
                    chunk_offset = curr_node['slice'][0] * self.meta.typeSize

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
                        log.debug(f'Chunk Offset:         {chunk_offset} ({int(chunk_offset/self.meta.typeSize)})')
                        log.debug(f'Buffer Index:         {buffer_index} ({int(buffer_index/self.meta.typeSize)})')
                        log.debug(f'Chunk Bytes:          {chunk_bytes} ({int(chunk_bytes/self.meta.typeSize)})\n')

                    # read chunk
                    if self.meta.filter[self.meta.DEFLATE_FILTER]:

                        # read data into chunk filter buffer (holds the compressed data)
                        chunk_buffer = self.resourceObject.ioRequest(child_addr, curr_node['chunk_size'])

                        # inflate directly into data buffer
                        if (chunk_bytes == self.dataChunkBufferSize) and (not self.meta.filter[self.meta.SHUFFLE_FILTER]):
                            buffer[buffer_index:buffer_index+chunk_bytes] = self.inflateChunk(chunk_buffer)

                        # inflate into data chunk buffer */
                        else:
                            chunk_buffer = self.inflateChunk(chunk_buffer)

                            # shuffle data chunk buffer into data buffer
                            if self.meta.filter[self.meta.SHUFFLE_FILTER]:
                                buffer[buffer_index:buffer_index+chunk_bytes] = self.shuffleChunk(chunk_buffer, chunk_index, chunk_bytes, self.meta.typeSize)

                            # copy data chunk buffer into data buffer
                            else:
                                buffer[buffer_index:buffer_index+chunk_bytes] = chunk_buffer[chunk_index:chunk_index+chunk_bytes]

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

                elif self.meta.ndims > 1:

                    # read entire chunk
                    chunk_buffer = self.resourceObject.ioRequest(child_addr, curr_node['chunk_size'])                       # read
                    if self.meta.filter[self.meta.DEFLATE_FILTER]:                                                          # if compressed
                        chunk_buffer = self.inflateChunk(chunk_buffer)                                                      #    decompress
                        if self.meta.filter[self.meta.SHUFFLE_FILTER]:                                                      # if shuffled
                            chunk_buffer = self.shuffleChunk(chunk_buffer, 0, self.dataChunkBufferSize, self.meta.typeSize) #    reshuffle

                    # get truncated slice to pull out of chunk
                    # (intersection of chunk_slice and hyperslice selection)
                    chunk_slice_to_read = self.hypersliceSubset(node_slice)

                    # build slice that is read
                    read_slice = []
                    for d in range(self.meta.ndims):
                        x0 = abs(chunk_slice_to_read[d][0] - node_slice[d][0])
                        x1 = x0 + abs(chunk_slice_to_read[d][1] - chunk_slice_to_read[d][0])
                        read_slice.append((x0, x1))

                    # build slice that is written
                    write_slice = []
                    for d in range(self.meta.ndims):
                        x0 = abs(chunk_slice_to_read[d][0] - self.hyperslice[d][0])
                        x1 = x0 + abs(chunk_slice_to_read[d][1] - chunk_slice_to_read[d][0])
                        write_slice.append((x0, x1))

                    # read subset of chunk into return buffer
                    self.readSlice(buffer, self.shape, write_slice, chunk_buffer, self.meta.chunkDimensions, read_slice)

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
        elements_to_shuffle = int(output_size / type_size)
        start_element = int(output_offset / type_size)
        for element_index in range(start_element, start_element + elements_to_shuffle):
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
