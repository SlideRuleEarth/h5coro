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

import numpy

###############################################################################
# H5Metadata Class
###############################################################################

class H5Metadata:

    #######################
    # Constants
    #######################
    # filters
    DEFLATE_FILTER          = 1
    SHUFFLE_FILTER          = 2
    FLETCHER32_FILTER       = 3
    SZIP_FILTER             = 4
    NBIT_FILTER             = 5
    SCALEOFFSET_FILTER      = 6
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
    # numpy type conversion
    TO_NUMPY_TYPE = {
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
        }
    }

    #######################
    # Constructor
    #######################
    def __init__(self, address=0):
        self.ndims              = None
        self.dimensions         = []
        self.typeSize           = 0
        self.type               = None
        self.signedval          = True
        self.fillsize           = 0
        self.fillvalue          = None
        self.layout             = None
        self.size               = 0
        self.address            = address
        self.chunkElements      = 0
        self.chunkDimensions    = []
        self.elementSize        = 0
        self.isattribute        = False
        self.filter             = {
            self.DEFLATE_FILTER:        False,
            self.SHUFFLE_FILTER:        False,
            self.FLETCHER32_FILTER:     False,
            self.SZIP_FILTER:           False,
            self.NBIT_FILTER:           False,
            self.SCALEOFFSET_FILTER:    False
        }

    #######################
    # representation
    #######################
    def __repr__(self):
        typestr = f'{self.type} (unsupported)'
        if self.type == self.FIXED_POINT_TYPE or self.type == self.FLOATING_POINT_TYPE:
            datatype = self.TO_NUMPY_TYPE[self.type][self.signedval][self.typeSize]
            typestr = f'{datatype}'
        elif self.type == self.STRING_TYPE:
            typestr = f'{str}'
        return f'{{\"type\": {typestr}, \"dims\": {self.dimensions}}}'

    #######################
    # print
    #######################
    def __str__(self):
        return self.__repr__()
