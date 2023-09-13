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

###############################################################################
# H5Values Class
###############################################################################

class H5Values:

    #######################
    # Constructor
    #######################
    def __init__(self, _elements, _datasize, _numrows, _numcols, _datatype, _data):
        self.elements   = _elements
        self.datasize   = _datasize
        self.numrows    = _numrows
        self.numcols    = _numcols
        self.datatype   = _datatype
        self.data       = _data

    #######################
    # operator: []
    #######################
    def __getitem__(self, key):
        return self.data[key]

    #######################
    # length
    #######################
    def __len__(self):
        return len(self.data)

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
        return list(self.data)
