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

from h5coro.h5dataset import H5Dataset
from h5coro.h5promise import H5Promise, massagePath
from h5coro.h5metadata import H5Metadata
from h5coro.logger import log
from concurrent.futures import as_completed, ThreadPoolExecutor

###############################################################################
# CONSTANTS
###############################################################################

ENABLE_ATTRIBUTES_DEFAULT = False
EARLY_EXIT_DEFAULT = True
META_ONLY_DEFAULT = False
ERROR_CHECKING_DEFAULT = True
VERBOSE_DEFAULT = False

CACHE_LINE_SIZE_DEFAULT = 0x400000

ENABLE_PREFETCH_DEFAULT = False

###############################################################################
# H5Coro Functions
###############################################################################

def inspectThread(resourceObject, path, w_attr):
    try:
        _, attributes, metadata = resourceObject.inspectPath(path, w_attr=w_attr)
        return path, metadata, attributes
    except RuntimeError as e:
        log.warn(f'H5Coro encountered an error inspecting {path}: {e}')
        return path, H5Metadata(), {}

def isolateElement(path, group):
    if path.startswith(group):
        element = path[len(group):]
        if len(element) > 0:
            if element[0] == '/':
                element = element[1:]
            element = element.split('/')[0]
            return element
    return None

###############################################################################
# H5Coro Class
###############################################################################

class H5Coro:

    #######################
    # Constructor
    #######################
    def __init__(self,
        resource,
        driverClass,
        credentials={},
        cacheLineSize = CACHE_LINE_SIZE_DEFAULT,
        errorChecking = ERROR_CHECKING_DEFAULT,
        verbose = VERBOSE_DEFAULT,
        multiProcess = False
    ):
        self.resource = resource
        self.driver = driverClass(resource, credentials)

        self.errorChecking = errorChecking
        self.verbose = verbose
        self.multiProcess = multiProcess

        self.cacheLineSize = cacheLineSize
        self.cacheLineMask = (0xFFFFFFFFFFFFFFFF - (cacheLineSize-1))

        self.cache = {}
        self.pathAddresses = {}
        self.metadataTable = {}

        self.offsetSize = 0
        self.lengthSize = 0
        self.baseAddress = 0
        self.rootAddress = H5Dataset.readSuperblock(self)

    #######################
    # readDatasets
    #######################
    def readDatasets(self, datasets, block=True, earlyExit=EARLY_EXIT_DEFAULT, metaOnly=META_ONLY_DEFAULT, enableAttributes=ENABLE_ATTRIBUTES_DEFAULT):
        # check if datasets supplied
        if len(datasets) <= 0:
            return

        # make into dictionary
        dataset_table = {}
        for dataset in datasets:
            if type(dataset) == str:
                dataset = massagePath(dataset)
                dataset_table[dataset] = {"dataset": dataset, "hyperslice": []}
            else:
                dataset["dataset"] = massagePath(dataset["dataset"])
                dataset_table[dataset["dataset"]] = dataset

        # return promise
        return H5Promise(self, dataset_table, block, earlyExit=earlyExit, metaOnly=metaOnly, enableAttributes=enableAttributes)

    #######################
    # readPath
    #######################
    def inspectPath(self, path, w_attr=True):
        # initialize return values
        links = set()
        attributes = {}
        metadata = None

        # read elements at path
        H5Dataset(self, path, earlyExit=False, metaOnly=True, enableAttributes=w_attr)

        # pull out links and attributes from pathAddresses
        for _path in self.pathAddresses.keys():
            element = isolateElement(_path, path)
            if element != None:
                if _path in self.metadataTable and self.metadataTable[_path].isattribute:
                    attributes[element] = None
                else:
                    links.add(element)

        # pull out metadata from metadataTable
        if path in self.metadataTable:
            metadata = self.metadataTable[path]

        # read each attribute
        attr_paths = [os.path.join(path, attribute) for attribute in attributes]
        promise = self.readDatasets(attr_paths, enableAttributes=True)
        for attribute in attributes:
            attributes[attribute] = promise.datasets[os.path.join(path, attribute)].values

        # return results
        return links, attributes, metadata

    #######################
    # list
    #######################
    def list(self, path, w_attr=True):
        # sanatize inputs
        path = massagePath(path)

        # initialize return values
        variables = {}
        groups = {}

        # get links and attributes at specified path
        links, attributes, _ = self.inspectPath(path, w_attr)
        # inspect each link to get metadata, attributes, group info, etc
        if len(links) > 0:
            executor = ThreadPoolExecutor(max_workers=(len(links) + len(attributes)))
            futures = [executor.submit(inspectThread, self, f'{path}/{link}', w_attr) for link in links]
            for future in as_completed(futures):
                name, metadata, attrs = future.result() # overwrites attribute set
                element = isolateElement(name, path)
                if metadata == None: # group
                    groups[element] = {}
                    for attr in attrs:
                        groups[element][attr] = attrs[attr]
                elif metadata.type != None: # variable
                    variables[element] = {'__metadata__': metadata}
                    for attr in attrs:
                        variables[element][attr] = attrs[attr]

        # return results
        return variables, attributes, groups

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
                cache_line = (pos + self.baseAddress) & self.cacheLineMask
                # Populate Cache (if not there already)
                if cache_line not in self.cache:
                    self.cache[cache_line] = memoryview(self.driver.read(cache_line, self.cacheLineSize))
                # Update Indexes
                start_index = (pos + self.baseAddress) - cache_line
                stop_index = min(start_index + data_to_read, self.cacheLineSize)
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
            block_size = size + ((self.cacheLineSize - (size % self.cacheLineSize)) % self.cacheLineSize) # align to cache line boundary
            cache_line = (pos + self.baseAddress) & self.cacheLineMask
            data_block = memoryview(self.driver.read(cache_line, block_size))
            data_index = 0
            while data_index < block_size:
                # Cache the Line
                self.cache[cache_line] = data_block[data_index:data_index+self.cacheLineSize]
                cache_line += self.cacheLineSize
                data_index += self.cacheLineSize
            return None
        # Direct Read
        else:
            return self.driver.read(pos + self.baseAddress, size)
