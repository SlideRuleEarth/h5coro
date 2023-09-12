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

from h5coro.h5dataset import H5Dataset
from h5coro.h5promise import H5Promise
import concurrent.futures
import logging
import sys

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
# LOGGING
###############################################################################

logger = logging.getLogger(__name__)
def config( logLevel=None,
            logFormat='%(created)f %(levelname)-5s [%(filename)s:%(lineno)5d] %(message)s' ):
    if logLevel != None:
        logging.basicConfig(stream=sys.stdout, level=logLevel, format=logFormat)

###############################################################################
# H5Coro Functions
###############################################################################

def inspectThread(resourceObject, variable, w_attr):
    try:
        metadata, attributes = resourceObject.inspectVariable(variable, w_attr=w_attr)
        return variable, metadata, attributes
    except RuntimeError as e:
        logger.warning(f'H5Coro encountered an error inspecting {variable}: {e}')
        return variable, {}, {}

def isolateElement(path, group):
    if path.startswith(group):
        element = path.split(group)[1]
        if len(element) > 0:
            if element[0] == '/':
                element = element[1:]
            element = element.split('/')[0]
            return element
    return None

def massagePath(path):
    if path[0] == '/':
        path = path[1:]
    if path[-1] == '/':
        path = path[:-1]
    return path

###############################################################################
# H5Coro Class
###############################################################################

class H5Coro:

    #######################
    # Constructor
    #######################
    def __init__(self, 
        resource, 
        driver_class, 
        credentials={}, 
        cacheLineSize = CACHE_LINE_SIZE_DEFAULT,
        enablePrefetch = ENABLE_PREFETCH_DEFAULT,
        errorChecking = ERROR_CHECKING_DEFAULT,
        verbose = VERBOSE_DEFAULT

    ):
        self.resource = resource
        self.driver = driver_class(resource, credentials)

        self.errorChecking = errorChecking
        self.verbose = verbose

        self.cacheLineSize = cacheLineSize
        self.cacheLineMask = (0xFFFFFFFFFFFFFFFF - (cacheLineSize-1))
        self.enablePrefetch = enablePrefetch

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
        datasetTable = {}
        for dataset in datasets:
            dataset = massagePath(dataset)
            if type(dataset) == str:
                datasetTable[dataset] = {"dataset": dataset, "startrow": 0, "numrows": H5Dataset.ALL_ROWS}
            else:
                datasetTable[dataset["dataset"]] = dataset

        # return promise
        return H5Promise(self, datasetTable, block, earlyExit=earlyExit, metaOnly=metaOnly, enableAttributes=enableAttributes)

    #######################
    # inspectVariable
    #######################
    def inspectVariable(self, variable, w_attr=True):
        variable = massagePath(variable)
        # get metadata for variable
        promise = self.readDatasets([variable], block=True, earlyExit=True, metaOnly=True, enableAttributes=False)
        metadata = promise.datasets[variable].meta
        # if attributes request
        attributes = {}
        if w_attr:
            print("INSPECT", variable)
            # list attributes associated with variable
            _, attrs = self.listGroup(variable, w_attr)
            attr_paths = [f'{variable}/{attr}' for attr in attrs]
            print("ATTR", attr_paths)
            # read each attribute
            promise = self.readDatasets(attr_paths, enableAttributes=True)
            for attr in attrs:
                attributes[attr] = promise.datasets[f'{variable}/{attr}'].values

        # return results
        return metadata, attributes

    #######################
    # listGroup
    #######################
    def listGroup(self, group, w_attr=True, w_inspect=False):
        group = massagePath(group)
        variables = set()
        attributes = set()
        listing = {}

        try:
            # read elements in group
            H5Dataset(self, group, earlyExit=False, metaOnly=True, enableAttributes=w_attr)
    
            # populate variables and attributes by filtering pathAddresses 
            # for all entries starting with group string
            print("GROUP", group)
            for path in self.pathAddresses.keys():
                element = isolateElement(path, group)
                if element != None:
                    if path in self.metadataTable and self.metadataTable[path].isattribute:
                        print("... ATTRIBUTE", element)
                        attributes.add(element)
                    else:
                        variables.add(element)
                        print("... VARIABLE", element)

            # inspect each variable to get datatype, dimensions, and optionally the attributes
            if w_inspect and len(variables) > 0:
                executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(variables))
                futures = [executor.submit(inspectThread, self, f'{group}/{variable}', w_attr) for variable in variables]
                for future in concurrent.futures.as_completed(futures):
                    variable, metadata, attributes = future.result()
                    element = isolateElement(variable, group)
                    listing[element] = {'__metadata__': metadata}
                    for attribute in attributes:
                        listing[element][attribute] = attributes[attribute]

        except RuntimeError as e:
            print(f'H5Coro encountered an error listing the group {group}: {e}')
            logger.debug(f'H5Coro encountered an error listing the group {group}: {e}')

        # return results
        if w_inspect:
            return listing
        else:
            return variables, attributes

    #######################
    # readAttribute
    #######################
    def readAttribute(self, attribute):
        attribute = massagePath(attribute)
        promise = self.readDatasets([attribute], block=True, enableAttributes=True)
        return promise[attribute].values

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
