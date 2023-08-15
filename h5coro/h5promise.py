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

import concurrent.futures
from threading import Condition, Thread

###############################################################################
# Thread Functions
###############################################################################

def resultThread(promise, futures):
    for future in concurrent.futures.as_completed(futures):
        h5dataset = future.result()
        promise.conditions[h5dataset.dataset].acquire()
        promise.datasets[h5dataset.dataset] = h5dataset
        promise.conditions[h5dataset.dataset].notify()
        promise.conditions[h5dataset.dataset].release()

###############################################################################
# H5Promise Class
###############################################################################

class H5Promise:

    #######################
    # Constructor
    #######################
    def __init__(self, datasets, futures, block):
        # initialize dataset values
        self.datasets = {}
        self.conditions = {}
        for dataset in datasets:
            self.datasets[dataset] = None
            self.conditions[dataset] = Condition()
        # wait for datasets to be populated OR populate datasets in the background
        if block:
            resultThread(self, futures)
        else:
            Thread(target=resultThread, args=(self,futures), daemon=True).start()

    #######################
    # waitOnResult
    #######################
    def waitOnResult(self, dataset, timeout=None):
        if dataset in self.conditions:
            self.conditions[dataset].acquire()
            while self.datasets[dataset] == None:
                self.conditions[dataset].wait(timeout=timeout)
            self.conditions[dataset].release()
            return True
        else:
            return False

    #######################
    # operator: []
    #######################
    def __getitem__(self, key):
        self.waitOnResult(key)
        return self.datasets[key]

    #######################
    # representation
    #######################
    def __repr__(self):
        rstr = '{ '
        total_count = len(self.keys())
        count = 1
        for dataset in self.keys():
            separator = count == total_count and ' ' or ','
            rstr += f'"{dataset}": {self.datasets[dataset]}{separator}'
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
        for key in self.datasets.keys():
            yield key

    #######################
    # keys
    #######################
    def keys(self):
        return self.datasets.keys()

