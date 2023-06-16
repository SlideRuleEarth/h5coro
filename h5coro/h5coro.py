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
import netrc
import requests
import json
import struct
import ctypes
import time
import logging
import numpy
from datetime import datetime, timedelta
from h5coro.h5coro import version

###############################################################################
# GLOBALS
###############################################################################

DAAC = "nsidc"

session = requests.Session()
session.trust_env = False

verbose = False

request_timeout = (10, 60) # (connection, read) in seconds

logger = logging.getLogger(__name__)


###############################################################################
# EXCEPTIONS
###############################################################################

class FatalError(RuntimeError):
    pass

class TransientError(RuntimeError):
    pass

###############################################################################
# UTILITIES
###############################################################################


###############################################################################
# APIs
###############################################################################

#
#  placeholder
#
def placeholder (parm={}):
    '''
    Placeholder

    Parameters
    ----------
        parm:       dict
                    dictionary of request parameters

    Returns
    -------
    bool
        it's always true

    Examples
    --------
        >>> import h5coro
        >>> result = h5coro.placeholder({})
        >>> print(result)
        True
    '''
    return True