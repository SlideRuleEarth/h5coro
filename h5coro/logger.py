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

import logging

# Constants
LOG_FORMAT = '%(created)f %(levelname)-5s [%(filename)s:%(lineno)5d] %(message)s'

# Create and Initialize Log
log = logging.getLogger(__name__)
format = logging.Formatter(LOG_FORMAT)
console = logging.StreamHandler()
console.setFormatter(format)
log.addHandler(console)

# Config Function
def config(logLevel):
    if logLevel == "DEBUG":
        logLevel = logging.DEBUG
    elif logLevel == "INFO":
        logLevel = logging.INFO
    elif logLevel == "WARNING":
        logLevel = logging.WARNING
    elif logLevel == "WARN":
        logLevel = logging.WARN
    elif logLevel == "ERROR":
        logLevel = logging.ERROR
    elif logLevel == "FATAL":
        logLevel = logging.FATAL
    elif logLevel == "CRITICAL":
        logLevel = logging.CRITICAL
    log.setLevel(logLevel)
    console.setLevel(logLevel)
    