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
import argparse
import h5coro
from h5coro import s3driver, filedriver

###############################################################################
# COMMAND LINE ARGUMENTS
###############################################################################

parser = argparse.ArgumentParser(description="""Subset ATL06 granules""")
parser.add_argument('--granule','-g', type=str, default="/data/ATLAS/ATL03_20181017222812_02950102_005_01.h5")
parser.add_argument('--variables','-x', nargs='+', type=str, default=["/gt2l/heights/h_ph"])
parser.add_argument('--profile','-p', type=str, default="default")
parser.add_argument('--driver','-d', type=str, default="file") # s3
parser.add_argument('--checkErrors','-e', action='store_true', default=False)
parser.add_argument('--verbose','-v', action='store_true', default=False)
parser.add_argument('--enableAttributes','-a', action='store_true', default=False)
parser.add_argument('--slice','-s', nargs=2, type=int, default=[0,10])
args,_ = parser.parse_known_args()

if args.driver == "file":
    args.driver = filedriver.FileDriver
elif args.driver == "s3":
    args.driver = s3driver.S3Driver
else:
    args.driver = None

###############################################################################
# MAIN
###############################################################################

try:
    h5coro.config(errorChecking=args.checkErrors, verbose=args.verbose, enableAttributes=args.enableAttributes, logLevel=logging.INFO)
    h5obj = h5coro.H5Coro(args.granule, args.driver, datasets=args.variables, block=False, credentials={"profile":args.profile})
    for variable in h5obj:
        print(f'{variable}: {h5obj[variable][args.slice[0]:args.slice[1]]}')
except Exception as e:
    print(f'{e.__class__.__name__}: {e}')
