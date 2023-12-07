import h5coro
from h5coro import s3driver, filedriver, logger
import earthaccess
import argparse
import logging

# Command Line Arguments #
parser = argparse.ArgumentParser(description="""Deep inspection of a subgroup""")
parser.add_argument('--granule','-f', type=str, default="nsidc-cumulus-prod-protected/ATLAS/ATL03/006/2018/10/17/ATL03_20181017222812_02950102_006_02.h5")
parser.add_argument('--variables','-x', nargs='+', type=str, default=["/gt2l/heights/h_ph"])
parser.add_argument('--group','-g', nargs='+', type=str, default="/gt2l/heights")
parser.add_argument('--slice','-s', nargs=2, type=int, default=[0,10])
parser.add_argument('--profile','-p', type=str, default="default")
parser.add_argument('--driver','-d', type=str, default="s3") # s3 or file
parser.add_argument('--enableAttributes','-a', action='store_true', default=False)
parser.add_argument('--checkErrors','-e', action='store_true', default=False)
parser.add_argument('--verbose','-v', action='store_true', default=False)
parser.add_argument('--loglevel','-l', type=str, default="unset")
parser.add_argument('--daac','-q', type=str, default="NSIDC")
args,_ = parser.parse_known_args()

# Conifugre I/O Driver #
if args.driver == "file":
    args.driver = filedriver.FileDriver
elif args.driver == "s3":
    args.driver = s3driver.S3Driver
else:
    args.driver = None

# Configure Logging #
if args.loglevel == "unset":
    args.loglevel = logging.CRITICAL
else:
    args.verbose = True
logger.config(logLevel=args.loglevel)

# Configure Credentials #
credentials = {"profile":args.profile}
if args.daac != "None":
    auth = earthaccess.login()
    s3_creds = auth.get_s3_credentials(daac=args.daac)
    credentials = { "aws_access_key_id": s3_creds["accessKeyId"],
                    "aws_secret_access_key": s3_creds["secretAccessKey"],
                    "aws_session_token": s3_creds["sessionToken"] }
