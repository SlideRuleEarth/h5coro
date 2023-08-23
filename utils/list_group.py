import h5coro
from h5coro import s3driver, filedriver
import earthaccess
import argparse
import logging

###############################################################################
# COMMAND LINE ARGUMENTS
###############################################################################

parser = argparse.ArgumentParser(description="""Deep inspection of a subgroup""")
parser.add_argument('--granule','-g', type=str, default="nsidc-cumulus-prod-protected/ATLAS/ATL03/006/2019/11/30/ATL03_20191130112041_09860505_006_01.h5")
parser.add_argument('--group','-x', nargs='+', type=str, default="/gt2l/heights")
parser.add_argument('--profile','-p', type=str, default="default")
parser.add_argument('--driver','-d', type=str, default="s3") # s3 or file
parser.add_argument('--checkErrors','-e', action='store_true', default=False)
parser.add_argument('--verbose','-v', action='store_true', default=False)
parser.add_argument('--daac','-c', type=str, default="NSIDC")
args,_ = parser.parse_known_args()

if args.driver == "file":
    args.driver = filedriver.FileDriver
elif args.driver == "s3":
    args.driver = s3driver.S3Driver
else:
    args.driver = None

credentials = {"profile":args.profile}
if args.daac != "None":
    auth = earthaccess.login()
    s3_creds = auth.get_s3_credentials(daac=args.daac)
    credentials = { "aws_access_key_id": s3_creds["accessKeyId"],
                    "aws_secret_access_key": s3_creds["secretAccessKey"],
                    "aws_session_token": s3_creds["sessionToken"] }

###############################################################################
# MAIN
###############################################################################

try:
    h5coro.config(logLevel=logging.ERROR)
    h5obj = h5coro.H5Coro(args.granule, args.driver, errorChecking=args.checkErrors, verbose=args.verbose, credentials=credentials)

    variables, attributes = h5obj.listGroup(args.group, w_attr=True, w_inspect=True)
    for variable, results in variables.items():
        print(f'{variable}: {results["metadata"]}')
        for attribute, value in results["attributes"].items():
            value_str = "<unsupported>"
            if value != None:
                value_str = f'{value.values}'
            print(f'  {attribute}: {value_str}')
except Exception as e:
    print(f'{e.__class__.__name__}: {e}')
    raise

