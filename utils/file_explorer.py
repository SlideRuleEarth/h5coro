from h5coro import h5coro, s3driver, filedriver, logger
import earthaccess
import argparse
import logging
import sys

# Command Line Arguments #
parser = argparse.ArgumentParser(description="""Interactive inspection of an HDF5 file""")
parser.add_argument('--granule',            type=str,               default="nsidc-cumulus-prod-protected/ATLAS/ATL03/006/2018/10/17/ATL03_20181017222812_02950102_006_02.h5")
parser.add_argument('--path',               type=str,               default="/")
parser.add_argument('--driver',             type=str,               default="s3") # s3 or file
parser.add_argument('--profile',            type=str,               default="default")
parser.add_argument('--enableAttributes',   action='store_true',    default=False)
parser.add_argument('--checkErrors',        action='store_true',    default=False)
parser.add_argument('--verbose',            action='store_true',    default=False)
parser.add_argument('--loglevel',           type=str,               default="unset")
parser.add_argument('--daac',               type=str,               default="NSIDC")
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
print(f'Authenticating to {args.daac}...', end='')
sys.stdout.flush()
credentials = {"profile":args.profile}
if args.daac != "None":
    auth = earthaccess.login()
    s3_creds = auth.get_s3_credentials(daac=args.daac)
    credentials = { "aws_access_key_id": s3_creds["accessKeyId"],
                    "aws_secret_access_key": s3_creds["secretAccessKey"],
                    "aws_session_token": s3_creds["sessionToken"] }
print(f'complete.')

# Open H5 Object #
print(f'Opening {args.granule}...', end='')
sys.stdout.flush()
h5obj = h5coro.H5Coro(args.granule, args.driver, errorChecking=args.checkErrors, verbose=args.verbose, credentials=credentials)
print(f'complete.')

# REPL #
current_path = args.path
while True:

    # State #
    print(f'Exploring {current_path}')
    label = f'(..)'
    print(f'{label:<5} <parent>')

    # Inspect Current Path #
    variables, attributes, groups = h5obj.list(current_path, w_attr=args.enableAttributes)

    # Initialize Selection List #
    selection = {}

    # List Variables #
    v = 0
    for variable in variables:
        label = f'(v{v})'
        print(f'{label:<5} {variable}')
        selection[f'v{v}'] = variable
        v += 1

    # List Groups #
    g = 0
    for group in groups:
        label = f'(g{g})'
        print(f'{label:<5} {group}')
        selection[f'g{g}'] = group
        g += 1

    # List Attributes #
    a = 0
    for attribute in attributes:
        label = f'(a{a})'
        print(f'{label:<5} {attribute}')
        selection[f'a{a}'] = attribute
        a += 1

    # Get User Input #
    line = input("> ").strip()
    try:
        if line == 'exit':
            break
        elif len(line) <= 0:
            pass
        elif line[0] == 'v':
            print(f'{selection[line]} - {variables[selection[line]]}')
        elif line[0] == 'a':
            print(f'{selection[line]} - {attributes[selection[line]]}')
        elif line[0] == 'g':
            print(f'{selection[line]} - {groups[selection[line]]}')
            if current_path == "/":
                current_path = selection[line]
            else:
                current_path = f'{current_path}/{selection[line]}'
        elif line == "..":
            current_path = '/'.join(current_path.split("/")[:-1])
            if current_path == "":
                current_path = "/"
    except Exception as e:
        print(f'Invalid entry')



