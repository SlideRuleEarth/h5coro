import logging

import boto3
import botocore
from botocore.handlers import disable_signing
from botocore.config import Config
from botocore.exceptions import ReadTimeoutError, EndpointConnectionError
import socket

###############################################################################
# Globals
###############################################################################

logger = logging.getLogger(__name__)

###############################################################################
# Exceptions
###############################################################################


class FatalError(RuntimeError):
    pass


###############################################################################
# S3Driver Class
###############################################################################


class S3Driver:
    #######################
    # Constructor
    #######################
    def __init__(self, resource, credentials, max_connections=None):
        # Use the default value of 50 if max_connections is not provided
        if max_connections is None:
            max_connections = 50

        # Store the credentials for reuse
        self.cached_credentials = credentials

        # Construct path to resource
        self.resourcePath = list(filter(("").__ne__, resource.split("/")))

        # Initialize session based on credentials
        self.session = self.create_session(credentials)

        # Set up the HTTP connection pool
        self.config = Config(max_pool_connections=max_connections, read_timeout=30, retries={"max_attempts": 3})

        # Open the S3 resource object
        self.obj = self.session.resource("s3", config=self.config).Object(
            self.resourcePath[0], "/".join(self.resourcePath[1:])
        )

        # Initialize the _closed attribute to track resource closure
        self._closed = False

    #######################
    # Copy Constructor
    #######################
    def copy(self, max_connections=None):
        resource_str = "/".join(self.resourcePath)
        return S3Driver(resource_str, self.cached_credentials, max_connections)

    #######################
    # Create Boto3 Session
    #######################
    def create_session(self, credentials):
        if "annon" in credentials and credentials["annon"]:
            session = boto3.Session()
            session.events.register("choose-signer.s3.*", disable_signing)
            return session
        elif "role" in credentials and credentials["role"]:
            session = boto3.Session()
            return session
        elif "profile" in credentials:
            return boto3.Session(profile_name=credentials["profile"])
        elif (
            "aws_access_key_id" in credentials
            and "aws_secret_access_key" in credentials
            and "aws_session_token" in credentials
        ):
            return boto3.Session(
                aws_access_key_id=credentials["aws_access_key_id"],
                aws_secret_access_key=credentials["aws_secret_access_key"],
                aws_session_token=credentials["aws_session_token"],
            )
        elif (
            "accessKeyId" in credentials
            and "secretAccessKey" in credentials
            and "sessionToken" in credentials
        ):
            return boto3.Session(
                aws_access_key_id=credentials["accessKeyId"],
                aws_secret_access_key=credentials["secretAccessKey"],
                aws_session_token=credentials["sessionToken"],
            )
        elif len(credentials) == 0:
            return boto3.Session()
        else:
            raise FatalError(
                "invalid credential keys provided, looking for: aws_access_key_id, aws_secret_access_key, and aws_session_token"
            )

    #######################
    # read
    #######################
    def read(self, pos, size):
        try:
            stream = self.obj.get(Range=f"bytes={pos}-{pos+size-1}")["Body"]
            return stream.read()
        except (ReadTimeoutError, socket.timeout) as e:
            logger.error(f"Read timeout occurred: {e}")
            raise RuntimeError("Read operation timed out") from e
        except EndpointConnectionError as e:
            logger.error(f"Connection error: {e}")
            raise RuntimeError("Connection error during read operation") from e
        except Exception as e:
            logger.error(f"Unexpected error during read: {e}")
            raise

    #######################
    # Close resources
    #######################
    def close(self):
        """Explicitly clean up the session and S3 object."""
        if not self._closed:
            if self.session is not None:
                try:
                    # If resource has close method, call it
                    if hasattr(self.obj, "close"):
                        self.obj.close()

                    # session component doesn't have a close method, this effectively closes the session
                    boto3.DEFAULT_SESSION = None

                except Exception as e:
                    logger.warning(f"Unexpected error while closing resources: {e}")

                # Ensure the session reference is removed
                self.session = None
                self.obj = None

            self._closed = True