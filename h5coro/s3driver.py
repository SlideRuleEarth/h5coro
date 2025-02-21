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
    _shared_session = None  # Class-level shared session
    _global_max_connections = 100  # Ensure enough sockets for high concurrency

    @classmethod
    def get_shared_session(cls, credentials):
        """ Returns a shared session with authentication handling. """
        if cls._shared_session is None:
            if "annon" in credentials and credentials["annon"]:
                session = boto3.Session()
                session.events.register("choose-signer.s3.*", disable_signing)
            elif "profile" in credentials:
                session = boto3.Session(profile_name=credentials["profile"])
            elif (
                "aws_access_key_id" in credentials
                and "aws_secret_access_key" in credentials
            ):
                session = boto3.Session(
                    aws_access_key_id=credentials["aws_access_key_id"],
                    aws_secret_access_key=credentials["aws_secret_access_key"],
                    aws_session_token=credentials.get("aws_session_token"),
                )
            else:
                session = boto3.Session()  # Default session

            cls._shared_session = session

        return cls._shared_session

    #######################
    # Constructor
    #######################
    def __init__(self, resource, credentials, max_connections=None):
        self.cached_credentials = credentials
        self.resourcePath = list(filter(("").__ne__, resource.split("/")))

        # If max_connections is set, use it. Otherwise, inherit the latest known global max_connections.
        self.max_connections = max_connections if max_connections is not None else S3Driver._global_max_connections

        # Ensure one shared session
        self.session = self.get_shared_session(self.cached_credentials)

        # Thread-safe client: one per instance, reused across all threads
        self.client = self.session.client(
            "s3",
            use_ssl=False,  # Remove SSL overhead (only if S3 allows non-SSL)
            config=boto3.session.Config(
                max_pool_connections=self.max_connections,  # Avoid connection queueing
                retries={'max_attempts': 2, 'mode': 'adaptive'},  # Lower retries due to high concurrency
                read_timeout=5,  # Lower read timeout to fail fast
                connect_timeout=2,  # Reduce connection stalls
                tcp_keepalive=True  # Keep TCP connections open for repeated use
            ),
        )

        # Extract bucket name and key
        self.bucket_name = self.resourcePath[0]
        self.key = "/".join(self.resourcePath[1:])
        self._closed = False

    #######################
    # read
    #######################
    def read(self, offset, size):
        """ Reads a specific range of bytes from an S3 object. """
        if self._closed:
            raise RuntimeError("S3Driver has been closed and cannot be used.")

        try:
            response = self.client.get_object(
                Bucket=self.bucket_name,
                Key=self.key,
                Range=f"bytes={offset}-{offset + size - 1}"
            )
            return response["Body"].read()
        except self.client.exceptions.NoSuchKey:
            logger.error(f"S3 key {self.key} does not exist in bucket {self.bucket_name}")
            return None
        except Exception as e:
            logger.error(f"Error reading {self.key} from {self.bucket_name}: {e}")
            return None

    #######################
    # Copy Constructor
    #######################
    def copy(self, max_connections=None):
        """Creates a new independent S3Driver instance with a specified max_connections."""
        return S3Driver("/".join(self.resourcePath), self.cached_credentials,
                        max_connections if max_connections is not None else S3Driver._global_max_connections)

    #######################
    # Close resources
    #######################
    def close(self):
        """ Cleans up the S3 client when done. """
        if not self._closed:
            try:
                self.client.close()  # Explicitly close the S3 client
            except AttributeError:
                pass  # Some boto3 versions do not have a close() method

            self._closed = True