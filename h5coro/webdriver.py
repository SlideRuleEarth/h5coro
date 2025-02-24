import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import time

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

class HTTPDriver:

    #######################
    # Constructor
    #######################
    def __init__(self, resource, credentials, max_connections=None, max_retries=3, backoff_factor=1):
        """HTTP driver for H5Coro

        Parameters:
            resource (String): HTTP URL pointing to a NASA file
            credentials (String): EDL token
            max_connections (int): Maximum number of concurrent connections
            max_retries (int): Maximum number of retries for failed requests
            backoff_factor (int): Time factor for exponential backoff between retries
        """
        # Use the default value of 50 if max_connections is not provided
        if max_connections is None:
            max_connections = 50

        # Store the credentials for reuse
        self.cached_credentials = credentials

        # Construct path to resource
        self.resource = resource
        self.session = requests.Session()

        # Configure retries and backoff
        retry_strategy = Retry(
            total=max_retries,                           # Maximum number of retries
            status_forcelist=[500, 502, 503, 504],       # Retry on these HTTP error codes
            allowed_methods=["HEAD", "GET", "OPTIONS"],  # Retry only for safe methods
            backoff_factor=backoff_factor                # Exponential backoff factor (1s, 2s, 4s, ...)
        )

        # Set up the HTTP connection pool with retry strategy
        self.adapter = HTTPAdapter(pool_connections=max_connections, pool_maxsize=max_connections, max_retries=retry_strategy)
        self.session.mount("http://", self.adapter)
        self.session.mount("https://", self.adapter)

        # Set the Authorization header if credentials are provided
        if type(credentials) == str:
            self.token = credentials
            self.session.headers.update({"Authorization": f"Bearer {self.token}"})

     #######################
    # Copy Constructor
    #######################
    def copy(self, max_connections=None):
        """Create and return a copy of the current HTTPDriver instance."""
        new_driver = HTTPDriver(self.resource, self.cached_credentials, max_connections)
        return new_driver

    #######################
    # read
    #######################
    def read(self, pos, size):
        headers = {"Range": f"bytes={pos}-{pos+size-1}"}

        use_streaming = size > 8192   # Enable streaming for reads over 8KB
        timeout = 30                  # Don't let it block for more than 30 seconds

        try:
            # Perform the HTTP GET request with range headers
            stream = self.session.get(self.resource, headers=headers, allow_redirects=True,
                                      timeout=timeout, stream=use_streaming)

            # Success case: return content if status is 200 or 206 (Partial Content)
            if stream.status_code in [200, 206]:
                return stream.content

            # Log the error if it's not successful
            logger.warning(f"HTTP error {stream.status_code} - Failed to read")

        except requests.RequestException as e:
            logger.error(f"Request failed with error: {e}")

        # All retries handled by the session; raise FatalError on failure
        raise FatalError(f"Failed to read range {pos}-{pos+size-1} after retries")


    #######################
    # Close resources
    #######################
    def close(self):
        """Close the HTTP session and adapter sockets."""
        if self.session:
            try:
                self.session.close()
            except Exception as e:
                logger.error(f"Error while closing session: {e}")
            finally:
                self.session = None

        if self.adapter:
            try:
                self.adapter.close()
            except Exception as e:
                logger.error(f"Error while closing adapter: {e}")
            finally:
                self.adapter = None