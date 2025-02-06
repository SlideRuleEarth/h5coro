import requests
from requests.adapters import HTTPAdapter
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
    def __init__(self, resource, credentials):
        """HTTP driver for H5Coro

        Parameters:
            resource (String): HTTP URL pointing to a NASA file
            credentials (String): EDL token
        Returns:
            class HTTPDriver: reader that can access NASA data out of us-west-2
        """
        # Store the credentials for reuse
        self.cached_credentials = credentials

        # construct path to resource
        self.resource = resource
        self.session = requests.Session()

        # Set up the HTTP connection pool
        adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        if type(credentials) is str:
            self.token = credentials
            self.session.headers.update({"Authorization": f"Bearer {self.token}"})

     #######################
    # Copy Constructor
    #######################
    def copy(self):
        """Create and return a copy of the current HTTPDriver instance."""
        new_driver = HTTPDriver(self.resource, self.cached_credentials)
        return new_driver

    #######################
    # read
    #######################
    def read(self, pos, size):
        headers = {"Range": f"bytes={pos}-{pos+size-1}"}

        retries = 2
        delay = 1  # 1-second delay between retries

        for attempt in range(1, retries + 1):
            try:
                stream = self.session.get(self.resource, headers=headers, allow_redirects=True)

                # Success case: return content if status is 200 or 206 (Partial Content)
                if stream.status_code in [200, 206]:
                    return stream.content

                # Log the error if it's not successful
                logger.warning(f"HTTP error {stream.status_code} - Retrying...")

            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt}: Request failed with error: {e}")

            # Delay before retrying if there are remaining attempts
            if attempt < retries:
                time.sleep(delay)

        # All retries failed, raise FatalError
        raise FatalError(f"Failed to read range {pos}-{pos+size-1} after {retries} attempts")
