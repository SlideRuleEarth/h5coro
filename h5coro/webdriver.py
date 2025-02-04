import requests
import logging

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
        stream = self.session.get(self.resource, headers=headers, allow_redirects=True)
        if stream.status_code > 200 and stream.status_code < 400:
            return stream.content
        else:
            print(stream, stream.request.headers)
            raise FatalError

