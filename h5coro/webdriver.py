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

        # construct path to resource
        self.resource = resource
        self.session = requests.Session()
        if type(credentials) is str:
            self.token = credentials
            self.session.headers.update({"Authorization": f"Bearer {self.token}"})

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

