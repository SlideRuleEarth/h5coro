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
        if type(credentials) is str:
            self.session = requests.Session(headers={"Authorization": f"Bearer: {credentials}"})
        else:
            self.session = requests.Session()

    #######################
    # read
    #######################
    def read(self, pos, size):
        headers = {"Range": f"bytes={pos}-{pos+size-1}"}
        stream = self.session.get(self.resource, headers=headers, allow_redirects=True)
        if stream.status_code > 200 and stream.status_code < 400:
            return stream.content
        else:
            raise FatalError

