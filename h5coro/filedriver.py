
class FileDriver:

    def __init__(self, resource, credentials):
        self.resource = resource
        self.f = open(resource, "rb")

    def read(self, pos, size):
        self.f.seek(pos)
        return self.f.read(size)

    def copy(self):
        return FileDriver(self.resource, None)

