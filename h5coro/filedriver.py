import threading


class FileDriver:

    def __init__(self, resource, credentials):
        self.lock = threading.Lock()
        self.resource = resource
        self.f = open(resource, "rb")

    def read(self, pos, size):
        with self.lock:
            self.f.seek(pos)
            return self.f.read(size)

    def copy(self, max_connections=None):
        return FileDriver(self.resource, None)

    def close(self):
        with self.lock:
            self.f.close()

    def __del__(self):
        self.close()

