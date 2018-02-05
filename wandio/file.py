class GenericReader(object):
    """
    Wraps a file-like object
    """

    def __init__(self, fh):
        self.closed = False
        self.fh = fh
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def __iter__(self):
        return self

    def next(self):
        return self.fh.next()

    def read(self, *args):
        return self.fh.read(*args)

    def readline(self):
        return self.fh.readline()

    def close(self):
        self.fh.close()


class SimpleReader(GenericReader):

    def __init__(self, filename):
        super(SimpleReader, self).__init__(open(filename, "r"))
