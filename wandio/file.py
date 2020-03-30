import os
import stat
import sys


def file_stat(filename):
    sr = os.stat(filename)
    return {
        "size": sr[stat.ST_SIZE],
        "mtime": sr[stat.ST_MTIME],
    }


class GenericReader(object):
    """
    Wraps a file-like object
    """

    def __init__(self, fh):
        self.fh = fh

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.fh)

    def next(self):
        return self.__next__()

    def read(self, *args):
        return self.fh.read(*args)

    def readline(self):
        return self.fh.readline()

    def close(self):
        self.fh.close()


class GenericWriter(object):
    """
    Wraps a file-like writer object
    """

    def __init__(self, fh):
        self.fh = fh

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def flush(self):
        self.fh.flush()

    def write(self, data):
        self.fh.write(data)

    def writelines(self, lines):
        self.fh.writelines(lines)

    def close(self):
        self.fh.close()


class StdinReader(GenericReader):

    def __init__(self):
        super(StdinReader, self).__init__(sys.stdin)


class SimpleReader(GenericReader):

    def __init__(self, filename):
        super(SimpleReader, self).__init__(open(filename, "r"))


class SimpleWriter(GenericWriter):

    def __init__(self, filename):
        super(SimpleWriter, self).__init__(open(filename, "w"))
