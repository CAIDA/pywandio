#!/usr/bin/env python

import argparse
import bz2
import os
import shutil
import swiftclient
import sys
import urllib2
import urlparse
import zlib


class GenericReader(object):

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


class HttpReader(GenericReader):

    def __init__(self, url):
        self.url = url
        super(HttpReader, self).__init__(urllib2.urlopen(self.url))


class SwiftReader(GenericReader):

    CHUNK_SIZE = 1*1024*1024

    REQ_ENV = [
        "OS_AUTH_URL",
        "OS_USERNAME",
        "OS_PASSWORD",
        "OS_PROJECT_NAME",
        "OS_IDENTITY_API_VERSION",
    ]

    def __init__(self, url):
        self.url = url
        # check that the required environment variables have been set
        for var in self.REQ_ENV:
            if os.environ.get(var) is None:
                raise ValueError("Missing Swift environment variable (%s)"
                                 % var)
        # check that the auth version is correct
        if os.environ.get("OS_IDENTITY_API_VERSION") != "3":
            raise NotImplementedError("FileOpener only supports "
                                      "Swift Auth version 3")

        self.conn = swiftclient.Connection(
            authurl=os.environ.get("OS_AUTH_URL"),
            user=os.environ.get("OS_USERNAME"),
            key=os.environ.get("OS_PASSWORD"),
            tenant_name=os.environ.get("OS_PROJECT_NAME"),
            auth_version=os.environ.get("OS_IDENTITY_API_VERSION")
        )
        url = self.url.replace("swift://", "")
        if url.find("/") == -1:
            raise ValueError("Swift url must be 'swift://container/object'")
        pieces = url.split("/")
        containername = pieces[0]
        objname = "/".join(pieces[1:])
        (hdr, body) = self.conn.get_object(containername, objname,
                                           resp_chunk_size=self.CHUNK_SIZE)
        super(SwiftReader, self).__init__(body)

    def close(self):
        pass


class CompressedReader(GenericReader):

    BUFLEN = 4096

    def __init__(self, decompressor, child_reader):
        self.dc = decompressor
        self.child_reader = child_reader
        self.buf = ""
        self.eof = False
        self._refill()
        super(CompressedReader, self).__init__(child_reader)

    def _refill(self):
        if self.eof:
            return
        while len(self.buf) < self.BUFLEN:
            compressed = self.child_reader.read(self.BUFLEN)
            res = self.dc.decompress(compressed)
            # TODO: use a byte array?
            self.buf += res
            if len(compressed) < self.BUFLEN:
                self.eof = True
                return

    def read(self, size=None):
        res = ""
        while ((not self.eof) or len(self.buf)) and (size is None or len(res) < size):
            if not len(self.buf):
                self._refill()
            toread = size-len(res) if size is not None else len(self.buf)
            res += self.buf[0:toread]
            self.buf = self.buf[toread:]
        # TODO: remove these asserts
        assert(size is None or len(res) <= size)
        assert(size is None or len(res) == size or len(self.buf) == 0)
        return res

    def next(self):
        line = self.readline()
        if not line:
            assert(not len(self.buf) and self.eof)
            raise StopIteration
        return line

    def readline(self):
        res = ""
        while not len(res) or res[-1] != "\n":
            idx = self.buf.find("\n")
            if idx == -1:
                res += self.buf
                self.buf = ""
                self._refill()
            else:
                res += self.buf[0:idx+1]
                self.buf = self.buf[idx+1:]
            if not len(self.buf) and self.eof:
                break
        if not len(res) and not len(self.buf) and self.eof:
            return None
        return res


class GzipReader(CompressedReader):

    def __init__(self, child):
        decompressor = zlib.decompressobj(16+zlib.MAX_WBITS)
        super(GzipReader, self).__init__(decompressor, child)


class BzipReader(CompressedReader):

    def __init__(self, child):
        decompressor = bz2.BZ2Decompressor()
        super(BzipReader, self).__init__(decompressor, child)


class FileOpener(GenericReader):
    """A class to open and read compressed and uncompressed files available
       on the local filesystem or over http. Open the file with read privileges.
        :param filename: file to open
        :return the file handler of the opened file
    """
    def __init__(self, filename):
        self.filename = filename

        # check for the transport types first (HTTP, Swift, Simple)

        # is this Swift (TODO)
        if filename.startswith("swift://"):
            fh = SwiftReader(self.filename)

        # is this simple HTTP ?
        elif urlparse.urlparse(self.filename).netloc:
            fh = HttpReader(self.filename)

        # then it must be a simple local file
        else:
            fh = SimpleReader(self.filename)

        assert fh

        # now check the encoding types (gzip, bzip, plain)

        # Gzip?
        if filename.endswith(".gz"):
            fh = GzipReader(fh)

        # Bzip2?
        elif filename.endswith(".bz2"):
            fh = BzipReader(fh)

        # Plain, leave the transport handle as-is
        else:
            pass

        super(FileOpener, self).__init__(fh)


def wandio_open(filename):
    return FileOpener(filename)


def main():
    parser = argparse.ArgumentParser(description="""
    Reads from a file (or files) and writes its contents to stdout. Supports
    any compression/transport that the dataconcierge.FileOpener supports.
    E.g. HTTP, Swift, gzip, bzip
    """)

    parser.add_argument('-l', '--use-readline', required=False,
                        action='store_true',
                        help="Force use of readline (for testing)")

    parser.add_argument('-n', '--use-next', required=False,
                        action='store_true',
                        help="Force use of next (for testing)")

    parser.add_argument('files', nargs='+', help='Files to read from')

    opts = vars(parser.parse_args())

    for file in opts['files']:
        with FileOpener(file) as fh:
            if opts['use_next']:
                sys.stderr.write("Reading using 'next'\n")
                for line in fh:
                    sys.stdout.write(line)
            elif opts['use_readline']:
                sys.stderr.write("Reading using 'readline'\n")
                line = fh.readline()
                while line:
                    sys.stdout.write(line)
                    line = fh.readline()
            else:
                sys.stderr.write("Reading using 'shutil'\n")
                shutil.copyfileobj(fh, sys.stdout)
