#!/usr/bin/env python

import argparse
import os, sys, errno
import shutil
import socket

# urllib import compatible with both python2 and python3
# https://python-future.org/compatible_idioms.html#urllib-module
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

import wandio.compressed
import wandio.file
import wandio.wand_http
import wandio.swift


class Reader(wandio.file.GenericReader):

    def __init__(self, filename, mode="r", options=None):
        self.filename = filename

        # check for the transport types first (HTTP, Swift, Simple)

        # is this Swift
        if filename.startswith("swift://"):
            fh = wandio.swift.SwiftReader(self.filename, options=options)

        # is this simple HTTP ?
        elif urlparse(self.filename).netloc:
            fh = wandio.wand_http.HttpReader(self.filename)

        # stdin?
        elif filename == "-":
            fh = wandio.file.StdinReader()

        # then it must be a simple local file
        else:
            # default reading as binary mode for .gz and .bz2 files
            if filename.endswith(".gz") or filename.endswith(".bz2"):
                mode = "rb"
            fh = wandio.file.SimpleReader(self.filename, mode=mode)

        assert fh

        # now check the encoding types (gzip, bzip, plain)

        # Gzip?
        if filename.endswith(".gz"):
            fh = wandio.compressed.GzipReader(fh)

        # Bzip2?
        elif filename.endswith(".bz2"):
            fh = wandio.compressed.BzipReader(fh)

        # Plain, leave the transport handle as-is
        else:
            pass

        super(Reader, self).__init__(fh)


# TODO: refactor Reader and Writer
class Writer(wandio.file.GenericWriter):

    def __init__(self, filename, mode="w", options=None):
        self.filename = filename

        is_binary_file = True if filename.endswith(".gz") or filename.endswith(".bz2") else False

        # check for the transport types first (HTTP, Swift, Simple)
        # is this Swift
        if filename.startswith("swift://"):
            fh = wandio.swift.SwiftWriter(self.filename, options=options, use_bytes_io=is_binary_file)

        # is this simple HTTP ?
        elif urlparse(self.filename).netloc:
            raise NotImplementedError("Writing to HTTP is not supported")

        # then it must be a simple local file
        else:
            if is_binary_file:
                mode = "wb"
            fh = wandio.file.SimpleWriter(self.filename, mode=mode)

        assert fh

        # now check the encoding types (gzip, bzip, plain)

        # Gzip?
        if filename.endswith(".gz"):
            fh = wandio.compressed.GzipWriter(fh)

        # Bzip2?
        elif filename.endswith(".bz2"):
            fh = wandio.compressed.BzipWriter(fh)

        # Plain, leave the transport handle as-is
        else:
            pass

        super(Writer, self).__init__(fh)


def wandio_open(filename, mode="r", options=None):
    if mode in ["r", "rb"]:
        return Reader(filename, mode, options)
    elif mode in ["w", "wb"]:
        return Writer(filename, mode, options)
    else:
        raise ValueError("Invalid mode. Mode must be either 'r'/'rb' or 'w'/'wb'")


def wandio_stat(filename):
    # currently we support a *very* limited set stat fields:
    # - mtime (Last-Modified for HTTP)
    # - size
    # is this Swift
    if filename.startswith("swift://"):
        # TODO
        raise NotImplementedError("Stat not yet supported for Swift files")

    # is this simple HTTP ?
    elif urlparse(filename).netloc:
        statfunc = wandio.wand_http.http_stat

    # stdin?
    elif filename == "-":
        raise NotImplementedError("Cannot perform stat operation on STDIN")

    # then it must be a simple local file
    else:
        statfunc = wandio.file.file_stat

    return statfunc(filename)


def read_main():
    parser = argparse.ArgumentParser(description="""
    Reads from a file (or files) and writes its contents to stdout. Supports
    any compression/transport that pywandio supports. E.g. HTTP, Swift, gzip, bzip
    """)

    parser.add_argument('-l', '--use-readline', required=False,
                        action='store_true',
                        help="Force use of readline (for testing)")

    parser.add_argument('-n', '--use-next', required=False,
                        action='store_true',
                        help="Force use of next (for testing)")

    parser.add_argument('files', nargs='+', help='Files to read from')
    parser.add_argument('-m', '--file-mode', required=False,
                        type=str, default='r',
                        help="Open files using this file mode")

    opts = vars(parser.parse_args())

    for filename in opts['files']:
        with Reader(filename, mode=opts['file_mode']) as fh:
            if opts['use_next']:
                # sys.stderr.write("Reading using 'next'\n")
                for line in fh:
                    sys.stdout.write(line)
            elif opts['use_readline']:
                # sys.stderr.write("Reading using 'readline'\n")
                line = fh.readline()
                while line:
                    sys.stdout.write(line)
                    line = fh.readline()
            else:
                # sys.stderr.write("Reading using 'shutil'\n")
                if (sys.version_info > (3, 0)):
                    try:
                        shutil.copyfileobj(fh, sys.stdout.buffer)
                    except BrokenPipeError:
                        devnull = os.open(os.devnull, os.O_WRONLY)
                        os.dup2(devnull, sys.stdout.fileno())
                        sys.exit(1)
                else:
                    try:
                        shutil.copyfileobj(fh, sys.stdout)
                    except IOError as e:
                        if e.errno != errno.EPIPE:
                            # if it is not a BrokenPipeError, raise the error.
                            raise e
                        devnull = os.open(os.devnull, os.O_WRONLY)
                        os.dup2(devnull, sys.stdout.fileno())
                        sys.exit(1)

def write_main():
    parser = argparse.ArgumentParser(description="""
    Reads from stdin and writes to a file. Supports any compression/transport
    that pywandio supports. E.g. HTTP, Swift, gzip, bzip
    """)

    parser.add_argument('file', help='File to write to')

    opts = vars(parser.parse_args())

    with Writer(opts["file"]) as out_fh:
        with Reader("-") as in_fh:
            for line in in_fh:
                out_fh.write(line)


def stat_main():
    parser = argparse.ArgumentParser(description="""
    Obtains and prints statistics about the given file (either local or remote)
    """)

    parser.add_argument('file', help='File to obtain statistics for')

    opts = vars(parser.parse_args())

    s = wandio_stat(opts["file"])
    print("mtime: %s" % s["mtime"])
    print("size: %s" % s["size"])
