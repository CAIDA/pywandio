#!/usr/bin/env python

import argparse
import shutil
import sys
import urlparse

import wandio.compressed
import wandio.file
import wandio.http
import wandio.swift


class Reader(wandio.file.GenericReader):

    def __init__(self, filename):
        self.filename = filename

        # check for the transport types first (HTTP, Swift, Simple)

        # is this Swift (TODO)
        if filename.startswith("swift://"):
            fh = wandio.swift.SwiftReader(self.filename)

        # is this simple HTTP ?
        elif urlparse.urlparse(self.filename).netloc:
            fh = wandio.http.HttpReader(self.filename)

        # then it must be a simple local file
        else:
            fh = wandio.file.SimpleReader(self.filename)

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


def wandio_open(filename, mode="r"):
    if mode == "r":
        return Reader(filename)
    elif mode == "w":
        raise NotImplementedError("PyWandio does not currently support writing")
    else:
        raise ValueError("Invalid mode. Mode must be either 'r' or 'w'")


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
        with Reader(file) as fh:
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
