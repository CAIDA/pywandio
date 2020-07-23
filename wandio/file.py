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
        self.buf_lines = []
        self.buf_lastline = ""
        self.ended = False

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def __iter__(self):
        return self

    def next(self):
        """
        Extended `next()` processing to handle cases where the file-handler does not break strings by line breaks.
        """

        # if in previous iteration, we have reached the end of file, but we forcibly returned the cached last line,
        # we will end the iteration here manually.
        # this only happens if file contains strings and the last line does not have line break.
        if self.ended:
            raise StopIteration

        # if in previous step, we have cached multiple lines, pop the first line and return
        if self.buf_lines:
            return self.buf_lines.pop(0)

        # we do not have cached lines, do a reading
        try:
            res = self.fh.next()
        except StopIteration as ex:
            # reached the end of file but still have unreturned last line, return the line
            # also mark the iteration as ended, and it will return in the next run
            if self.buf_lastline:
                self.ended = True
                return self.buf_lastline
            raise ex

        # we have some content from the file handle
        if isinstance(res, str):
            # if it is a string, it is possible that the string is not line-separated.
            # if the string contains linebreaks, we do the linebreak here and then cached the lines.
            # the cached lines will be returned one at a time in the follow-up iterations.

            # split lines by linebreaks, keep the linebreak symbols (os agnostic)
            self.buf_lines = res.splitlines(True)

            if self.buf_lines[-1].rstrip() == self.buf_lines[-1]:
                # no newline for the last line, it is possible that the read stopped in the middle of a line.
                # to be safe, cache the last line separately, and pop the last line from the lines cache.
                self.buf_lastline = self.buf_lines.pop()
            elif self.buf_lastline:
                # if in the previous runs, we have cached the last line, and know we are in a new read,
                # that means the previous last line and the current first line is acutally one single line,
                # we extend the first line with the previous last line and clear the last line cache.
                self.buf_lines[0] = self.buf_lastline + self.buf_lines[0]
                self.buf_lastline = ""

            # now we have a cached lines, each with linebreak at the end, we return the first line in cache.
            return self.buf_lines.pop(0)
        else:
            # the read content is not a string, e.g. a byte array, we simply return what was read.
            return res


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
