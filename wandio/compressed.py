import bz2
import zlib
import wandio.file


class CompressedReader(wandio.file.GenericReader):

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