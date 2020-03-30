import bz2
import zlib
import wandio.file


class CompressedReader(wandio.file.GenericReader):

    BUFLEN = 4096

    def __init__(self, child_reader, flush_dc):
        self.child_reader = child_reader
        self.flush_dc = flush_dc
        self.dc = None
        self.c_buf = b""
        self.buf = b""
        self.eof = False
        self._refill()
        super(CompressedReader, self).__init__(child_reader)

    def _get_dc(self):
        raise NotImplementedError

    def _refill(self):
        if self.eof:
            return
        # keep reading from the child reader until we've decompressed
        # enough data to fill our buffer
        while len(self.c_buf) or len(self.buf) < self.BUFLEN:
            if not len(self.c_buf):
                # fill our buffer with compressed data
                self.c_buf = self.child_reader.read(self.BUFLEN)
                if len(self.c_buf) < self.BUFLEN:
                    self.eof = True
            # pass the compressed data to the decompressor, and get back
            # some decompressed data
            if self.dc is None:
                self.dc = self._get_dc()
            # feed in the data in c_buf to decompress it
            self.buf += self.dc.decompress(self.c_buf)
            # if we are EOF on the child reader, then flush the decompressor
            if self.eof and self.flush_dc:
                self.buf += self.dc.flush()
            self.c_buf = self.dc.unused_data
            if len(self.c_buf):
                # we have leftover data after compression ended,
                # so now we need a new decompressor
                if self.flush_dc:
                    self.buf += self.dc.flush()
                self.dc = None
            # only heed the eof once we've emptied c_buf
            if not len(self.c_buf) and self.eof:
                break

    def read(self, size=None):
        res = b""
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

    def __next__(self):
        line = self.readline()
        if not line:
            assert(not len(self.buf) and self.eof)
            raise StopIteration
        return line

    def readline(self):
        res = b""
        while not len(res) or not res.endswith(b"\n"):
            idx = self.buf.find(b"\n")
            if idx == -1:
                res += self.buf
                self.buf = b""
                self._refill()
            else:
                res += self.buf[0:idx+1]
                self.buf = self.buf[idx+1:]
            if not len(self.buf) and self.eof:
                break
        if not len(res) and not len(self.buf) and self.eof:
            return None
        return res.decode()


class CompressedWriter(wandio.file.GenericWriter):

    def __init__(self, compressor, child_writer):
        self.compressor = compressor
        self.child_writer = child_writer
        super(CompressedWriter, self).__init__(child_writer)

    def flush(self):
        cd = self.compressor.flush()
        self.fh.write(cd)
        self.fh.flush()

    def write(self, data):
        cd = self.compressor.compress(data)
        # cd is partial compressed data
        self.fh.write(cd)

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def close(self):
        self.flush()
        self.fh.close()


class GzipReader(CompressedReader):

    def __init__(self, child):
        super(GzipReader, self).__init__(child, flush_dc=True)

    def _get_dc(self):
        return zlib.decompressobj(16 + zlib.MAX_WBITS)


class GzipWriter(CompressedWriter):

    def __init__(self, child):
        compressor = zlib.compressobj(-1, zlib.DEFLATED, 16+zlib.MAX_WBITS)
        super(GzipWriter, self).__init__(compressor, child)


class BzipReader(CompressedReader):

    def __init__(self, child):
        super(BzipReader, self).__init__(child, flush_dc=False)

    def _get_dc(self):
        return bz2.BZ2Decompressor()


class BzipWriter(CompressedWriter):

    def __init__(self, child):
        compressor = bz2.BZ2Compressor()
        super(BzipWriter, self).__init__(compressor, child)
