import sys

import email
import wandio.file

# urllib import compatible with both python2 and python3
# https://python-future.org/compatible_idioms.html#urllib-module
try:
    from urllib.request import urlopen, Request
except ImportError:
    from urllib2 import urlopen, Request


def http_stat(filename):
    request = Request(filename)
    request.get_method = lambda: 'HEAD'
    response = urlopen(request)
    hdrs = response.info()

    # Last Modified time
    mtime = None
    if "Last-Modified" in hdrs:
        mtime = hdrs["Last-Modified"]
        mtime = email.utils.parsedate_tz(mtime)
        mtime = email.utils.mktime_tz(mtime)

    # Content Length
    size = None
    if "Content-Length" in hdrs:
        size = int(hdrs["Content-Length"])

    return {
        "mtime": mtime,
        "size": size,
    }


class HttpReader(wandio.file.GenericReader):

    def __init__(self, url):
        self.url = url
        super(HttpReader, self).__init__(urlopen(self.url))

    def __next__(self):
        return next(self.fh).decode()
