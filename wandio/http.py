import email
import urllib2
import wandio.file


def http_stat(filename):
    request = urllib2.Request(filename)
    request.get_method = lambda: 'HEAD'
    response = urllib2.urlopen(request)
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
        super(HttpReader, self).__init__(urllib2.urlopen(self.url))
