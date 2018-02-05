import urllib2
import wandio.file


class HttpReader(wandio.file.GenericReader):

    def __init__(self, url):
        self.url = url
        super(HttpReader, self).__init__(urllib2.urlopen(self.url))
