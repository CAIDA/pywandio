import swiftclient
import swiftclient.service
import file


class SwiftReader(file.GenericReader):

    CHUNK_SIZE = 1*1024*1024

    DEFAULT_OPTIONS = {
        "os_auth_url": "https://hermes-auth.caida.org",
        "auth_version": "3",
    }

    def __init__(self, url, options=None):
        self.url = url

        self.options = self._get_options(options)

        self.conn = swiftclient.service.get_conn(self.options)

        url = self.url.replace("swift://", "")
        if url.find("/") == -1:
            raise ValueError("Swift url must be 'swift://container/object'")
        pieces = url.split("/")
        containername = pieces[0]
        objname = "/".join(pieces[1:])
        (hdr, body) = self.conn.get_object(containername, objname,
                                           resp_chunk_size=self.CHUNK_SIZE)
        super(SwiftReader, self).__init__(body)

    def _get_options(self, options):
        # following is borrowed from swiftclient.service.SwiftService
        default_opts = dict(
            swiftclient.service._default_global_options,
            **dict(
                swiftclient.service._default_local_options,
                **self.DEFAULT_OPTIONS
            )
        )
        if options is not None:
            options = dict(
                default_opts,
                **options
            )
        else:
            options = default_opts
        swiftclient.service.process_options(options)

        return options

    def close(self):
        pass
