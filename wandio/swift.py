import swiftclient
import swiftclient.service
import file

CHUNK_SIZE = 1 * 1024 * 1024

DEFAULT_OPTIONS = {
    "os_auth_url": "https://hermes-auth.caida.org",
    "auth_version": "3",
}


def process_options(options=None):
    # following is borrowed from swiftclient.service.SwiftService
    default_opts = dict(
        swiftclient.service._default_global_options,
        **dict(
            swiftclient.service._default_local_options,
            **DEFAULT_OPTIONS
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


def get_auth(options=None, connection=None):
    """
    Get the auth URL and auth token for the given options or connection
    :param options:
    :param connection:
    :return:
    """
    if connection is None:
        connection = get_connection(options)
    return connection.get_auth()


def get_service(options=None):
    """
    Get a ready-to-use SwiftService instance
    :param options:
    :return:
    """
    options = process_options(options)
    return swiftclient.service.SwiftService(options=options)


def get_connection(options=None):
    """
    Get a swift Connection instance
    :param options:
    :return:
    """
    options = process_options(options)
    return swiftclient.service.get_conn(options)


def parse_url(url):
    """
    Parse a 'swift://CONTAINER/OBJECT' style URL
    :param url:
    :return: dictionary with "container" and "obj" keys
    """
    url = url.replace("swift://", "")
    if url.find("/") == -1:
        raise ValueError("Swift url must be 'swift://container/object'")
    pieces = url.split("/")
    containername = pieces[0]
    objname = "/".join(pieces[1:])
    return {
        "container": containername,
        "obj": objname,
    }


def list(container=None, options=None):
    """
    Get a list of objects in the account or container
    :param container: container to list (if None, the account will be listed)
    :param options:
    :return:
    """
    swift = get_service(options)
    for page in swift.list(container=container):
        if page["success"]:
            for item in page["listing"]:
                yield item["name"]
        else:
            raise page["error"]


class SwiftReader(file.GenericReader):

    def __init__(self, url, options=None):
        self.conn = get_connection(options)
        (hdr, body) = self.conn.get_object(resp_chunk_size=CHUNK_SIZE,
                                           **parse_url(url))
        super(SwiftReader, self).__init__(body)

    def close(self):
        pass
