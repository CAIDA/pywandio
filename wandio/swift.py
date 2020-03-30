import io
import swiftclient
import swiftclient.service
import wandio.file

CHUNK_SIZE = 1 * 1024 * 1024

SEGMENT_SIZE = 1073741824
SEGMENT_CONTAINER_TMPL = ".%s-segments"

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


def list(container=None, options=None, swift=None):
    """
    Get a list of objects in the account or container
    :param container: container to list (if None, the account will be listed)
    :param options:
    :param swift:
    :return:
    """
    if swift is None:
        swift = get_service(options)
    for page in swift.list(container=container):
        if page["success"]:
            for item in page["listing"]:
                yield item["name"]
        else:
            raise page["error"]


def stat(container=None, objects=None, options=None, swift=None):
    """
    Get stats for a list of objects in the account or container
    :param container: container to stat (if None, the account will be listed)
    :param objects: objects to get stats for (if None, the container will be listed)
    :param options:
    :param swift:
    :return:
    """
    if swift is None:
        swift = get_service(options)
    stat_res = swift.stat(container=container, objects=objects)
    if objects is None:
        if not stat_res["success"]:
            raise stat_res["error"]
        yield stat_res
    else:
        for obj_stat in stat_res:
            if not obj_stat["success"]:
                raise obj_stat["error"]
            yield obj_stat


def upload(local_file, container, obj, options=None, swift=None):
    """
    Upload a local file (or file-like object) to the given container and object
    :param local_file: path to local file, or file-like object to upload
    :param container: container to upload to
    :param obj: object name to upload to
    :param options: swift options
    :param swift: existing swift service instance to use
    :return:
    """
    if swift is None:
        swift = get_service(options)
    suo = swiftclient.service.SwiftUploadObject(local_file, object_name=obj)
    results = swift.upload(container, [suo], options={
        "segment_size": SEGMENT_SIZE,
        "segment_container": SEGMENT_CONTAINER_TMPL % container,
    })
    for res in results:
        if not res["success"]:
            # Failing to create a container is a warning, not an error -- Shane
            # Ref: https://github.com/openstack/python-swiftclient/blob/master/swiftclient/shell.py
            # inside function st_upload()
            if 'action' in res and res['action'] == "create_container":
                continue
            else:
                raise res["error"]


def download(container, obj, local_file=None, local_dir=None,
             remove_prefix=True, options=None, swift=None):
    """
    Download an object from swift to the given local file or directory
    :param container: container to download from
    :param obj: object name to download
    :param local_file: path to local file to download to
    :param local_dir: path to local directory to download to
    :param remove_prefix: if local_dir is set, remove the prefix from the object name
    :param options: swift options
    :param swift: existing swift service instance to use
    :return:
    """
    if swift is None:
        swift = get_service(options)
    if local_file is not None:
        opts = {"out_file": local_file}
    elif local_dir is not None:
        opts = {"out_directory": local_dir, "remove_prefix": remove_prefix}
    else:
        raise ValueError("Either localfile or localdir argument must be given")
    results = swift.download(container, [obj], options=opts)
    for res in results:
        if not res["success"]:
            raise res["error"]


def delete(container=None, objects=None, options=None, swift=None):
    """
    Delete a container or a list of objects in the given container
    :param container: container to delete (from)
    :param objects: objects to delete (if None, the container will be deleted)
    :param options:
    :param swift:
    :return:
    """
    if swift is None:
        swift = get_service(options)
    res_iter = swift.delete(container=container, objects=objects)
    for res in res_iter:
        if not res["success"]:
            raise res["error"]


class SwiftReader(wandio.file.GenericReader):

    def __init__(self, url, options=None):
        self.conn = get_connection(options)
        (hdr, body) = self.conn.get_object(resp_chunk_size=CHUNK_SIZE,
                                           **parse_url(url))
        super(SwiftReader, self).__init__(body)

    def close(self):
        if self.conn:
            self.conn.close()


# TODO: figure out how to stream to swift rather than buffer all in memory
class SwiftWriter(wandio.file.GenericWriter):

    def __init__(self, url, options=None):
        parsed_url = parse_url(url)
        self.container = parsed_url["container"]
        self.object = parsed_url["obj"]
        self.options = options
        self.buffer = io.StringIO()
        super(SwiftWriter, self).__init__(self.buffer)

    def flush(self):
        pass

    def close(self):
        self.buffer.reset()
        upload(self.buffer, container=self.container,
               obj=self.object, options=self.options)
