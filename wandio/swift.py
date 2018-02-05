import os
import swiftclient
import file


class SwiftReader(file.GenericReader):

    CHUNK_SIZE = 1*1024*1024

    REQ_ENV = [
        "OS_AUTH_URL",
        "OS_USERNAME",
        "OS_PASSWORD",
        "OS_PROJECT_NAME",
        "OS_IDENTITY_API_VERSION",
    ]

    def __init__(self, url):
        self.url = url
        # check that the required environment variables have been set
        for var in self.REQ_ENV:
            if os.environ.get(var) is None:
                raise ValueError("Missing Swift environment variable (%s)"
                                 % var)
        # check that the auth version is correct
        if os.environ.get("OS_IDENTITY_API_VERSION") != "3":
            raise NotImplementedError("FileOpener only supports "
                                      "Swift Auth version 3")

        self.conn = swiftclient.Connection(
            authurl=os.environ.get("OS_AUTH_URL"),
            user=os.environ.get("OS_USERNAME"),
            key=os.environ.get("OS_PASSWORD"),
            tenant_name=os.environ.get("OS_PROJECT_NAME"),
            auth_version=os.environ.get("OS_IDENTITY_API_VERSION")
        )
        url = self.url.replace("swift://", "")
        if url.find("/") == -1:
            raise ValueError("Swift url must be 'swift://container/object'")
        pieces = url.split("/")
        containername = pieces[0]
        objname = "/".join(pieces[1:])
        (hdr, body) = self.conn.get_object(containername, objname,
                                           resp_chunk_size=self.CHUNK_SIZE)
        super(SwiftReader, self).__init__(body)

    def close(self):
        pass
