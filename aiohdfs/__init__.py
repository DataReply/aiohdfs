__version__="0.1.2"
import http.client as http_client

from aiohttp import request
from asyncio import coroutine
try:
    from urllib.parse import quote, quote_plus
except ImportError:
    from urllib import quote, quote_plus


from . import errors
from . import operations


class Client(object):
    """
    Client is a Python wrapper for the Hadoop WebHDFS REST API

    To use this client:

    >>> from aiohdfs import Client
    """

    def __init__(self, host='localhost', port='50070', user_name=None):
        """
        Create a new client for interacting with WebHDFS

        :param host: the ip address or hostname of the HDFS namenode
        :param port: the port number for WebHDFS on the namenode
        :param user_name: WebHDFS user.name used for authentication

        >>> hdfs = Client(host='host',port='50070', user_name='hdfs')
        """

        self.host = host
        self.port = port
        self.user_name = user_name

        # create base uri to be used in request operations
        self.base_uri = 'http://{host}:{port}/webhdfs/v1/'.format(
            host=self.host, port=self.port)
    @coroutine
    def create_file(self, path, file_data, **kwargs):
        """
        Creates a new file on HDFS

        :param path: the HDFS file path without a leading '/'
        :param file_data: the initial data to write to the new file

        The function wraps the WebHDFS REST call:

        PUT http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE

        [&overwrite=<true|false>][&blocksize=<LONG>][&replication=<SHORT>]
        [&permission=<OCTAL>][&buffersize=<INT>]

        The function accepts all WebHDFS optional arguments shown above

        Example:

        >>> hdfs = Client(host='host',port='50070', user_name='hdfs')
        >>> my_data = '01010101010101010101010101010101'
        >>> my_file = 'user/hdfs/data/myfile.txt'
        >>> asyncio.get_event_loop().run_until_complete(hdfs.create_file(my_file, my_data))

        Example with optional args:

        >>> asyncio.get_event_loop().run_until_complete(hdfs.create_file(my_file, my_data, overwrite=True, blocksize=64))

        Or for sending data from file like objects:

        >>> with open('file.data') as file_data:
        >>>     asyncio.get_event_loop().run_until_complete(hdfs.create_file(hdfs_path, data=file_data))


        Note: The create_file function does not follow automatic redirects but
        instead uses a two step call to the API as required in the
        WebHDFS documentation
        """

        # make the initial CREATE call to the HDFS namenode
        optional_args = kwargs
        uri = self._create_uri(path, operations.CREATE, **optional_args)

        init_response = yield from request('put', uri, allow_redirects=False)

        if not init_response.status == http_client.TEMPORARY_REDIRECT:
            msg=yield from init_response.content.read()
            _raise_aiohdfs_exception(
                init_response.status, msg)

        # Get the address provided in the location header of the
        # initial response from the namenode and make the CREATE request
        # to the datanode
        uri = init_response.headers['location']
        response = yield from request('put',  uri, data=file_data,
                   headers={'content-type': 'application/octet-stream'})

        if not response.status == http_client.CREATED:
            msg=yield from init_response.content.read()
            _raise_aiohdfs_exception(response.status, msg)

        return True

    @coroutine
    def append_file(self, path, file_data, **kwargs):
        """
        Appends to an existing file on HDFS

        :param path: the HDFS file path without a leading '/'
        :param file_data: data to append to existing file

        The function wraps the WebHDFS REST call:

        POST http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND

        [&buffersize=<INT>]

        The function accepts all WebHDFS optional arguments shown above

        Example:

        >>> hdfs = Client(host='host',port='50070', user_name='hdfs')
        >>> my_data = '01010101010101010101010101010101'
        >>> my_file = 'user/hdfs/data/myfile.txt'
        >>> asyncio.get_event_loop().run_until_complete(hdfs.append_file(my_file, my_data))

        Example with optional args:

        >>> asyncio.get_event_loop().run_until_complete(hdfs.append_file(my_file, my_data, overwrite=True, buffersize=4096))

        Note: The append_file function does not follow automatic redirects but
        instead uses a two step call to the API as required in the
        WebHDFS documentation

        Append is not supported in Hadoop 1.x
        """

        # make the initial APPEND call to the HDFS namenode
        optional_args = kwargs
        uri = self._create_uri(path, operations.APPEND, **optional_args)
        init_response =  yield from request('post', uri, allow_redirects=False)

        if not init_response.status == http_client.TEMPORARY_REDIRECT:
            msg=yield from init_response.content.read()
            _raise_aiohdfs_exception(
                init_response.status,msg)

        # Get the address provided in the location header of the
        # initial response from the namenode and make the APPEND request
        # to the datanode
        uri = init_response.headers['location']
        response =  yield from request('post',  uri, data=file_data,
                   headers={'content-type': 'application/octet-stream'})

        if not response.status == http_client.OK:
            msg=yield from response.content.read()
            _raise_aiohdfs_exception(response.status, msg)

        return True

    @coroutine
    def read_file(self, path, **kwargs):
        """
        Reads from a file on HDFS  and returns the content

        :param path: the HDFS file path without a leading '/'

        The function wraps the WebHDFS REST call:

        GET http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN

        [&offset=<LONG>][&length=<LONG>][&buffersize=<INT>]

        Note: this function follows automatic redirects

        Example:

        >>> hdfs = Client(host='host',port='50070', user_name='hdfs')
        >>> my_file = 'user/hdfs/data/myfile.txt'
        >>> asyncio.get_event_loop().run_until_complete(hdfs.read_file(my_file))
        01010101010101010101010101010101
        01010101010101010101010101010101
        01010101010101010101010101010101
        01010101010101010101010101010101
        """

        optional_args = kwargs
        uri = self._create_uri(path, operations.OPEN, **optional_args)

        response =yield from request('get', uri, allow_redirects=True)

        if not response.status == http_client.OK:
            msg=yield from response.content.read()
            _raise_aiohdfs_exception(response.status, msg)

        return (yield from response.content.read())
    @coroutine
    def make_dir(self, path, **kwargs):
        """
        Create a new directory on HDFS

        :param path: the HDFS file path without a leading '/'

        The function wraps the WebHDFS REST call:

        PUT http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=MKDIRS

        [&permission=<OCTAL>]

        Example:

        >>> hdfs = Client(host='host',port='50070', user_name='hdfs')
        >>> my_dir = 'user/hdfs/data/new_dir'
        >>> asyncio.get_event_loop().run_until_complete(hdfs.make_dir(my_dir))

        Example with optional args:

        >>> hdfs.make_dir(my_dir, permission=755)
        """

        optional_args = kwargs
        uri = self._create_uri(path, operations.MKDIRS, **optional_args)

        response = yield from request('put', uri, allow_redirects=True)

        if not response.status == http_client.OK:
            msg=yield from response.content.read()
            _raise_aiohdfs_exception(response.status, msg)

        return True
    @coroutine
    def rename_file_dir(self, path, destination_path):
        """
        Rename an existing directory or file on HDFS

        :param path: the HDFS file path without a leading '/'
        :param destination_path: the new file path name

        The function wraps the WebHDFS REST call:

        PUT <HOST>:<PORT>/webhdfs/v1/<PATH>?op=RENAME&destination=<PATH>

        Example:

        >>> hdfs = Client(host='host',port='50070', user_name='hdfs')
        >>> current_dir = 'user/hdfs/data/my_dir'
        >>> destination_dir = 'user/hdfs/data/renamed_dir'
        >>> asyncio.get_event_loop().run_until_complete(hdfs.rename_file_dir(current_dir, destination_dir))
        """

        destination_path = '/' + destination_path.lstrip('/')
        uri = self._create_uri(path, operations.RENAME,
                               destination=destination_path)

        response =  yield from request('put', uri, allow_redirects=True)

        if not response.status == http_client.OK:
            msg=yield from response.content.read()
            _raise_aiohdfs_exception(response.status, msg)

        return True
    @coroutine
    def delete_file_dir(self, path, recursive=False):
        """
        Delete an existing file or directory from HDFS

        :param path: the HDFS file path without a leading '/'

        The function wraps the WebHDFS REST call:

        DELETE "http://<host>:<port>/webhdfs/v1/<path>?op=DELETE

        [&recursive=<true|false>]

        Example for deleting a file:

        >>> hdfs = Client(host='host',port='50070', user_name='hdfs')
        >>> my_file = 'user/hdfs/data/myfile.txt'
        >>> asyncio.get_event_loop().run_until_complete(hdfs.delete_file_dir(my_file))

        Example for deleting a directory:

        >>> asyncio.get_event_loop().run_until_complete(hdfs.delete_file_dir(my_file, recursive=True))
        """

        uri = self._create_uri(path, operations.DELETE, recursive=recursive)
        response =  yield from request('delete', uri, allow_redirects=True)

        if not response.status == http_client.OK:
            msg=yield from response.content.read()
            _raise_aiohdfs_exception(response.status,msg)

        return True
    @coroutine
    def get_file_dir_status(self, path):
        """
        Get the file_status of a single file or directory on HDFS

        :param path: the HDFS file path without a leading '/'

        The function wraps the WebHDFS REST call:

        GET http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILESTATUS

        Example for getting file status:

        >>> hdfs = Client(host='host',port='50070', user_name='hdfs')
        >>> my_file = 'user/hdfs/data/myfile.txt'
        >>> asyncio.get_event_loop().run_until_complete(hdfs.get_file_dir_status(my_file))
        {
            "FileStatus":{
                "accessTime":1371737704282,
                "blockSize":134217728,
                "group":"hdfs",
                "length":90,
                "modificationTime":1371737704595,
                "owner":"hdfs",
                "pathSuffix":"",
                "permission":"755",
                "replication":3,
                "type":"FILE"
            }
        }

        Example for getting directory status:

        >>> my_dir = 'user/hdfs/data/'
        >>> hdfs.get_file_dir_status(my_file)
        {
            "FileStatus":{
                "accessTime":0,
                "blockSize":0,
                "group":"hdfs",
                "length":0,
                "modificationTime":1371737704208,
                "owner":"hdfs",
                "pathSuffix":"",
                "permission":"755",
                "replication":0,
                "type":"DIRECTORY"
            }
        }
        """

        uri = self._create_uri(path, operations.GETFILESTATUS)
        response = yield from request('get', uri, allow_redirects=True)

        if not response.status == http_client.OK:
            msg=yield from response.content.read()
            _raise_aiohdfs_exception(response.status,msg)

        return (yield from response.json())
    @coroutine
    def get_file_checksum(self, path):
        """
        Get the file_checksum of a single file on HDFS

        :param path: the HDFS file path without a leading '/'

        The function wraps the WebHDFS REST call:

        GET http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILECHECKSUM

        Example for getting file status:

        >>> hdfs = Client(host='host',port='50070', user_name='hdfs')
        >>> my_file = 'user/hdfs/data/myfile.txt'
        >>> asyncio.get_event_loop().run_until_complete(hdfs.get_file_checksum(my_file))
        {
            "FileChecksum":{
                "algorithm": "MD5-of-1MD5-of-512CRC32",
                "bytes": "000002000000000000000000729a144ad5e9399f70c9bed...",
                "length": 28
            }
        }
        """

        uri = self._create_uri(path, operations.GETFILECHECKSUM)
        response = yield from request('get', uri, allow_redirects=True)

        if not response.status == http_client.OK:
            msg=yield from response.content.read()
            _raise_aiohdfs_exception(response.status, msg)

        return (yield from response.json())
    @coroutine
    def list_dir(self, path):
        """
        Get a list of file_status for all files and directories
        inside an HDFS directory

        :param path: the HDFS file path without a leading '/'

        The function wraps the WebHDFS REST call:

        GET http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS

        Example for listing a directory:

        >>> hdfs = Client(host='host',port='50070', user_name='hdfs')
        >>> my_dir = 'user/hdfs'
        >>> asyncio.get_event_loop().run_until_complete(hdfs.list_dir(my_dir))
        {
            "FileStatuses":{
                "FileStatus":[
                    {
                        "accessTime":1371737704282,
                        "blockSize":134217728,
                        "group":"hdfs",
                        "length":90,
                        "modificationTime":1371737704595,
                        "owner":"hdfs",
                        "pathSuffix":"example3.txt",
                        "permission":"755",
                        "replication":3,
                        "type":"FILE"
                    },
                    {
                        "accessTime":1371678467205,
                        "blockSize":134217728,
                        "group":"hdfs","length":1057,
                        "modificationTime":1371678467394,
                        "owner":"hdfs",
                        "pathSuffix":"example2.txt",
                        "permission":"700",
                        "replication":3,
                        "type":"FILE"
                    }
                ]
            }
        }

        """

        uri = self._create_uri(path, operations.LISTSTATUS)

        response = yield from request('get', uri, allow_redirects=True)
        #response = requests.get(uri, allow_redirects=True)

        if not response.status == http_client.OK:
            msg=yield from response.content.read()
            _raise_aiohdfs_exception(response.status, msg)


        return (yield from response.json())

    def _create_uri(self, path, operation, **kwargs):
        """
        internal function used to construct the WebHDFS request uri based on
        the <PATH>, <OPERATION>, and any provided optional arguments
        """

        path_param = quote(path.encode('utf8'))

        # setup the parameter represent the WebHDFS operation
        operation_param = '?op={operation}'.format(operation=operation)

        # configure authorization based on provided credentials
        auth_param = str()
        if self.user_name:
            auth_param = '&user.name={user_name}'.format(
                user_name=self.user_name)

        # setup any optional parameters
        keyword_params = str()
        for key in kwargs:
            try:
                value = quote_plus(kwargs[key].encode('utf8'))
            except:
                value = str(kwargs[key]).lower()
            keyword_params = '{params}&{key}={value}'.format(
                params=keyword_params, key=key, value=value)

        # build the complete uri from the base uri and all configured params
        uri = '{base_uri}{path}{operation}{keyword_args}{auth}'.format(
            base_uri=self.base_uri, path=path_param,
            operation=operation_param, keyword_args=keyword_params,
            auth=auth_param)

        return uri


def _raise_aiohdfs_exception(resp_code, message=None):

    if resp_code == http_client.BAD_REQUEST:
        raise errors.BadRequest(msg=message)
    elif resp_code == http_client.UNAUTHORIZED:
        raise errors.Unauthorized(msg=message)
    elif resp_code == http_client.NOT_FOUND:
        raise errors.FileNotFound(msg=message)
    elif resp_code == http_client.METHOD_NOT_ALLOWED:
        raise errors.MethodNotAllowed(msg=message)
    else:
        raise errors.AioHdfsException(msg=message)
