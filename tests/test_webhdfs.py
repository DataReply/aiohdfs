from six.moves import http_client
import unittest

from mock import MagicMock
from mock import patch

from aiohdfs import errors
from aiohdfs import Client, _raise_aiohdfs_exception
from aiohdfs import operations


class WhenTestingaiohdfsConstructor(unittest.TestCase):

    def test_init_default_args(self):
        aiohdfs = Client()
        self.assertEqual('localhost', aiohdfs.host)
        self.assertEqual('50070', aiohdfs.port)
        self.assertIsNone(aiohdfs.user_name)

    def test_init_args_provided(self):
        host = '127.0.0.1'
        port = '50075'
        user_name = 'myUser'

        aiohdfs = Client(host=host, port=port, user_name=user_name)
        self.assertEqual(host, aiohdfs.host)
        self.assertEqual(port, aiohdfs.port)
        self.assertEqual(user_name, aiohdfs.user_name)


class WhenTestingCreateOperation(unittest.TestCase):

    def setUp(self):

        self.host = 'hostname'
        self.port = '00000'
        self.user_name = 'username'
        self.aiohdfs = Client(host=self.host, port=self.port,
                                       user_name=self.user_name)
        self.response = MagicMock()
        self.requests = MagicMock(return_value=self.response)
        self.location = 'redirect_uri'
        self.path = 'user/hdfs'
        self.file_data = '010101'
        self.init_response = MagicMock()
        self.init_response.headers = {'location': self.location}
        self.response = MagicMock()
        self.expected_headers = {'content-type': 'application/octet-stream'}

    def test_create_throws_exception_for_no_redirect(self):

        self.init_response.status_code = http_client.BAD_REQUEST
        self.response.status_code = http_client.CREATED
        self.requests.put.side_effect = [self.init_response, self.response]
        with patch('aiohdfs.request', self.requests):
            with self.assertRaises(errors.AioHdfsException):
                self.aiohdfs.create_file(self.path, self.file_data)

    def test_create_throws_exception_for_not_created(self):

        self.init_response.status_code = http_client.TEMPORARY_REDIRECT
        self.response.status_code = http_client.BAD_REQUEST
        self.requests.put.side_effect = [self.init_response, self.response]
        with patch('aiohdfs.request', self.requests):
            with self.assertRaises(errors.AioHdfsException):
                self.aiohdfs.create_file(self.path, self.file_data)

    def test_create_returns_file_location(self):

        self.init_response.status_code = http_client.TEMPORARY_REDIRECT
        self.response.status_code = http_client.CREATED
        self.put_method = MagicMock(
            side_effect=[self.init_response, self.response])
        self.requests.put = self.put_method
        with patch('aiohdfs.request', self.requests):
            result = self.aiohdfs.create_file(self.path, self.file_data)
        self.assertTrue(result)
        self.put_method.assert_called_with(
            self.location, headers=self.expected_headers, data=self.file_data)


class WhenTestingAppendOperation(unittest.TestCase):

    def setUp(self):

        self.host = 'hostname'
        self.port = '00000'
        self.user_name = 'username'
        self.aiohdfs = Client(host=self.host, port=self.port,
                                       user_name=self.user_name)
        self.response = MagicMock()
        self.requests = MagicMock(return_value=self.response)
        self.location = 'redirect_uri'
        self.path = 'user/hdfs'
        self.file_data = '010101'
        self.init_response = MagicMock()
        self.init_response.header = {'location': self.location}
        self.response = MagicMock()

    def test_append_throws_exception_for_no_redirect(self):

        self.init_response.status_code = http_client.BAD_REQUEST
        self.response.status_code = http_client.OK
        self.requests.post.side_effect = [self.init_response, self.response]
        with patch('aiohdfs.request', self.requests):
            with self.assertRaises(errors.AioHdfsException):
                self.aiohdfs.append_file(self.path, self.file_data)

    def test_append_throws_exception_for_not_ok(self):

        self.init_response.status_code = http_client.TEMPORARY_REDIRECT
        self.response.status_code = http_client.BAD_REQUEST
        self.requests.post.side_effect = [self.init_response, self.response]
        with patch('aiohdfs.request', self.requests):
            with self.assertRaises(errors.AioHdfsException):
                self.aiohdfs.append_file(self.path, self.file_data)

    def test_append_returns_true(self):

        self.init_response.status_code = http_client.TEMPORARY_REDIRECT
        self.response.status_code = http_client.OK
        self.requests.post.side_effect = [self.init_response, self.response]
        with patch('aiohdfs.request', self.requests):
            result = self.aiohdfs.append_file(self.path, self.file_data)
        self.assertTrue(result)


class WhenTestingOpenOperation(unittest.TestCase):

    def setUp(self):

        self.host = 'hostname'
        self.port = '00000'
        self.user_name = 'username'
        self.aiohdfs = Client(host=self.host, port=self.port,
                                       user_name=self.user_name)
        self.response = MagicMock()
        self.requests = MagicMock(return_value=self.response)
        self.path = 'user/hdfs'
        self.file_data = u'010101'
        self.response = MagicMock()
        self.response.content = self.file_data

    def test_read_throws_exception_for_not_ok(self):

        self.response.status_code = http_client.BAD_REQUEST
        self.requests.get.return_value = self.response
        with patch('aiohdfs.request', self.requests):
            with self.assertRaises(errors.AioHdfsException):
                self.aiohdfs.read_file(self.path)

    def test_read_returns_file(self):

        self.response.status_code = http_client.OK
        self.requests.get.return_value = self.response
        with patch('aiohdfs.request', self.requests):
            result = self.aiohdfs.read_file(self.path)
        self.assertEqual(result, self.file_data)


class WhenTestingMkdirOperation(unittest.TestCase):

    def setUp(self):

        self.host = 'hostname'
        self.port = '00000'
        self.user_name = 'username'
        self.aiohdfs = Client(host=self.host, port=self.port,
                                       user_name=self.user_name)
        self.response = MagicMock()
        self.requests = MagicMock(return_value=self.response)
        self.path = 'user/hdfs'
        self.response = MagicMock()

    def test_mkdir_throws_exception_for_not_ok(self):

        self.response.status_code = http_client.BAD_REQUEST
        self.requests.put.return_value = self.response
        with patch('aiohdfs.request', self.requests):
            with self.assertRaises(errors.AioHdfsException):
                self.aiohdfs.make_dir(self.path)

    def test_mkdir_returns_true(self):

        self.response.status_code = http_client.OK
        self.requests.put.return_value = self.response
        with patch('aiohdfs.request', self.requests):
            result = self.aiohdfs.make_dir(self.path)
        self.assertTrue(result)


class WhenTestingRenameOperation(unittest.TestCase):

    def setUp(self):

        self.host = 'hostname'
        self.port = '00000'
        self.user_name = 'username'
        self.aiohdfs = Client(host=self.host, port=self.port,
                                       user_name=self.user_name)
        self.response = MagicMock()
        self.requests = MagicMock(return_value=self.response)
        self.path = 'user/hdfs/old_dir'
        self.new_path = '/user/hdfs/new_dir'
        self.response = MagicMock()

    def test_rename_throws_exception_for_not_ok(self):

        self.response.status_code = http_client.BAD_REQUEST
        self.requests.put.return_value = self.response
        with patch('aiohdfs.request', self.requests):
            with self.assertRaises(errors.AioHdfsException):
                self.aiohdfs.rename_file_dir(self.path, self.new_path)

    def test_rename_returns_true(self):

        self.response.status_code = http_client.OK
        self.requests.put.return_value = self.response
        with patch('aiohdfs.request', self.requests):
            result = self.aiohdfs.rename_file_dir(self.path, self.new_path)
        self.assertTrue(result)


class WhenTestingDeleteOperation(unittest.TestCase):

    def setUp(self):

        self.host = 'hostname'
        self.port = '00000'
        self.user_name = 'username'
        self.aiohdfs = Client(host=self.host, port=self.port,
                                       user_name=self.user_name)
        self.response = MagicMock()
        self.requests = MagicMock(return_value=self.response)
        self.path = 'user/hdfs/old_dir'
        self.response = MagicMock()

    def test_rename_throws_exception_for_not_ok(self):

        self.response.status_code = http_client.BAD_REQUEST
        self.requests.delete.return_value = self.response
        with patch('aiohdfs.request', self.requests):
            with self.assertRaises(errors.AioHdfsException):
                self.aiohdfs.delete_file_dir(self.path)

    def test_rename_returns_true(self):

        self.response.status_code = http_client.OK
        self.requests.delete.return_value = self.response
        with patch('aiohdfs.request', self.requests):
            result = self.aiohdfs.delete_file_dir(self.path)
        self.assertTrue(result)


class WhenTestingGetFileStatusOperation(unittest.TestCase):

    def setUp(self):

        self.host = 'hostname'
        self.port = '00000'
        self.user_name = 'username'
        self.aiohdfs = Client(host=self.host, port=self.port,
                                       user_name=self.user_name)
        self.response = MagicMock()
        self.requests = MagicMock(return_value=self.response)
        self.path = 'user/hdfs/old_dir'
        self.response = MagicMock()
        self.file_status = {
            "FileStatus": {
                "accessTime": 0,
                "blockSize": 0,
                "group": "supergroup",
                "length": 0,
                "modificationTime": 1320173277227,
                "owner": "webuser",
                "pathSuffix": "",
                "permission": "777",
                "replication": 0,
                "type": "DIRECTORY"
            }
        }
        self.response.json = MagicMock(return_value=self.file_status)

    def test_get_status_throws_exception_for_not_ok(self):

        self.response.status_code = http_client.BAD_REQUEST
        self.requests.get.return_value = self.response
        with patch('aiohdfs.request', self.requests):
            with self.assertRaises(errors.AioHdfsException):
                self.aiohdfs.get_file_dir_status(self.path)

    def test_get_status_returns_true(self):

        self.response.status_code = http_client.OK
        self.requests.get.return_value = self.response
        with patch('aiohdfs.request', self.requests):
            result = self.aiohdfs.get_file_dir_status(self.path)

        for key in result:
            self.assertEqual(result[key], self.file_status[key])

class WhenTestingGetFileChecksumOperation(unittest.TestCase):

    def setUp(self):

        self.host = 'hostname'
        self.port = '00000'
        self.user_name = 'username'
        self.aiohdfs = Client(host=self.host, port=self.port,
                                       user_name=self.user_name)
        self.response = MagicMock()
        self.requests = MagicMock(return_value=self.response)
        self.path = 'user/hdfs/old_dir'
        self.response = MagicMock()
        self.file_checksum = {
            "FileChecksum": {
                "algorithm": "MD5-of-1MD5-of-512CRC32",
                "bytes": ("000002000000000000000000729a144ad5e9399f70c9bedd757"
                          "2e6bf00000000"),
                "length": 28
            }
        }
        self.response.json = MagicMock(return_value=self.file_checksum)

    def test_get_status_throws_exception_for_not_ok(self):

        self.response.status_code = http_client.BAD_REQUEST
        self.requests.get.return_value = self.response
        with patch('aiohdfs.request', self.requests):
            with self.assertRaises(errors.AioHdfsException):
                self.aiohdfs.get_file_checksum(self.path)

    def test_get_status_returns_true(self):

        self.response.status_code = http_client.OK
        self.requests.get.return_value = self.response
        with patch('aiohdfs.request', self.requests):
            result = self.aiohdfs.get_file_checksum(self.path)

        for key in result:
            self.assertEqual(result[key], self.file_checksum[key])


class WhenTestingListDirOperation(unittest.TestCase):

    def setUp(self):

        self.host = 'hostname'
        self.port = '00000'
        self.user_name = 'username'
        self.aiohdfs = Client(host=self.host, port=self.port,
                                       user_name=self.user_name)
        self.response = MagicMock()
        self.requests = MagicMock(return_value=self.response)
        self.path = 'user/hdfs/old_dir'
        self.response = MagicMock()
        self.file_status = {
            "FileStatuses": {
                "FileStatus": [
                    {
                        "accessTime": 0,
                        "blockSize": 0,
                        "group": "supergroup",
                        "length": 24930,
                        "modificationTime": 1320173277227,
                        "owner": "webuser",
                        "pathSuffix": "a.patch",
                        "permission": "777",
                        "replication": 0,
                        "type": "FILE"
                    },
                    {
                        "accessTime": 0,
                        "blockSize": 0,
                        "group": "supergroup",
                        "length": 0,
                        "modificationTime": 1320173277227,
                        "owner": "webuser",
                        "pathSuffix": "",
                        "permission": "777",
                        "replication": 0,
                        "type": "DIRECTORY"
                    }
                ]
            }
        }
        self.response.json = MagicMock(return_value=self.file_status)

    def test_get_status_throws_exception_for_not_ok(self):

        self.response.status_code = http_client.BAD_REQUEST
        self.requests.get.return_value = self.response
        with patch('aiohdfs.request', self.requests):
            with self.assertRaises(errors.AioHdfsException):
                self.aiohdfs.list_dir(self.path)

    def test_get_status_returns_true(self):

        self.response.status_code = http_client.OK
        self.requests.get.return_value = self.response
        with patch('aiohdfs.request', self.requests):
            result = self.aiohdfs.list_dir(self.path)

        for key in result:
            self.assertEqual(result[key], self.file_status[key])


class WhenTestingCreateUri(unittest.TestCase):

    def setUp(self):
        self.host = 'hostname'
        self.port = '00000'
        self.user_name = 'username'
        self.path = 'user/hdfs'
        self.aiohdfs = Client(host=self.host, port=self.port,
                                       user_name=self.user_name)

    def test_create_uri_no_kwargs(self):
        op = operations.CREATE
        uri = 'http://{host}:{port}/webhdfs/v1/' \
              '{path}?op={op}&user.name={user}'\
            .format(
                host=self.host, port=self.port, path=self.path,
                op=op, user=self.user_name)
        result = self.aiohdfs._create_uri(self.path, op)
        self.assertEqual(uri, result)

    def test_create_uri_with_kwargs(self):
        op = operations.CREATE
        mykey = 'mykey'
        myval = 'myval'
        uri = 'http://{host}:{port}/webhdfs/v1/' \
              '{path}?op={op}&{key}={val}' \
              '&user.name={user}' \
            .format(
                host=self.host, port=self.port, path=self.path,
                op=op, key=mykey, val=myval, user=self.user_name)
        result = self.aiohdfs._create_uri(self.path, op, mykey=myval)
        self.assertEqual(uri, result)

    def test_create_uri_with_unicode_path(self):
        op = operations.CREATE
        mykey = 'mykey'
        myval = 'myval'
        path = u'die/Stra\xdfe'
        quoted_path = 'die/Stra%C3%9Fe'
        uri = 'http://{host}:{port}/webhdfs/v1/' \
              '{path}?op={op}&{key}={val}' \
              '&user.name={user}' \
            .format(
                host=self.host, port=self.port, path=quoted_path,
                op=op, key=mykey, val=myval, user=self.user_name)
        result = self.aiohdfs._create_uri(path, op, mykey=myval)
        self.assertEqual(uri, result)


class WhenTestingRaiseExceptions(unittest.TestCase):

    def test_400_raises_bad_request(self):
        with self.assertRaises(errors.BadRequest):
            _raise_aiohdfs_exception(http_client.BAD_REQUEST)

    def test_401_raises_unuathorized(self):
        with self.assertRaises(errors.Unauthorized):
            _raise_aiohdfs_exception(http_client.UNAUTHORIZED)

    def test_404_raises_not_found(self):
        with self.assertRaises(errors.FileNotFound):
            _raise_aiohdfs_exception(http_client.NOT_FOUND)

    def test_all_other_raises_aiohdfs_exception(self):
        with self.assertRaises(errors.AioHdfsException):
            _raise_aiohdfs_exception(http_client.GATEWAY_TIMEOUT)
