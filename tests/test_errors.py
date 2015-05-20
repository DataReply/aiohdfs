import unittest

from aiohdfs import errors


class WhenTestingErrors(unittest.TestCase):
    def setUp(self):
        pass

    def test_aiohdfs_exception_is_exception(self):
        self.assertIsInstance(errors.AioHdfsException(), Exception)
        with self.assertRaises(Exception):
            raise errors.AioHdfsException

    def test_aiohdfs_exception(self):
        msg = 'message'
        ex = errors.AioHdfsException(msg=msg)
        self.assertIs(msg, ex.msg)
