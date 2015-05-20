
class AioHdfsException(Exception):
    def __init__(self, msg=str()):
        self.msg = msg
        super(AioHdfsException, self).__init__(self.msg)


class BadRequest(AioHdfsException):
    pass


class Unauthorized(AioHdfsException):
    pass


class FileNotFound(AioHdfsException):
    pass


class MethodNotAllowed(AioHdfsException):
    pass
