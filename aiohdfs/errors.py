
class AioHdfsException(Exception):
    def __init__(self, msg=str()):
        self.msg = msg
        super(AioHdfsException, self).__init__(self.msg)

    @classmethod
    def from_response(cls, resp_code, message):
        for subcls in cls.__subclasses__():
            if subcls.resp_code == resp_code:
                return subcls(message)
        else:
            return cls(message)


class BadRequest(AioHdfsException):
    resp_code = 400


class Unauthorized(AioHdfsException):
    resp_code = 401


class FileNotFound(AioHdfsException):
    resp_code = 404


class IO(AioHdfsException):
    resp_code = 403


class MethodNotAllowed(AioHdfsException):
    resp_code = 500
