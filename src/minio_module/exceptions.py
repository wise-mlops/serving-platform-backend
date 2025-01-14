import json

from minio.error import MinioException, S3Error
from starlette import status

from src.minio_module.config import MODULE_CODE


class MinIOException(Exception):
    def __init__(self, code: int, message: str, result):
        self.code = int(f"{MODULE_CODE}{code}")
        self.message = message
        self.result = result

    def __str__(self):
        exception_data = {
            "code": self.code,
            "message": self.message,
            "result": self.result
        }
        return json.dumps(exception_data, indent=4, ensure_ascii=False)


class MinIOApiError(MinIOException):
    def __init__(self, e: MinioException):
        self.code = int(f"{MODULE_CODE}{status.HTTP_400_BAD_REQUEST}")
        self.message = "minioException"
        self.result = e.args

        if isinstance(e, S3Error):
            self.code = int(f"{MODULE_CODE}{e.response.status}")
            self.message = e.code
            self.result = e.message
