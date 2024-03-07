import json
from typing import Union

from kserve import ApiException

from src.kserve_module.config import MODULE_CODE


class KServeException(Exception):
    def __init__(self, code: int, message: str, result):
        self.code = code
        self.message = message
        self.result = result

    def __str__(self):
        exception_data = {
            "code": self.code,
            "message": self.message,
            "result": self.result
        }
        return json.dumps(exception_data, indent=4, ensure_ascii=False)


class KServeApiError(KServeException):
    def __init__(self, e: Union[ApiException]):
        self.code = 400000
        self.message = 'BAD REQUEST'
        self.result = ['Your request has been denied.']

        if isinstance(e, ApiException):
            self.code = int(f"{MODULE_CODE}{e.status}")
            self.message = e.reason
            self.result = e.body


def parse_response(response_str):
    response_str = response_str[0]
    response_json_str = response_str.split("HTTP response body: ")[-1]
    response_dict = json.loads(response_json_str)

    code = response_dict.get('code')
    message = response_dict.get('message')

    # 딕셔너리로 반환
    return {
        'code': code,
        'message': message
    }
