import json
from typing import Union

from kserve import ApiException

from src.kserve_module.config import MODULE_CODE


class KServeException(Exception):
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


class KServeApiError(KServeException):
    def __init__(self, e: Union[ApiException, RuntimeError]):
        self.code = int(f"{MODULE_CODE}400")
        self.message = 'BAD REQUEST'
        self.result = ['Your request has been denied.']
        #
        if isinstance(e, RuntimeError):
            response_str = e.args[0]
            response_split = response_str.split("\n")
            reason = response_split[1].split("Reason: ")[-1]
            response_body = response_split[3].split("HTTP response body: ")[-1]
            response_dict = json.loads(response_body)

            self.code = int(f"{MODULE_CODE}{response_dict.get('code')}")
            self.message = reason
            self.result = response_dict.get('message')

        if isinstance(e, ApiException):
            self.code = int(f"{MODULE_CODE}{e.status}")
            self.message = e.reason
            self.result = e.body
