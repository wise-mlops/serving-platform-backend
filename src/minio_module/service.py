import io
import json
import os
import re
import zipfile
from datetime import timedelta
from tempfile import TemporaryDirectory
from typing import Optional, List, Any
from urllib.parse import quote

import requests
from fastapi import UploadFile
from minio import Minio
from minio.error import MinioException
from starlette.responses import StreamingResponse

from src.kserve_module import service as kserve_service
from src.kserve_module.schemas import InferenceServiceInfo
from src.minio_module.exceptions import MinIOApiError, MinIOException
from src.minio_module.schemas import BucketInfo, convert_datetime_to_str
from src.paging import get_page


class MinIOService:
    def __init__(self, endpoint, access_key, secret_key, download_host=''):
        endpoint_split = endpoint.split("://")
        self.endpoint = endpoint_split[-1]
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = True if endpoint_split[0] == "https" else False
        self.download_host = download_host if download_host != '' else self.endpoint

    def get_client(self):
        try:
            client = Minio(endpoint=self.endpoint, access_key=self.access_key, secret_key=self.secret_key,
                           secure=self.secure)
            return client
        except MinioException as e:
            raise MinIOApiError(e)

    def list_buckets(self, page_index: Optional[int] = None, page_size: Optional[int] = None,
                     search_keyword: Optional[str] = None, search_column: Optional[str] = None,
                     sort: Optional[bool] = None, sort_column: Optional[str] = None):
        client = self.get_client()
        try:
            metadata_dicts = client.list_buckets()
            bucket_list = [obj.__dict__ for obj in metadata_dicts]
            for item in bucket_list:
                item['_creation_date'] = convert_datetime_to_str(item['_creation_date'])

            result = get_page(bucket_list, search_keyword=search_keyword, search_column=search_column, sort=sort,
                              sort_column=sort_column, page_index=page_index, page_size=page_size)
            return result
        except MinioException as e:
            raise MinIOApiError(e)

    def _bucket_exists(self, bucket_name: str):
        client = self.get_client()
        bucket_name = self.validate_bucket_name(bucket_name)
        try:
            result = client.bucket_exists(bucket_name)
            return result
        except MinioException as e:
            raise MinIOApiError(e)
        except ValueError as e:
            raise MinIOException(code=400, message="BAD REQUEST",
                                 result=e.args[0])

    def bucket_exists(self, bucket_name: str):
        exists = self._bucket_exists(bucket_name)
        if not exists:
            raise MinIOException(code=404, message="NOT FOUND", result=f"Bucket [{bucket_name}] does not exist.")
        return exists

    @staticmethod
    def validate_bucket_name(bucket_name):
        if len(bucket_name) < 3 or len(bucket_name) > 63:
            raise MinIOException(code=400, message="BAD REQUEST",
                                 result="Bucket name length should be between 3 and 63 characters.")

        if not bucket_name[0].isalnum() or not bucket_name[-1].isalnum():
            raise MinIOException(code=400, message="BAD REQUEST",
                                 result="Bucket name should start and end with alphanumeric characters.")

        if not re.match(r'^[a-z\d.-]+$', bucket_name):
            raise MinIOException(code=400, message="BAD REQUEST",
                                 result="Bucket name should contain lowercase letters, numbers, dots, and hyphens.")

        if '..' in bucket_name:
            raise MinIOException(code=400, message="BAD REQUEST",
                                 result="Bucket name should not contain consecutive dots.")

        return bucket_name

    def make_bucket(self, bucket_info: BucketInfo):
        available = not self._bucket_exists(bucket_info.bucket_name)
        if not available:
            raise MinIOException(code=409, message="CONFLICT",
                                 result=f"Bucket [{bucket_info.bucket_name}] already exists.")
        client = self.get_client()
        try:
            client.make_bucket(bucket_info.bucket_name, object_lock=bucket_info.object_lock)
            return available
        except MinioException as e:
            raise MinIOApiError(e)

    def remove_bucket(self, bucket_name: str):
        available = self.bucket_exists(bucket_name)
        client = self.get_client()
        try:
            client.remove_bucket(bucket_name)
            return available
        except MinioException as e:
            raise MinIOApiError(e)

    def set_bucket_policy(self, bucket_name: str):
        available = self.bucket_exists(bucket_name)
        client = self.get_client()
        try:
            policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "*"},
                        "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
                        "Resource": "arn:aws:s3:::my-bucket",
                    },
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "*"},
                        "Action": "s3:GetObject",
                        "Resource": "arn:aws:s3:::my-bucket/*",
                    },
                ],
            }
            client.set_bucket_policy(bucket_name, json.dumps(policy))
            return available
        except MinioException as e:
            raise MinIOApiError(e)

    def get_bucket_policy(self, bucket_name: str):
        self.bucket_exists(bucket_name)
        client = self.get_client()
        try:
            result = client.get_bucket_policy(bucket_name)
            return result
        except MinioException as e:
            raise MinIOApiError(e)

    def delete_bucket_policy(self, bucket_name: str):
        available = self.bucket_exists(bucket_name)
        client = self.get_client()
        try:
            client.delete_bucket_policy(bucket_name)
            return available
        except MinioException as e:
            raise MinIOApiError(e)

    def get_bucket_notification(self, bucket_name: str):
        self.bucket_exists(bucket_name)
        client = self.get_client()
        try:
            result = client.get_bucket_notification(bucket_name)
            return result
        except MinioException as e:
            raise MinIOApiError(e)

    def _list_objects(self, bucket_name: str, prefix: str, recursive: bool = False):
        client = self.get_client()
        try:
            result = client.list_objects(bucket_name, prefix=prefix, recursive=recursive)
            return result
        except MinioException as e:
            raise MinIOApiError(e)

    def list_objects(self, bucket_name: str,
                     prefix: Optional[str] = None,
                     recursive: bool = False,
                     page_index: Optional[int] = None,
                     page_size: Optional[int] = 0,
                     search_keyword: Optional[str] = None,
                     search_column: Optional[str] = None,
                     sort: Optional[bool] = None,
                     sort_column: Optional[str] = None):
        available = self.bucket_exists(bucket_name)
        try:
            object_list = [*self._list_objects(bucket_name, prefix=prefix, recursive=recursive)]
            object_list = [obj.__dict__ for obj in object_list]

            for item in object_list:
                if item['_last_modified'] is not None:
                    item['_last_modified'] = convert_datetime_to_str(item['_last_modified'])
            object_list = [{'_object_name': obj['_object_name'],
                            '_last_modified': obj['_last_modified'],
                            '_size': obj['_size']}
                           for obj in object_list]

            result = get_page(object_list, search_keyword=search_keyword, search_column=search_column, sort=sort,
                              sort_column=sort_column, page_index=page_index, page_size=page_size)
            return result
        except MinioException as e:
            raise MinIOApiError(e)

    def put_objects(self, bucket_name: str, upload_files: List[UploadFile], folder_path: str):
        self.bucket_exists(bucket_name)
        client = self.get_client()
        succeeded = []
        failed = []

        for upload_file in upload_files:
            if folder_path is None:
                object_name = upload_file.filename
            else:
                object_name = folder_path + '/' + upload_file.filename

            try:
                file_size = os.fstat(upload_file.file.fileno()).st_size
                client.put_object(bucket_name, object_name=object_name, data=upload_file.file, length=file_size)
                object_url = self._get_object_url(bucket_name, object_name, expire_days=7)
                succeeded.append(object_url)
            except MinioException:
                failed.append(object_name)

        result = {
            "succeeded": succeeded,
            "failed": failed
        }
        return result

    def _add_folder_to_zip(self, zip_file: zipfile.ZipFile, bucket_name: str, folder_name: str):
        item_list = self.list_objects(bucket_name=bucket_name, prefix=folder_name, recursive=True)
        item_list = item_list['result_details']
        for item in item_list:
            object_item = item['_object_name']
            if object_item.endswith('/'):
                self._add_folder_to_zip(zip_file, bucket_name, object_item)
            else:
                download_url = self._get_object_url(bucket_name, object_item, expire_days=7)
                result = requests.get(download_url)
                file = io.BytesIO(result.content)
                zip_file.writestr(os.path.join(folder_name, os.path.relpath(object_item, folder_name)), file.getvalue())

    def fget_object(self, bucket_name: str, object_names: List[str]):
        filename = "download.zip"

        if len(object_names) == 1 and not object_names[0].endswith('/'):
            filename = object_names[0]
            download_url = self._get_object_url(bucket_name, object_names[0], expire_days=7)
            result = requests.get(download_url)
            file_content = io.BytesIO(result.content)
            media_type = "application/octet-stream"
        else:
            file_content = io.BytesIO()
            with zipfile.ZipFile(file_content, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
                for item in object_names:
                    if item.endswith('/'):
                        self._add_folder_to_zip(zip_file, bucket_name, item)
                    else:
                        download_url = self._get_object_url(bucket_name, item, expire_days=7)
                        result = requests.get(download_url)
                        file = io.BytesIO(result.content)
                        zip_file.writestr(os.path.basename(item), file.getvalue())
            file_content.seek(0)
            media_type = "application/zip"
        return self.create_file_response(stream=file_content, media_type=media_type, filename=filename)

    @staticmethod
    def create_file_response(*, stream: io.BytesIO, media_type: str, filename: str, **kwargs: Any) -> StreamingResponse:
        res = StreamingResponse(stream, media_type=media_type, **kwargs)
        res.raw_headers.append((
            b'content-disposition',
            f"attachment; filename*=utf-8''{quote(filename)}".encode('latin-1'),
        ))
        return res

    def fput_object(self, bucket_name: str,
                    object_name: str, file_path: str):
        self.bucket_exists(bucket_name)
        client = self.get_client()
        try:
            result = client.fput_object(bucket_name, object_name, file_path)
            return result
        except MinioException as e:
            raise MinIOApiError(e)

    def stat_object(self, bucket_name: str, object_name: str):
        available = self.bucket_exists(bucket_name)
        client = self.get_client()
        try:
            result = client.stat_object(bucket_name=bucket_name, object_name=object_name)
            return result
        except MinioException as e:
            raise MinIOApiError(e)

    def remove_objects(self, bucket_name: str, object_names: List[str]):
        self.bucket_exists(bucket_name)
        client = self.get_client()
        succeeded = []
        failed = []
        for object_name in object_names:
            objects = self._list_objects(bucket_name, prefix=object_name, recursive=True)
            for obj in objects:
                try:
                    client.remove_object(bucket_name, obj.object_name)
                    succeeded.append(obj.object_name)
                except MinioException:
                    failed.append(obj.object_name)

        result = {
            "succeeded": succeeded,
            "failed": failed
        }

        return result

    def _get_object_url(self, bucket_name: str, object_name: str, expire_days: int = 7, object_version_id: str = None):
        client = self.get_client()
        if expire_days > 7 or expire_days < 1:
            expires = timedelta(days=7)
        else:
            expires = timedelta(days=expire_days)
        return client.presigned_get_object(bucket_name, object_name, expires=expires, version_id=object_version_id)

    def presigned_get_object(self, bucket_name: str, object_name: str, expire_days: Optional[int] = None,
                             version_id: Optional[str] = None):
        try:
            result = self._get_object_url(bucket_name=bucket_name, object_name=object_name,
                                          expire_days=expire_days,
                                          object_version_id=version_id)
            return result
        except MinioException as e:
            raise MinIOApiError(e)

    def put_object_serving(self, bucket_name: str, model_format: str, upload_file: UploadFile,
                           service_name: str):
        client = self.get_client()
        file_size = os.fstat(upload_file.file.fileno()).st_size
        if upload_file.filename.endswith('.zip'):
            with TemporaryDirectory() as tmp_dir:
                zip_path = os.path.join(tmp_dir, upload_file.filename)
                with open(zip_path, 'wb') as tmp_file:
                    tmp_file.write(upload_file.file.read())
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    for filename in zip_ref.namelist():
                        with zip_ref.open(filename) as extracted_file:
                            data = extracted_file.read()
                            client.put_object(bucket_name, object_name=filename, data=io.BytesIO(data),
                                              length=len(data))
        else:
            object_name = f'{service_name}/{upload_file.filename}'
            client.put_object(bucket_name, object_name=object_name, data=upload_file.file,
                              length=file_size)
        create_inference_service = '''
{
  "name": "{{service_name}}",
  "namespace": "kubeflow-user-example-com",
  "inference_service_spec": {
    "predictor": {
      "model_spec": {
        "storage_uri": "s3://{{bucket_name}}/{{service_name}}",
        "protocolVersion": "v2",
        "model_format": {
          "name": "{{model_format}}"
        }
      },
      "service_account_name": "kserve-sa"
    }
  },
  "sidecar_inject": false
}
'''
        serving_text = create_inference_service.replace("{{model_format}}", model_format)
        if upload_file.filename.endswith('.zip'):
            filename_to_use = os.path.splitext(upload_file.filename)[0] if upload_file.filename.endswith(
                '.zip') else upload_file.filename
            serving_text = serving_text.replace("/{{service_name}}", '/' + filename_to_use)
        serving_text = serving_text.replace("{{bucket_name}}", bucket_name)
        serving_text = serving_text.replace("{{service_name}}", service_name)
        return kserve_service.create_inference_service(InferenceServiceInfo(**json.loads(serving_text)))
