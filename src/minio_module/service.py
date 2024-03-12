import io
import json
import os
import re
import zipfile
from urllib.parse import quote_plus
from datetime import timedelta
from tempfile import TemporaryDirectory
from typing import Optional, List

import requests
from fastapi import UploadFile
from minio import Minio, S3Error
from starlette.responses import StreamingResponse

from src import app_config
from src.kserve_module.schemas import InferenceServiceInfo
from src.kserve_module.service import KServeService
from src.minio_module.exceptions import minio_response
from src.minio_module.schemas import BucketInfo, convert_datetime_to_str

my_service = KServeService(app_env=app_config.APP_ENV,
                           config_path=app_config.CLUSTER_KUBE_CONFIG_PATH)


class MinIOService:
    def __init__(self, endpoint, access_key, secret_key, download_host=''):
        endpoint_split = endpoint.split("://")
        self.endpoint = endpoint_split[-1]
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = True if endpoint_split[0] == "https" else False
        self.download_host = download_host if download_host != '' else self.endpoint

    def get_client(self):
        return Minio(endpoint=self.endpoint, access_key=self.access_key, secret_key=self.secret_key, secure=self.secure)

    def list_buckets(self, page_index: Optional[int] = None, page_object: Optional[int] = None, paging: bool = True,
                     search_query: Optional[str] = None, col_query: Optional[str] = None,
                     sort_query: Optional[bool] = None, sort_query_col: Optional[str] = None):
        client = self.get_client()
        metadata_dicts = client.list_buckets()
        bucket_list = [obj.__dict__ for obj in metadata_dicts]
        for item in bucket_list:
            item['_creation_date'] = convert_datetime_to_str(item['_creation_date'])

        if search_query:
            bucket_list = [bucket for bucket in bucket_list if search_query.lower() in str(bucket).lower()]

            if col_query:
                bucket_list = [item for item in bucket_list if search_query.lower()
                               in str(item[col_query]).lower()]

        if (sort_query is not None) and sort_query_col:
            bucket_list = sorted(bucket_list, key=lambda x: x[sort_query_col], reverse=sort_query)

        total_bucket = len(bucket_list)

        if paging:
            start_index = (page_index - 1) * page_object
            end_index = start_index + page_object
            bucket_list = bucket_list[start_index:end_index]

        message = {
            "total_result_details": total_bucket,
            "result_details": bucket_list,
        }

        result = {
            "message": message
        }
        return minio_response(result)

    def bucket_exists(self, bucket_name: str):
        client = self.get_client()
        return minio_response(client.bucket_exists(bucket_name))

    @staticmethod
    def validate_bucket_name(bucket_name):
        if len(bucket_name) < 3 or len(bucket_name) > 63:
            raise Exception("Bucket name length should be between 3 and 63 characters.")

        if not bucket_name[0].isalnum() or not bucket_name[-1].isalnum():
            raise Exception("Bucket name should start and end with alphanumeric characters.")

        if not re.match(r'^[a-z0-9.-]+$', bucket_name):
            raise Exception("Bucket name should only contain lowercase letters, numbers, dots, and hyphens.")

        if '..' in bucket_name:
            raise Exception("Bucket name should not contain consecutive dots.")

        return bucket_name

    def make_bucket(self, bucket_info: BucketInfo):
        try:
            bucket_name = self.validate_bucket_name(bucket_info.bucket_name)
            client = self.get_client()
            available = not client.bucket_exists(bucket_name)
            if available:
                client.make_bucket(bucket_name, object_lock=bucket_info.object_lock)
            elif not available:
                available = {
                    "status": available,
                    "message": "It already"
                }
                return minio_response(available, 409)
            return minio_response(available)
        except Exception as e:
            return minio_response(e.args, 400)

    def remove_bucket(self, bucket_name: str):
        try:
            client = self.get_client()
            available = client.bucket_exists(bucket_name)
            if available:
                client.remove_bucket(bucket_name)
            return minio_response(available)
        except S3Error as e:
            return minio_response(e.message, 400)

    def set_bucket_policy(self, bucket_name: str):
        try:
            client = self.get_client()
            available = client.bucket_exists(bucket_name)
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
            if available:
                client.set_bucket_policy(bucket_name, json.dumps(policy))
            return minio_response(available)
        except S3Error as e:
            return minio_response(e.message, 400)

    def get_bucket_policy(self, bucket_name: str):
        try:
            client = self.get_client()
            available = client.bucket_exists(bucket_name)
            if available:
                return minio_response(client.get_bucket_policy(bucket_name))
            return minio_response(available)
        except S3Error as e:
            return minio_response(e.message, 400)

    def delete_bucket_policy(self, bucket_name: str):
        try:
            client = self.get_client()
            available = client.bucket_exists(bucket_name)
            if available:
                client.delete_bucket_policy(bucket_name)
            return minio_response(available)
        except S3Error as e:
            return minio_response(e.message, 400)

    def get_bucket_notification(self, bucket_name: str):
        try:
            client = self.get_client()
            available = client.bucket_exists(bucket_name)
            if available:
                return minio_response(client.get_bucket_notification(bucket_name))
            return minio_response(available)
        except S3Error as e:
            return minio_response(e.message, 400)

    def list_objects(self, bucket_name: str,
                     prefix: Optional[str] = None,
                     recursive: bool = False,
                     page_index: Optional[int] = None,
                     page_object: Optional[int] = None,
                     paging: bool = True,
                     search_query: Optional[str] = None,
                     col_query: Optional[str] = None,
                     sort_query: Optional[bool] = None,
                     sort_query_col: Optional[str] = None):
        client = self.get_client()
        object_list = [*client.list_objects(bucket_name, prefix=prefix, recursive=recursive)]
        object_list = [obj.__dict__ for obj in object_list]

        for item in object_list:
            if item['_last_modified'] is not None:
                item['_last_modified'] = convert_datetime_to_str(item['_last_modified'])
        object_list = [{'_object_name': obj['_object_name'],
                        '_last_modified': obj['_last_modified'],
                        '_size': obj['_size']}
                       for obj in object_list]

        if search_query:
            object_list = [item for item in object_list if search_query.lower() in str(item).lower()]

            if col_query:
                object_list = [item for item in object_list if search_query.lower()
                               in str(item[col_query]).lower()]

        if (sort_query is not None) and sort_query_col:
            object_list = sorted(object_list, key=lambda x: (x[sort_query_col] is None, x[sort_query_col]),
                                 reverse=sort_query)

        total_bucket = len(object_list)

        if paging:
            start_index = (page_index - 1) * page_object
            end_index = start_index + page_object
            object_list = object_list[start_index:end_index]

        message = {
            "total_result_details": total_bucket,
            "result_details": object_list,
        }

        result = {
            "message": message
        }

        return minio_response(result)

    def put_objects(self, bucket_name: str, upload_files: List[UploadFile], folder_path: str):
        client = self.get_client()
        responses = []

        for upload_file in upload_files:
            if folder_path is None:
                object_name = upload_file.filename
            else:
                object_name = folder_path + '/' + upload_file.filename

            try:
                client.get_object(bucket_name, object_name)
                result_error = {
                    "code": 409,
                    "message": "Object Already Exists"
                }
                responses.append(minio_response(result_error, code=409))
            except Exception as e:
                if e:
                    pass
                try:
                    file_size = os.fstat(upload_file.file.fileno()).st_size
                    client.put_object(bucket_name, object_name=object_name, data=upload_file.file, length=file_size)
                    object_url = self._get_object_url(bucket_name, object_name, expire_days=7)
                    responses.append(minio_response(object_url))
                except Exception as e:
                    responses.append(minio_response(e.args, code=400))
        result = {
            "code": 200,
            "message": [response["message"] for response in responses]
        }
        return result

    def _add_folder_to_zip(self, zip_file: zipfile.ZipFile, bucket_name: str, folder_name: str):
        item_list = self.list_objects(bucket_name=bucket_name, prefix=folder_name, recursive=True, paging=False)
        item_list = item_list['message']['message']['result_details']
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
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
            for item in object_names:
                if item.endswith('/'):
                    self._add_folder_to_zip(zip_file, bucket_name, item)
                else:
                    download_url = self._get_object_url(bucket_name, item, expire_days=7)
                    result = requests.get(download_url)
                    file = io.BytesIO(result.content)
                    zip_file.writestr(os.path.basename(item), file.getvalue())

        zip_buffer.seek(0)
        if len(object_names) == 1 and not object_names[0].endswith('/'):
            file_name = object_names[0]
            file_name = quote_plus(file_name.encode('utf-8'))
            download_url = self._get_object_url(bucket_name, file_name, expire_days=7)
            result = requests.get(download_url)
            file_content = result.content
            headers = {
                "Content-Disposition": f"attachment; filename={file_name}",
                "Content-Type": "application/octet-stream",
            }
            return StreamingResponse(io.BytesIO(file_content), media_type="application/octet-stream", headers=headers)
        else:
            headers = {
                "Content-Disposition": "attachment; filename=download.zip",
                "Content-Type": "application/zip",
            }
            return StreamingResponse(zip_buffer, media_type="application/zip", headers=headers)

    def fput_object(self, bucket_name: str,
                    object_name: str, file_path: str):
        client = self.get_client()
        return minio_response(client.fput_object(bucket_name, object_name,
                                                 file_path))

    def stat_object(self, bucket_name: str, object_name: str):
        client = self.get_client()
        return minio_response(client.stat_object(bucket_name=bucket_name, object_name=object_name))

    def remove_objects(self, bucket_name: str, object_names: List[str]):
        client = self.get_client()
        for object_name in object_names:
            objects = client.list_objects(bucket_name, prefix=object_name, recursive=True)
            for obj in objects:
                client.remove_object(bucket_name, obj.object_name)
        return minio_response("success")

    def _get_object_url(self, bucket_name: str, object_name: str, expire_days: int = 7, object_version_id: str = None):
        client = self.get_client()
        if expire_days > 7 or expire_days < 1:
            expires = timedelta(days=7)
        else:
            expires = timedelta(days=expire_days)
        return client.presigned_get_object(bucket_name, object_name, expires=expires, version_id=object_version_id)

    def presigned_get_object(self, bucket_name: str, object_name: str, expire_days: Optional[int] = None,
                             version_id: Optional[str] = None):
        return minio_response(self._get_object_url(bucket_name=bucket_name, object_name=object_name,
                                                   expire_days=expire_days,
                                                   object_version_id=version_id))

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
        return my_service.create_inference_service(InferenceServiceInfo(**json.loads(serving_text)))
