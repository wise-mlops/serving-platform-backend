import os
import json
import io
import zipfile
from tempfile import TemporaryDirectory
from datetime import timedelta
from typing import Optional

from fastapi import UploadFile
from minio import Minio

from src import app_config
from src.kserve_module.schemas import InferenceServiceInfo
from src.minio_module.exceptions import minio_response
from src.minio_module.schemas import BucketInfo
from src.kserve_module.service import KServeService

my_service = KServeService(app_env=app_config.APP_ENV,
                           config_path=app_config.CLUSTER_KUBE_CONFIG_PATH)


class MinIOService:
    def __init__(self, endpoint, access_key, secret_key):
        endpoint_split = endpoint.split("://")
        self.endpoint = endpoint_split[-1]
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = True if endpoint_split[0] == "https" else False

    def get_client(self):
        return Minio(endpoint=self.endpoint, access_key=self.access_key, secret_key=self.secret_key, secure=self.secure)

    def list_buckets(self, page: Optional[int] = None, search_query: Optional[str] = None):
        client = self.get_client()

        metadata_dicts = client.list_buckets()
        if search_query:
            metadata_dicts = [bucket for bucket in metadata_dicts if search_query.lower() in str(bucket).lower()]

        total_bucket = len(metadata_dicts)

        if page is not None:
            result_details_per_page = 10
            start_index = (page - 1) * result_details_per_page
            end_index = start_index + result_details_per_page
            metadata_dicts = metadata_dicts[start_index:end_index]

        message = {
            "total_result_details": total_bucket,
            "result_details": metadata_dicts,
        }

        result = {
            "message": message
        }
        return minio_response(result)

    def bucket_exists(self, bucket_name: str):
        client = self.get_client()
        return minio_response(client.bucket_exists(bucket_name))

    def make_bucket(self, bucket_info: BucketInfo):
        client = self.get_client()
        available = not client.bucket_exists(bucket_info.bucket_name)
        if available:
            client.make_bucket(bucket_info.bucket_name, object_lock=bucket_info.object_lock)
        return minio_response(available)

    def remove_bucket(self, bucket_name: str):
        client = self.get_client()
        available = client.bucket_exists(bucket_name)
        if available:
            client.remove_bucket(bucket_name)
        return minio_response(available)

    def list_objects(self, bucket_name: str,
                     prefix: Optional[str] = None,
                     recursive: bool = False,
                     search_query: Optional[str] = None,
                     col_query: Optional[str] = None):
        client = self.get_client()
        object_list = [*client.list_objects(bucket_name, prefix=prefix, recursive=recursive)]
        object_list = [obj.__dict__ for obj in object_list]

        if search_query:
            object_list = [item for item in object_list if search_query.lower() in str(item).lower()]

            if col_query:
                object_list = [item for item in object_list if search_query.lower()
                               in str(item[col_query]).lower()]

        return minio_response(object_list)

    def put_object(self, bucket_name: str, upload_file: UploadFile, object_name: str):
        client = self.get_client()
        if client.get_object(bucket_name=bucket_name, object_name=upload_file.filename).status == 200:
            return minio_response('False')
        if object_name is None:
            object_name = upload_file.filename
        file_size = os.fstat(upload_file.file.fileno()).st_size
        client.put_object(bucket_name, object_name=object_name, data=upload_file.file, length=file_size)
        return minio_response(self._get_object_url(bucket_name, object_name, expire_days=7))

    def fget_object(self, bucket_name: str,
                    object_name: str, file_path: str, ):
        client = self.get_client()
        if file_path is None:
            file_path = object_name
        return minio_response(client.fget_object(bucket_name, object_name, file_path))

    def fput_object(self, bucket_name: str,
                    object_name: str, file_path: str):
        client = self.get_client()
        return minio_response(client.fput_object(bucket_name, object_name,
                                                 file_path))

    def stat_object(self, bucket_name: str, object_name: str):
        client = self.get_client()
        return minio_response(client.stat_object(bucket_name=bucket_name, object_name=object_name))

    def remove_object(self, bucket_name: str, object_name: str):
        client = self.get_client()
        client.remove_object(bucket_name, object_name)
        return minio_response("success")

    def _get_object_url(self, bucket_name: str, object_name: str, expire_days: int = 7, object_version_id: str = None):
        client = self.get_client()
        if expire_days > 7 or expire_days < 1:
            expires = timedelta(days=7)
        else:
            expires = timedelta(days=expire_days)
        return minio_response(client.presigned_get_object(bucket_name, object_name, expires=expires,
                                                          version_id=object_version_id))

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
