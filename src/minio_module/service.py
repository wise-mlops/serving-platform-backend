import os
from datetime import timedelta
from typing import Optional

from fastapi import UploadFile
from minio import Minio

from src.minio_module.exceptions import minio_response
from src.minio_module.schemas import BucketInfo, ObjectInfo


class MinIOService:
    def __init__(self, endpoint, access_key, secret_key):
        endpoint_split = endpoint.split("://")
        self.endpoint = endpoint_split[-1]
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = True if endpoint_split[0] == "https" else False

    def get_client(self):
        return Minio(endpoint=self.endpoint, access_key=self.access_key, secret_key=self.secret_key, secure=self.secure)

    def list_buckets(self):
        client = self.get_client()
        return minio_response(client.list_buckets())

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
                     start_after: Optional[str] = None):
        client = self.get_client()
        return minio_response([*client.list_objects(bucket_name, prefix=prefix, recursive=recursive,
                                                    start_after=start_after)])

    def put_object(self, bucket_name: str, upload_file: UploadFile, object_name: str):
        client = self.get_client()
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
        return minio_response(minio_response(client.fput_object(bucket_name, object_name,
                                                                file_path)))

    def stat_object(self, bucket_name: str, object_name: str,
                    version_id: str):
        client = self.get_client()
        return minio_response(client.stat_object(bucket_name=bucket_name, object_name=object_name,
                                                 version_id=version_id))

    def remove_object(self, bucket_name: str, version_id: str,
                      object_name: str):
        client = self.get_client()
        client.remove_object(bucket_name, object_name, version_id=version_id)
        return minio_response("success")

    def _get_object_url(self, bucket_name: str, object_name: str, expire_days: int = 7, object_version_id: str = None):
        client = self.get_client()
        if expire_days > 7 or expire_days < 1:
            expires = timedelta(days=7)
        else:
            expires = timedelta(days=expire_days)
        return minio_response(client.presigned_get_object(bucket_name, object_name, expires=expires,
                                                          version_id=object_version_id))

    def presigned_get_object(self, bucket_name: str, object_name: str, expire_days: Optional[str] = None,
                             version_id: Optional[str] = None):
        return minio_response(self._get_object_url(bucket_name=bucket_name, object_name=object_name,
                                                   expire_days=expire_days,
                                                   object_version_id=version_id))
