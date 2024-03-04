from typing import Optional

from fastapi import APIRouter, UploadFile
from fastapi.responses import JSONResponse

from src.minio_module import service
from src.minio_module.config import MODULE_CODE
from src.minio_module.schemas import BucketInfo
from src.response import Response

router = APIRouter(
    prefix="/bucket",
    responses={404: {"description": "Not found"}},
    default_response_class=JSONResponse,
)


@router.get("", tags=["bucket"], response_model=Response)
async def list_buckets(page: Optional[int] = None, search_query: Optional[str] = None, col_query: Optional[str] = None):
    return Response.from_result(MODULE_CODE, service.list_buckets(page, search_query, col_query))


@router.post("", tags=["bucket"], response_model=Response)
async def make_bucket(bucket_info: BucketInfo):
    return Response.from_result(MODULE_CODE, service.make_bucket(bucket_info))


@router.get("/{bucket_name}", tags=["bucket"], response_model=Response)
async def bucket_exists(bucket_name: str):
    return Response.from_result(MODULE_CODE, service.bucket_exists(bucket_name))


@router.delete("/{bucket_name}", tags=["bucket"], response_model=Response)
async def remove_bucket(bucket_name: str):
    return Response.from_result(MODULE_CODE, service.remove_bucket(bucket_name))


@router.get("/object/{bucket_name}", tags=["object"], response_model=Response)
async def list_objects(bucket_name: str,
                       prefix: Optional[str] = None,
                       recursive: bool = False,
                       page: Optional[int] = None,
                       search_query: Optional[str] = None,
                       col_query: Optional[str] = None):
    return Response.from_result(MODULE_CODE,
                                service.list_objects(bucket_name, prefix=prefix, recursive=recursive, page=page,
                                                     search_query=search_query, col_query=col_query))


@router.post("/object/{bucket_name}", tags=["object"], response_model=Response)
def put_object(bucket_name: str, file: UploadFile, object_name: str = None):
    return Response.from_result(MODULE_CODE, service.put_object(bucket_name, file, object_name))


@router.get("/object/{bucket_name}/stat", tags=["object"], response_model=Response)
def stat_object(bucket_name: str,
                object_name: str):
    return Response.from_result(MODULE_CODE, service.stat_object(bucket_name, object_name))


@router.get("/object/{bucket_name}/download", tags=["object"], response_model=Response)
def fget_object(bucket_name: str, object_name: str, file_path: Optional[str] = None):
    return Response.from_result(MODULE_CODE, service.fget_object(bucket_name, object_name, file_path))


@router.post("/object/{bucket_name}/download/url", tags=["object"], response_model=Response)
def presigned_get_object(bucket_name: str, object_name: str, expire_days: Optional[str] = None):
    return Response.from_result(MODULE_CODE, service.presigned_get_object(bucket_name, object_name,
                                                                          expire_days))


@router.post("/object/{bucket_name}/upload", tags=["object"], response_model=Response)
def fput_object(bucket_name: str,
                object_name: str, file_path: str):
    return Response.from_result(MODULE_CODE, service.fput_object(bucket_name, object_name, file_path))


@router.delete("/object/{bucket_name}", tags=["object"], response_model=Response)
def remove_object(bucket_name: str,
                  object_name: str):
    return Response.from_result(MODULE_CODE, service.remove_object(bucket_name, object_name))


@router.post("/object/serving/{bucket_name}", tags=["object"], response_model=Response)
def put_object_serving(bucket_name: str, model_format: str, file: UploadFile, service_name: str):
    return Response.from_result(MODULE_CODE, service.put_object_serving(bucket_name, model_format, file, service_name))
