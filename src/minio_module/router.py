from typing import Optional, List

from fastapi import APIRouter, UploadFile, Query, Path
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
def list_buckets(page_index: Optional[int] = Query(default=1, description='페이지 번호 설정'),
                 page_size: Optional[int] = Query(default=6, description='한 페이지마다 객체 수 설정'
                                                                         '(0 이하 값이면 페이징 처리 X)'),
                 search_keyword: Optional[str] = Query(default=None, description='검색 키워드 설정'),
                 search_column: Optional[str] = Query(default=None, description='속성 검색 설정'),
                 sort: Optional[bool] = Query(default=True, description='True 내림차순, False 오름차순'),
                 sort_column: Optional[str] = Query(default='_creation_date', description='정렬 기준 속성 설정')):
    """
    bucket list를 출력합니다.
    """
    return Response.from_result(MODULE_CODE, service.list_buckets(page_index, page_size, search_keyword,
                                                                  search_column, sort, sort_column))


@router.post("", tags=["bucket"], response_model=Response)
def make_bucket(bucket_info: BucketInfo):
    """
    bucket을 생성합니다.\n
        - object_lock은 객체를 보안하는 설정입니다. (수정, 삭제 등 불가)
    """
    return Response.from_result(MODULE_CODE, service.make_bucket(bucket_info))


@router.get("/{bucket_name}", tags=["bucket"], response_model=Response)
def bucket_exists(bucket_name: str = Path(..., description='bucket의 이름 설정')):
    """
    bucket의 존재 여부를 확인합니다.
    """
    return Response.from_result(MODULE_CODE, service.bucket_exists(bucket_name))


@router.delete("/{bucket_name}", tags=["bucket"], response_model=Response)
def remove_bucket(bucket_name: str = Path(..., description='bucket의 이름 설정')):
    """
    bucket을 삭제합니다.
    """
    return Response.from_result(MODULE_CODE, service.remove_bucket(bucket_name))


@router.post("/{bucket_name}/policy", tags=["bucket"], response_model=Response)
def set_bucket_policy(bucket_name: str = Path(..., description='bucket의 이름 설정')):
    """
    bucket의 접근 제어를 설정합니다.
    """
    return Response.from_result(MODULE_CODE, service.set_bucket_policy(bucket_name))


@router.get("/{bucket_name}/policy", tags=["bucket"], response_model=Response)
def get_bucket_policy(bucket_name: str = Path(..., description='bucket의 이름 설정')):
    """
    bucket의 접근 제어를 조회합니다.
    """
    return Response.from_result(MODULE_CODE, service.get_bucket_policy(bucket_name))


@router.delete("/{bucket_name}/policy", tags=["bucket"], response_model=Response)
def delete_bucket_policy(bucket_name: str = Path(..., description='bucket의 이름 설정')):
    """
    bucket의 접근 제어를 삭제합니다.
    """
    return Response.from_result(MODULE_CODE, service.delete_bucket_policy(bucket_name))


@router.get("/object/{bucket_name}", tags=["object"], response_model=Response)
def list_objects(bucket_name: str = Path(..., description='bucket의 이름 설정'),
                 prefix: Optional[str] = Query(None, description='객체 경로 고정값 설정'),
                 recursive: bool = Query(False, description='True 모든 하위 폴더 및 파일 조회, False 직계 하위 폴더만 조회'),
                 page_index: Optional[int] = Query(default=1, description='페이지 번호 설정'),
                 page_size: Optional[int] = Query(default=10, description='한 페이지마다 객체 수 설정'
                                                                          '(0 이하 값이면 페이징 처리 X)'),
                 search_keyword: Optional[str] = Query(default=None, description='검색 키워드 설정'),
                 search_column: Optional[str] = Query(default=None, description='속성 검색 설정'),
                 sort: Optional[bool] = Query(default=True, description='True 내림차순, False 오름차순'),
                 sort_column: Optional[str] = Query(default='_last_modified', description='정렬 기준 속성 설정')):
    """
    bucket의 하위 폴더 및 파일의 리스트를 조회합니다.
    """
    return Response.from_result(MODULE_CODE,
                                service.list_objects(bucket_name, prefix=prefix, recursive=recursive,
                                                     page_index=page_index, page_size=page_size,
                                                     search_keyword=search_keyword, search_column=search_column,
                                                     sort=sort, sort_column=sort_column))


@router.post("/object/{bucket_name}", tags=["object"], response_model=Response)
def put_object(file: List[UploadFile],
               bucket_name: str = Path(..., description='bucket의 이름 설정'),
               folder_path: str = Query(None, description='파일을 저장할 경로 설정')):
    """
    버킷 안에 객체를 업로드 합니다.\n
        - 버킷 이름을 작성 후 file을 업로드 하면 됩니다.\n
        - forder_path: ex) test1/test2 -> 버킷 안의 test1폴더 -> test2폴더 -> 파일
    """
    return Response.from_result(MODULE_CODE, service.put_objects(bucket_name, file, folder_path))


@router.get("/object/{bucket_name}/stat", tags=["object"], response_model=Response)
def stat_object(bucket_name: str = Path(..., description='bucket의 이름 설정'),
                object_name: str = Query(..., description='파일명(bucket명 제외 나머지 경로 모두 작성) 설정')):
    """
    파일 정보를 조회합니다.
    """
    return Response.from_result(MODULE_CODE, service.stat_object(bucket_name, object_name))


@router.get("/object/{bucket_name}/download", tags=["object"])
def fget_object(bucket_name: str = Path(..., description='bucket의 이름 설정'),
                object_names: List[str] = Query(..., description='다운로드 할 파일명 설정')):
    """
    파일을 다운로드 합니다.\n
        - 여러개의 파일과 폴더는 zip 파일로 다운로드.\n
        - 폴더 다운로드는 '폴더명/' 와 같이 기입\n
        - object_name은 폴더 경로까지 같이 작성
    """
    return service.fget_object(bucket_name, object_names)


@router.post("/object/{bucket_name}/download/url", tags=["object"], response_model=Response)
def presigned_get_object(bucket_name: str = Path(..., description='bucket의 이름 설정'),
                         object_name: str = Query(..., description='다운로드 할 파일명 설정'),
                         expire_days: Optional[int] = Query(7, description='다운로드 링크 기간 설정')):
    """
    파일을 다운 받을 수 있는 url을 제공합니다.\n
        - object_name은 폴더 경로까지 같이 작성\n
        - 사용하지 않는 api로 추후 협의
    """
    return Response.from_result(MODULE_CODE, service.presigned_get_object(bucket_name, object_name,
                                                                          expire_days))


@router.post("/object/{bucket_name}/upload", tags=["object"], response_model=Response)
def fput_object(bucket_name: str = Path(..., description='bucket의 이름 설정'),
                object_name: str = Query(..., description='다운로드 할 파일명 설정'),
                file_path: str = Query(..., description='파일의 경로 설정')):
    """
    파일을 업로드 합니다.\n
        - 사용하지 않는 api로 추후 협의
    """
    return Response.from_result(MODULE_CODE, service.fput_object(bucket_name, object_name, file_path))


@router.delete("/object/{bucket_name}", tags=["object"], response_model=Response)
def remove_object(bucket_name: str = Path(..., description='bucket의 이름 설정'),
                  object_name: List[str] = Query(..., description='삭제할 파일 설정')):
    """
    파일을 삭제합니다.
        - 폴더 삭제는 '폴더명/' 와 같이 기입\n
        - object_name은 폴더 경로까지 같이 작성
    """
    return Response.from_result(MODULE_CODE, service.remove_objects(bucket_name, object_name))


@router.post("/object/serving/{bucket_name}", tags=["object"], response_model=Response)
def put_object_serving(file: UploadFile,
                       bucket_name: str = Path(..., description='bucket의 이름 설정'),
                       model_format: str = Query(..., description='inference service 모델의 프레임워크 설정'),
                       service_name: str = Query(..., description='inference service 이름 설정')):
    """
    모델 파일을 올리면서 inference service 까지 생성을 해줍니다.\n
        - torchserve와 같이 구조가 있는 경우는 zip 파일로 올리면 됩니다.\n
        - inference service와 모델명을 동일하게 가져가야 합니다.
    """
    return Response.from_result(MODULE_CODE, service.put_object_serving(bucket_name, model_format, file, service_name))
