from typing import Optional

from fastapi import APIRouter, Query, Path, Body
from fastapi.responses import JSONResponse

from src.kserve_module import service
from src.kserve_module.config import MODULE_CODE
from src.kserve_module.schemas import InferenceServiceInfo
from src.response import Response

router = APIRouter(
    prefix="/kserve",
    tags=["kserve"],
    responses={404: {"description": "Not found"}},
    default_response_class=JSONResponse,
)


@router.post("", response_model=Response)
async def create_inference_service(inference_service_info: InferenceServiceInfo):
    """
    inference service 만들기\n
        - model format 확인\n
        - inference service 이름 설정\n
            - 이름은 영어 소문자로 시작하고 끝나며, 그 안에는 소문자, 숫자, -만 허용됩니다.
        - minio 경로 설정
    """
    return Response.from_result(MODULE_CODE,
                                service.create_inference_service(inference_service_info))


@router.patch("", response_model=Response)
async def patch_inference_service(inference_service_info: InferenceServiceInfo):
    """
    inference service 수정\n
    추후 협의
    """
    return Response.from_result(MODULE_CODE,
                                service.patch_inference_service(inference_service_info))


@router.put("", response_model=Response)
async def replace_inference_service(inference_service_info: InferenceServiceInfo):
    """
    inference service 수정\n
    추후 협의
    """
    return Response.from_result(MODULE_CODE,
                                service.replace_inference_service(inference_service_info))


@router.delete("/{name}", response_model=Response)
async def delete_inference_service(name: str = Path(..., description='inference service명 설정')):
    """
    특정 inference service 제거
    """
    return Response.from_result(MODULE_CODE, service.delete_inference_service(name))


@router.get("/{name}", response_model=Response)
async def get_inference_service(name: str = Path(..., description='inference service명 설정')):
    """
    특정 inference service의 모든 정보를 받아 볼 수 있습니다.
    """
    return Response.from_result(MODULE_CODE, service.get_inference_service(name))


@router.get("", response_model=Response)
async def get_inference_service_list(page_index: Optional[int] = Query(default=1, description='페이지 번호 설정'),
                                     page_size: Optional[int] = Query(default=10, description='한 페이지마다 객체 수 설정'
                                                                                              '(0 이하 값이면 페이징 처리 X)'),
                                     search_keyword: Optional[str] = Query(default=None, description='검색 키워드 설정'),
                                     search_column: Optional[str] = Query(default=None, description='속성 검색 설정'),
                                     sort: Optional[bool] = Query(default=True, description='True 내림차순, False 오름차순'),
                                     sort_column: Optional[str] = Query(default='creationTimestamp',
                                                                        description='정렬 기준 속성 설정')):
    """
    inference service list를 출력합니다.
    """
    return Response.from_result(MODULE_CODE,
                                service.get_inference_service_list(page_index, page_size, search_keyword,
                                                                   search_column, sort, sort_column))


@router.post("/{namespace}/{name}/infer", response_model=Response)
async def infer_model(name: str = Path(..., description='inference service의 모델명 설정'),
                      model_format: str = Query(..., description='inference service 모델의 프레임워크 설정'),
                      data: list = Body(..., description='테스트 포맷에 맞게 input값을 설정')):
    """
    inference service를 통해 모델을 테스트 해볼 수 있습니다.\n
        - input값은 각 포맷에 맞게 입력시 output을 받아볼 수 있습니다.
    """
    return Response.from_result(MODULE_CODE, service.infer_model(name, model_format, data))


@router.get("/detail/{name}", response_model=Response)
async def get_inference_service_parse_detail(name: str = Path(..., description='inference service명 설정')):
    """
    특정 inference service의 필요한 정보를 받아 볼 수 있습니다.
    """
    return Response.from_result(MODULE_CODE, service.get_inference_service_parse_detail(name))


@router.get("/stat/{name}", response_model=Response)
async def get_inference_service_stat(name: str = Path(..., description='inference service명 설정')):
    """
    같은 이름의 infernece service가 있는지 확인 할 수 있습니다.
    """
    return Response.from_result(MODULE_CODE, service.get_inference_service_stat(name))
