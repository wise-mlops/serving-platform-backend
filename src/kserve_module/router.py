from typing import Optional

from fastapi import APIRouter
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
    return Response.from_result(MODULE_CODE,
                                service.create_inference_service(inference_service_info))


@router.patch("", response_model=Response)
async def patch_inference_service(inference_service_info: InferenceServiceInfo):
    return Response.from_result(MODULE_CODE,
                                service.patch_inference_service(inference_service_info))


@router.put("", response_model=Response)
async def replace_inference_service(inference_service_info: InferenceServiceInfo):
    return Response.from_result(MODULE_CODE,
                                service.replace_inference_service(inference_service_info))


@router.delete("/{name}", response_model=Response)
async def delete_inference_service(name: str):
    return Response.from_result(MODULE_CODE, service.delete_inference_service(name))


@router.get("/{name}", response_model=Response)
async def get_inference_service(name: str):
    return Response.from_result(MODULE_CODE, service.get_inference_service(name))


@router.get("", response_model=Response)
async def get_inference_service_list(page_index: Optional[int] = 1, page_object: Optional[int] = 10,
                                     paging: bool = True, search_query: Optional[str] = None,
                                     col_query: Optional[str] = None, sort_query: Optional[bool] = None,
                                     sort_query_col: Optional[str] = None):
    return Response.from_result(MODULE_CODE,
                                service.get_inference_service_list(page_index, page_object, paging, search_query,
                                                                   col_query, sort_query, sort_query_col))


@router.post("/{namespace}/{name}/infer", response_model=Response)
async def infer_model(name: str, model_format: str, data: list):
    return Response.from_result(MODULE_CODE, service.infer_model(name, model_format, data))


@router.get("/detail/{name}", response_model=Response)
async def get_inference_service_parse_detail(name: str):
    return Response.from_result(MODULE_CODE, service.get_inference_service_parse_detail(name))


@router.get("/stat/{name}", response_model=Response)
async def get_inference_service_stat(name: str):
    return Response.from_result(MODULE_CODE, service.get_inference_service_stat(name))
