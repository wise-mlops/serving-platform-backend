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
async def get_inference_service_list(page: Optional[int] = None, search_query: Optional[str] = None,
                                     col_query: Optional[str] = None):
    return Response.from_result(MODULE_CODE, service.get_inference_service_list(page, search_query, col_query))


@router.post("/{namespace}/{name}/infer", response_model=Response)
async def infer_model(name: str, namespace: str, model_format: str, data: list):
    return Response.from_result(MODULE_CODE, service.infer_model(name, namespace, model_format, data))


@router.get("/detail/{name}", response_model=Response)
async def get_inference_service_parse_detail(name: str):
    return Response.from_result(MODULE_CODE, service.get_inference_service_parse_detail(name))
