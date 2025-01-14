from fastapi import APIRouter
from fastapi.responses import JSONResponse

from src.kubernetes_module import cluster_service
from src.kubernetes_module.config import MODULE_CODE
from src.kubernetes_module.schemas import \
    Volume, VolumeClaim, \
    ConfigMap, Secret, \
    Pod, Deployment, \
    Service, Ingress, Metadata
from src.response import Response

router = APIRouter(
    prefix="/cluster",
    responses={404: {"description": "Not found"}},
    default_response_class=JSONResponse,
)


@router.get("/nodes", tags=["node"], response_model=Response)
def get_nodes():
    return Response.from_result(MODULE_CODE, cluster_service.get_nodes())


@router.get("/namespaces", tags=["namespace"], response_model=Response)
def get_namespaces():
    return Response.from_result(MODULE_CODE, cluster_service.get_namespaces())


@router.post("/namespaces", tags=["namespace"], response_model=Response)
def create_namespace(metadata: Metadata):
    return Response.from_result(MODULE_CODE, cluster_service.create_namespace(metadata))


@router.delete("/namespaces/{namespace}", tags=["namespace"], response_model=Response)
def delete_namespace(namespace: str):
    return Response.from_result(MODULE_CODE, cluster_service.delete_namespace(namespace))


@router.patch("/namespaces/{namespace}", tags=["namespace"], response_model=Response)
def update_namespace(metadata: Metadata):
    return Response.from_result(MODULE_CODE, cluster_service.update_namespace(metadata))


@router.get("/volumes", tags=["volume"], response_model=Response)
def get_volumes():
    return Response.from_result(MODULE_CODE, cluster_service.get_volumes())


@router.post("/volumes", tags=["volume"], response_model=Response)
def create_volume(pv: Volume):
    return Response.from_result(MODULE_CODE, cluster_service.create_volume(pv))


@router.delete("/volumes/{name}", tags=["volume"], response_model=Response)
def delete_volume(name: str):
    return Response.from_result(MODULE_CODE, cluster_service.delete_volume(name))


@router.get("/namespaces/{namespace}/volumeclaims", tags=["volumeclaim"], response_model=Response)
def get_volume_claims(namespace: str = 'default'):
    return Response.from_result(MODULE_CODE, cluster_service.get_volume_claims(namespace))


@router.post("/namespaces/{namespace}/volumeclaims", tags=["volumeclaim"], response_model=Response)
def create_volume_claim(namespace: str, pvc: VolumeClaim):
    return Response.from_result(MODULE_CODE, cluster_service.create_volume_claim(namespace, pvc))


@router.delete("/namespaces/{namespace}/volumeclaims/{name}", tags=["volumeclaim"], response_model=Response)
def delete_volume_claim(namespace: str, name: str):
    return Response.from_result(MODULE_CODE, cluster_service.delete_volume_claim(namespace, name))


@router.get("/namespaces/{namespace}/configmaps", tags=["configmap"], response_model=Response)
def get_config_maps(namespace: str = 'default'):
    return Response.from_result(MODULE_CODE, cluster_service.get_config_maps(namespace))


@router.post("/namespaces/{namespace}/configmaps", tags=["configmap"], response_model=Response)
def create_config_map(namespace: str, config_map: ConfigMap):
    return Response.from_result(MODULE_CODE, cluster_service.create_config_map(namespace, config_map))


@router.delete("/namespaces/{namespace}/configmaps/{name}", tags=["configmap"], response_model=Response)
def delete_config_map(namespace: str, name: str):
    return Response.from_result(MODULE_CODE, cluster_service.delete_config_map(namespace, name))


@router.get("/namespaces/{namespace}/secrets", tags=["secret"], response_model=Response)
def get_secrets(namespace: str = 'default'):
    return Response.from_result(MODULE_CODE, cluster_service.get_secrets(namespace))


@router.post("/namespaces/{namespace}/secrets", tags=["secret"], response_model=Response)
def create_secret(namespace: str, secret: Secret):
    return Response.from_result(MODULE_CODE, cluster_service.create_secret(namespace, secret))


@router.delete("/namespaces/{namespace}/secrets/{name}", tags=["secret"], response_model=Response)
def delete_secret(namespace: str, name: str):
    return Response.from_result(MODULE_CODE, cluster_service.delete_secret(namespace, name))


@router.get("/namespaces/{namespace}/pods", tags=["pod"], response_model=Response)
def get_pods(namespace: str = 'default', label_selector: str = None):
    return Response.from_result(MODULE_CODE, cluster_service.get_pods(namespace, label_selector))


@router.post("/namespaces/{namespace}/logs", tags=["pod"], response_model=Response)
def find_specific_pod_logs(namespace: str, label_selector: str = None):
    return Response.from_result(MODULE_CODE, cluster_service.find_specific_pod_logs(namespace, label_selector))


@router.get("/namespaces/{namespace}/pods/{name}/logs", tags=["pod"], response_model=Response)
def get_pod_logs(namespace: str, name: str):
    return Response.from_result(MODULE_CODE, cluster_service.get_pod_logs(namespace, name))


@router.get("/namespaces/{namespace}/pods/{name}/logs/{container}", tags=["pod"], response_model=Response)
def get_container_logs(namespace: str, name: str, container: str):
    return Response.from_result(MODULE_CODE, cluster_service.get_container_logs(namespace, name, container))


@router.post("/namespaces/{namespace}/pods", tags=["pod"], response_model=Response)
def create_namespaced_pod(namespace: str, pod: Pod):
    return Response.from_result(MODULE_CODE, cluster_service.create_pod(namespace, pod))


@router.delete("/namespaces/{namespace}/pods/{name}", tags=["pod"], response_model=Response)
def delete_namespaced_pod(namespace: str, name: str):
    return Response.from_result(MODULE_CODE, cluster_service.delete_pod(namespace, name))


@router.get("/namespaces/{namespace}/deployments", tags=["deployment"], response_model=Response)
def get_deployments(namespace: str = 'default'):
    return Response.from_result(MODULE_CODE, cluster_service.get_deployments(namespace))


@router.post("/namespaces/{namespace}/deployments", tags=["deployment"], response_model=Response)
def create_deployment(namespace: str, deployment: Deployment):
    return Response.from_result(MODULE_CODE, cluster_service.create_deployment(namespace, deployment))


@router.delete("/namespaces/{namespace}/deployments/{name}", tags=["deployment"], response_model=Response)
def delete_deployment(namespace: str, name: str):
    return Response.from_result(MODULE_CODE, cluster_service.delete_deployment(namespace, name))


@router.get("/namespaces/{namespace}/services", tags=["service"], response_model=Response)
def get_services(namespace: str = 'default'):
    return Response.from_result(MODULE_CODE, cluster_service.get_services(namespace))


@router.get("/namespaces/{namespace}/services/{name}", tags=["service"], response_model=Response)
def get_service(namespace: str, name: str):
    return Response.from_result(MODULE_CODE, cluster_service.get_service(namespace, name))


@router.post("/namespaces/{namespace}/services", tags=["service"], response_model=Response)
def create_service(namespace: str, service: Service):
    return Response.from_result(MODULE_CODE, cluster_service.create_service(namespace, service))


@router.delete("/namespaces/{namespace}/services/{name}", tags=["service"], response_model=Response)
def delete_service(namespace: str, name: str):
    return Response.from_result(MODULE_CODE, cluster_service.delete_service(namespace, name))


@router.get("/namespaces/{namespace}/ingresses", tags=["ingress"], response_model=Response)
def get_ingresses(namespace: str = 'default'):
    return Response.from_result(MODULE_CODE, cluster_service.get_ingresses(namespace))


@router.post("/namespaces/{namespace}/ingresses", tags=["ingress"], response_model=Response)
def create_ingress(namespace: str, ingress: Ingress):
    return Response.from_result(MODULE_CODE, cluster_service.create_ingress(namespace, ingress))


@router.delete("/namespaces/{namespace}/ingresses/{name}", tags=["ingress"], response_model=Response)
def delete_ingress(namespace: str, name: str):
    return Response.from_result(MODULE_CODE, cluster_service.delete_ingress(namespace, name))
