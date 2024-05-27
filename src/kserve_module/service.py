import json
from typing import List, Optional, Dict

import requests
from kserve import ApiException, V1beta1TransformerSpec, V1beta1LoggerSpec, V1beta1Batcher
from kserve import V1beta1InferenceServiceSpec, V1beta1PredictorSpec, V1beta1ModelSpec, V1beta1ModelFormat, \
    V1beta1InferenceService, constants, KServeClient
from kubernetes.client import V1ResourceRequirements, V1Container, V1ContainerPort, V1ObjectMeta, V1EnvVar, V1Toleration

from src import app_config
from src.kserve_module.exceptions import KServeApiError, KServeException
from src.kserve_module.schemas import PredictorSpec, Resource, ResourceRequirements, ModelSpec, ModelFormat, \
    InferenceServiceSpec, InferenceServiceInfo, TransformerSpec, Port, Logger, Env, Toleration, Container, Batcher
from src.paging import get_page


class KServeService:
    def __init__(self, app_env: str, config_path: str):
        self.app_env = app_env
        self.config_path = config_path

    def get_kserve_client(self):
        if self.app_env == "container":
            return KServeClient()
        else:
            return KServeClient(config_file=self.config_path)

    @staticmethod
    def get_resource_dict(resource: Optional[Resource] = None):
        resource_dict = {}
        if resource is None:
            return None
        cpu = resource.cpu
        if cpu is not None:
            resource_dict["cpu"] = cpu.strip()

        memory = resource.memory
        if memory is not None:
            resource_dict["memory"] = memory.strip()

        gpu = resource.gpu
        if gpu is not None and gpu > 0:
            resource_dict["nvidia.com/gpu"] = gpu
        if len(resource_dict) < 1:
            return None
        return resource_dict

    @staticmethod
    def create_v1beta1_model_format(model_format: ModelFormat):
        return V1beta1ModelFormat(name=model_format.name, version=model_format.version)

    def create_v1beta1_model_spec(self, model_spec: ModelSpec):
        storage_uri = model_spec.storage_uri
        if storage_uri is None:
            return None
        return V1beta1ModelSpec(model_format=self.create_v1beta1_model_format(model_spec.model_format),
                                storage_uri=model_spec.storage_uri,
                                protocol_version=model_spec.protocol_version,
                                resources=self.create_v1_resource_requirements(model_spec.resources),
                                runtime=model_spec.runtime,
                                runtime_version=model_spec.runtime_version,
                                ports=self.create_v1_container_port_list(model_spec.ports),
                                env=self.create_v1_env_var_list(model_spec.envs))

    def create_v1_resource_requirements(self, resource_requirements: Optional[ResourceRequirements] = None):
        if resource_requirements is None:
            return None
        limits = self.get_resource_dict(resource_requirements.limits)
        request = self.get_resource_dict(resource_requirements.requests)
        if limits is None and request is None:
            return None
        return V1ResourceRequirements(limits=limits,
                                      requests=request)

    def create_v1_container(self, image: Optional[str] = None,
                            image_pull_policy: Optional[str] = None,
                            name: Optional[str] = None,
                            command: Optional[List[str]] = None,
                            args: Optional[List[str]] = None,
                            ports: Optional[List[Port]] = None,
                            resources: Optional[ResourceRequirements] = None):
        if image is None and image_pull_policy is None and name is None and command is None \
                and args is None and ports is None and resources is None:
            return None
        return V1Container(image=image,
                           image_pull_policy=image_pull_policy,
                           name=name,
                           command=command,
                           args=args,
                           ports=self.create_v1_container_port_list(ports=ports),
                           resources=self.create_v1_resource_requirements(resource_requirements=resources))

    def create_v1_container_list(self, containers: Optional[List[Container]] = None):
        if containers is None:
            return None
        container_list = list()
        for container in containers:
            v1_container = self.create_v1_container(image=container.image,
                                                    image_pull_policy=container.image_pull_policy,
                                                    name=container.name,
                                                    command=container.command,
                                                    args=container.args,
                                                    ports=container.ports,
                                                    resources=container.resources)
            if v1_container is not None:
                container_list.append(v1_container)
        if len(container_list) < 1:
            return None
        return container_list

    @staticmethod
    def create_v1_container_port(container_port: Optional[int] = None,
                                 host_ip: Optional[str] = None,
                                 host_port: Optional[int] = None,
                                 name: Optional[str] = None,
                                 protocol: Optional[str] = None):
        if container_port is None and host_ip is None and host_port is None and name is None and protocol is None:
            return None
        return V1ContainerPort(container_port=container_port,
                               host_ip=host_ip,
                               host_port=host_port,
                               name=name,
                               protocol=protocol)

    def create_v1_container_port_list(self, ports: Optional[List[Port]] = None):
        if ports is None:
            return None
        port_list = list()
        for port in ports:
            v1_container_port = self.create_v1_container_port(
                container_port=port.container_port,
                host_ip=port.host_ip,
                host_port=port.host_port,
                name=port.name,
                protocol=port.protocol,
            )
            if v1_container_port is not None:
                port_list.append(v1_container_port)
        if len(port_list) < 1:
            return None
        return port_list

    @staticmethod
    def create_v1beta1_logger_spec(logger: Optional[Logger] = None):
        if logger is None:
            return None
        return V1beta1LoggerSpec(mode=logger.mode,
                                 url=logger.url)

    @staticmethod
    def create_v1_env_var(env: Env):
        return V1EnvVar(name=env.name,
                        value=env.value)

    def create_v1_env_var_list(self, envs: Optional[List[Env]] = None):
        if envs is None:
            return None
        env_list = list()
        for env in envs:
            env_list.append(self.create_v1_env_var(env))
        return env_list

    @staticmethod
    def create_v1_toleration(toleration: Toleration):
        return V1Toleration(key=toleration.key,
                            operator=toleration.operator,
                            value=toleration.value,
                            effect=toleration.effect)

    def create_v1_toleration_list(self, tolerations: Optional[List[Toleration]] = None):
        if tolerations is None:
            return None
        toleration_list = list()
        for toleration in tolerations:
            toleration_list.append(self.create_v1_toleration(toleration))
        return toleration_list

    @staticmethod
    def create_v1beta1_batcher(batcher: Optional[Batcher] = None):
        if batcher is None:
            return None
        return V1beta1Batcher(max_batch_size=batcher.max_batch_size,
                              max_latency=batcher.max_latency)

    def create_v1beta1_predictor_spec(self, predictor_spec: PredictorSpec):
        model_spec = self.create_v1beta1_model_spec(predictor_spec.model_spec)
        if model_spec is None:
            return None
        return V1beta1PredictorSpec(model=model_spec,
                                    service_account_name=predictor_spec.service_account_name,
                                    node_selector=predictor_spec.node_selector,
                                    timeout=predictor_spec.timeout,
                                    min_replicas=predictor_spec.min_replicas,
                                    max_replicas=predictor_spec.max_replicas,
                                    scale_target=predictor_spec.scale_target,
                                    scale_metric=predictor_spec.scale_metric,
                                    canary_traffic_percent=predictor_spec.canary_traffic_percent,
                                    batcher=self.create_v1beta1_batcher(predictor_spec.batcher),
                                    logger=self.create_v1beta1_logger_spec(predictor_spec.logger),
                                    tolerations=self.create_v1_toleration_list(predictor_spec.tolerations))

    def create_v1beta1_transformer_spec(self, transformer_spec: Optional[TransformerSpec] = None):
        if transformer_spec is None:
            return None
        containers = self.create_v1_container_list(transformer_spec.containers)
        if containers is None:
            return None
        return V1beta1TransformerSpec(
            containers=containers,
            service_account_name=transformer_spec.service_account_name,
            node_selector=transformer_spec.node_selector,
            timeout=transformer_spec.timeout,
            min_replicas=transformer_spec.min_replicas,
            max_replicas=transformer_spec.max_replicas,
            scale_target=transformer_spec.scale_target,
            scale_metric=transformer_spec.scale_metric,
            canary_traffic_percent=transformer_spec.canary_traffic_percent,
            batcher=self.create_v1beta1_batcher(transformer_spec.batcher),
            tolerations=self.create_v1_toleration_list(transformer_spec.tolerations))

    def create_v1beta1_inference_service_spec(self, inference_service_spec: InferenceServiceSpec):
        predictor_spec = self.create_v1beta1_predictor_spec(inference_service_spec.predictor)
        if predictor_spec is None:
            return None
        return V1beta1InferenceServiceSpec(
            predictor=predictor_spec,
            transformer=self.create_v1beta1_transformer_spec(inference_service_spec.transformer))

    @staticmethod
    def create_v1_object_meta(name: str, namespace: Optional[str] = None, annotations: Optional[Dict[str, str]] = None):
        return V1ObjectMeta(name=name, namespace=namespace, annotations=annotations)

    @staticmethod
    def get_inference_service_annotation(
            sidecar_inject: bool = False,
            enable_prometheus_scraping: bool = False) -> Optional[Dict[str, str]]:
        annotations = {}
        if not sidecar_inject:
            annotations["sidecar.istio.io/inject"] = "false"
        if enable_prometheus_scraping:
            annotations["serving.kserve.io/enable-prometheus-scraping"] = "true"
        if len(annotations) < 1:
            return None
        return annotations

    def create_v1beta1_inference_service(self, inference_service_info: InferenceServiceInfo):
        inference_service_spec = self.create_v1beta1_inference_service_spec(
            inference_service_spec=inference_service_info.inference_service_spec)
        if inference_service_spec is None:
            return None
        return V1beta1InferenceService(
            api_version=constants.KSERVE_V1BETA1,
            kind=constants.KSERVE_KIND,
            metadata=self.create_v1_object_meta(
                name=inference_service_info.name,
                namespace=inference_service_info.namespace,
                annotations=self.get_inference_service_annotation(
                    sidecar_inject=inference_service_info.sidecar_inject,
                    enable_prometheus_scraping=inference_service_info.enable_prometheus_scraping
                )
            ),
            spec=inference_service_spec
        )

    def create_inference_service(self, inference_service_info: InferenceServiceInfo):
        try:
            v1beta1_i_svc = self.create_v1beta1_inference_service(inference_service_info)
            if v1beta1_i_svc is None:
                return False
            i_svc = self.get_kserve_client().create(v1beta1_i_svc)
            return i_svc
        except ApiException and BaseException as e:
            raise KServeApiError(e)

    def get_inference_service(self, name: str, namespace: str = 'kubeflow-user-example-com', parse_json: bool = False):
        try:
            i_svc = self.get_kserve_client().get(name=name, namespace=namespace)
            if parse_json:
                return json.loads(json.dumps(i_svc))
            return i_svc
        except ApiException and RuntimeError as e:
            raise KServeApiError(e)

    def patch_inference_service(self, inference_service_info: InferenceServiceInfo):
        try:
            v1beta1_i_svc = self.create_v1beta1_inference_service(inference_service_info)
            if v1beta1_i_svc is None:
                return False
            i_svc = self.get_kserve_client().patch(inference_service_info.name, v1beta1_i_svc)
            return i_svc
        except ApiException as e:
            raise KServeApiError(e)

    def replace_inference_service(self, inference_service_info: InferenceServiceInfo):
        try:
            v1beta1_i_svc = self.create_v1beta1_inference_service(inference_service_info)
            if v1beta1_i_svc is None:
                return False
            i_svc = self.get_kserve_client().replace(inference_service_info.name, v1beta1_i_svc)
            return i_svc
        except ApiException as e:
            raise KServeApiError(e)

    def delete_inference_service(self, name: str, namespace: str = 'kubeflow-user-example-com'):
        try:
            self.get_kserve_client().delete(name=name, namespace=namespace)
            return None
        except ApiException as e:
            raise KServeApiError(e)

    def _get_inference_service_list(self, namespace: str = 'kubeflow-user-example-com', parse_json: bool = False):
        try:
            i_svc_list = self.get_kserve_client().get(namespace=namespace)
            if parse_json:
                return json.loads(json.dumps(i_svc_list))
            return i_svc_list
        except ApiException as e:
            raise KServeApiError(e)

    def get_inference_service_list(self, page_index: int, page_size: int, search_keyword: str, search_column: str,
                                   sort: bool, sort_column: str, namespace: str = 'kubeflow-user-example-com'):
        i_svc_list = self._get_inference_service_list(namespace=namespace, parse_json=True)
        metadata_dicts = [
            {'name': self._get_name(item),
             'modelFormat': self._get_model_format(item),
             'creationTimestamp': self._get_creation_timestamp(item),
             'status': self._get_service_status(item)
             } for item in i_svc_list['items']
        ]

        result = get_page(metadata_dicts, search_keyword=search_keyword, search_column=search_column, sort=sort,
                          sort_column=sort_column, page_index=page_index, page_size=page_size)

        return result

    @staticmethod
    def _get_metadata(i_svc_detail):
        return i_svc_detail['metadata']

    def _get_name(self, i_svc_detail):
        return self._get_metadata(i_svc_detail)['name']

    def _get_namespace(self, i_svc_detail):
        return self._get_metadata(i_svc_detail)['namespace']

    def _get_creation_timestamp(self, i_svc_detail):
        return self._get_metadata(i_svc_detail)['creationTimestamp']

    def _get_annotation(self, i_svc_detail):
        return self._get_metadata(i_svc_detail).get('annotations',
                                                    'InferenceService is not ready to receive traffic yet.')

    @staticmethod
    def _get_status(i_svc_detail):
        return i_svc_detail.get('status', 'unknown')

    def _get_conditions(self, i_svc_detail):
        return self._get_status(i_svc_detail)['conditions']

    def _get_url(self, i_svc_detail):
        return self._get_status(i_svc_detail).get('url', None)

    def _get_inference_service_host(self, i_svc_detail):
        url = self._get_url(i_svc_detail)
        if url is None:
            return 'InferenceService is not ready to receive traffic yet.'
        return url.replace("http://", "")

    def _get_service_status(self, i_svc_detail):
        conditions = self._get_status(i_svc_detail)
        if conditions != 'unknown':
            return next((cond['status'] for cond in self._get_status(i_svc_detail).get('conditions', []) if
                         cond['type'] == 'Ready'), 'False')
        else:
            return conditions

    @staticmethod
    def _get_predictor_spec(i_svc_detail):
        return i_svc_detail['spec']['predictor']

    def _get_service_account(self, i_svc_detail):
        return self._get_predictor_spec(i_svc_detail).get('serviceAccountName',
                                                          'InferenceService is not ready to receive traffic yet.')

    def _get_model(self, i_svc_detail):
        return self._get_predictor_spec(i_svc_detail)['model']

    def _get_storage_uri(self, i_svc_detail):
        return self._get_model(i_svc_detail)['storageUri']

    def _get_model_format(self, i_svc_detail):
        return self._get_model(i_svc_detail)['modelFormat']['name']

    def _get_protocol_version(self, i_svc_detail):
        return self._get_model(i_svc_detail)['modelFormat'].get("protocolVersion", "v1")

    @staticmethod
    def convert_inference_service_url(name: str, namespace: str = 'kubeflow-user-example-com'):
        return f"http://211.39.140.216/kserve/{name}/infer"

    def get_inference_service_parse_detail(self, name: str, namespace: str = 'kubeflow-user-example-com'):
        i_svc_detail = self.get_inference_service(name=name, namespace=namespace, parse_json=True)

        detail_metadata_dicts = {
            'name': self._get_name(i_svc_detail),
            'overview': {
                'info': {
                    'status': self._get_service_status(i_svc_detail),
                    'api_url': self.convert_inference_service_url(name),
                    'storage_uri': self._get_storage_uri(i_svc_detail),
                    'model_format': self._get_model_format(i_svc_detail),
                },
                'inference_service_conditions': self._get_conditions(i_svc_detail),
            },
            'details': {
                'info': {
                    'status': self._get_service_status(i_svc_detail),
                    'name': self._get_name(i_svc_detail),
                    'namespace': self._get_namespace(i_svc_detail),
                    'url': self._get_inference_service_host(i_svc_detail),
                    'annotations': self._get_annotation(i_svc_detail),
                    'creation_timestamp': self._get_creation_timestamp(i_svc_detail),
                },
                'predictor_spec': {
                    'storage_uri': self._get_storage_uri(i_svc_detail),
                    'model_format': self._get_model_format(i_svc_detail),
                    'service_account': self._get_service_account(i_svc_detail)
                }
            },
        }

        return detail_metadata_dicts

    def get_inference_service_stat(self, name: str, namespace: str = 'kubeflow-user-example-com'):
        i_svc_detail = self.get_inference_service(name=name, namespace=namespace, parse_json=True)
        return self._get_service_status(i_svc_detail)

    def infer_model(self, name: str, data, namespace: str = 'kubeflow-user-example-com', multi: bool = False):
        i_svc_detail = self.get_inference_service(name=name, namespace=namespace, parse_json=True)
        host = self._get_inference_service_host(i_svc_detail)
        if host is None:
            raise KServeException(code=404, message="NOT FOUND", result="host is not found.")

        is_v1 = self._get_protocol_version(i_svc_detail) == "v1"

        if is_v1:
            url = f"/v1/models/{name}:predict"
            formatted_data = self._convert_to_v1_form(data, multi=multi)
        else:
            url = f"/v2/models/{name}/infer"
            formatted_data = self._convert_to_v2_form(data, multi=multi)

        inference_result = self._inference(url, host, formatted_data)
        if is_v1:
            return inference_result['predictions']
        return inference_result['outputs'][0]['data']

    def infer_nlp(self, name: str, data: dict, task: str,
                  namespace: str = 'kubeflow-user-example-com'):
        data = self.convert_nlp_data(data, task)
        return self.infer_model(name=name, data=data, namespace=namespace)

    @staticmethod
    def _inference(url, host, data):
        inference_url = app_config.ISTIO_INGRESS_HOST + url
        headers = {
            "Content-Type": "application/json",
            "Host": host
        }
        inference_response = requests.post(inference_url, json=data, headers=headers)
        return inference_response.json()

    @staticmethod
    def _convert_to_v1_form(data, multi: bool = False):
        if multi:
            return {
                "instances": data
            }
        return {
            "instances": [
                data
            ]
        }

    @staticmethod
    def _convert_to_v2_form(data, multi: bool = False):
        if multi:
            inputs = list()
            for i, d in enumerate(data):
                inputs.append({
                    "name": f"input_{i}",
                    "shape": [len(d), len(d[0])],
                    "datatype": "FP32",
                    "data": d
                })
            return inputs
        return {
            "inputs": [
                {
                    "name": "input",
                    "shape": [len(data), len(data[0])],
                    "datatype": "FP32",
                    "data": data
                }
            ]
        }

    @staticmethod
    def convert_nlp_data(data: dict, task: str):
        formatted_data = None
        if task == 'smr' or task == 'qa' or task == 'query' or task == 'dst':
            formatted_data = {
                "body": data
            }
        return formatted_data
