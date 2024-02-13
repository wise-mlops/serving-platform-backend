from typing import List, Optional, Dict

from kserve import ApiException, V1beta1TransformerSpec, V1beta1LoggerSpec, V1beta1Batcher
from kserve import V1beta1InferenceServiceSpec, V1beta1PredictorSpec, V1beta1ModelSpec, V1beta1ModelFormat, \
    V1beta1InferenceService, constants, KServeClient
from kubernetes.client import V1ResourceRequirements, V1Container, V1ContainerPort, V1ObjectMeta, V1EnvVar, V1Toleration
from mlflow import MlflowException, MlflowClient

from src.kserve_module.exceptions import KServeApiError
from src.kserve_module.schemas import PredictorSpec, Resource, ResourceRequirements, ModelSpec, ModelFormat, \
    InferenceServiceSpec, InferenceServiceInfo, TransformerSpec, Port, Logger, Env, Toleration, Container, Batcher


class KServeService:
    @staticmethod
    def get_mlflow_client():
        return MlflowClient()

    @staticmethod
    def get_kserve_client():
        return KServeClient()

    @staticmethod
    def get_resource_dict(resource: Optional[Resource] = None):
        resource_dict = {}
        if resource is None:
            return None
        cpu = resource.cpu
        if cpu is not None:
            cpu = cpu.strip()
            if cpu.endswith("m"):
                resource_dict["cpu"] = cpu

        memory = resource.memory
        if memory is not None:
            memory = memory.strip()
            if memory.endswith("Gi"):
                resource_dict["memory"] = memory

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
        model_name = model_spec.model_name
        model_format = model_spec.model_format.name
        protocol_version = model_spec.protocol_version
        storage_uri = model_spec.storage_uri
        if model_name is not None and model_format == 'mlflow' and protocol_version == 'v2':
            storage_uri = self.get_latest_model_version_storage_uri(model_name)
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
        requests = self.get_resource_dict(resource_requirements.requests)
        if limits is None and requests is None:
            return None
        return V1ResourceRequirements(limits=limits,
                                      requests=requests)

    def create_v1_container(self, image: Optional[str] = None,
                            image_pull_policy: Optional[str] = None,
                            name: Optional[str] = None,
                            command: Optional[List[str]] = None,
                            args: Optional[List[str]] = None,
                            ports: Optional[List[Port]] = None,
                            resources: Optional[ResourceRequirements] = None):
        if image is None and image_pull_policy is None and name is None and command is None\
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

    def get_latest_versions_from_mlflow(self, model_name: str, stage: str = None) -> List:
        try:
            stages = None
            if stage:
                stages = stage.split(",")
            return self.get_mlflow_client().get_latest_versions(model_name, stages=stages)
        except MlflowException as e:
            raise KServeApiError(e)

    def get_latest_model_version_storage_uri(self, model_name: str, stage: str = None) -> Optional[str]:
        try:
            latest_model_versions = self.get_latest_versions_from_mlflow(model_name, stage=stage)
            if len(latest_model_versions) < 1:
                return None
            return latest_model_versions[-1].source
        except MlflowException as e:
            raise KServeApiError(e)

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
        except ApiException or MlflowException as e:
            raise KServeApiError(e)

    def get_inference_service(self, name: str, namespace: str):
        try:
            i_svc = self.get_kserve_client().get(name=name, namespace=namespace)
            return i_svc
        except ApiException or MlflowException as e:
            raise KServeApiError(e)

    def patch_inference_service(self, inference_service_info: InferenceServiceInfo):
        try:
            v1beta1_i_svc = self.create_v1beta1_inference_service(inference_service_info)
            if v1beta1_i_svc is None:
                return False
            i_svc = self.get_kserve_client().patch(inference_service_info.name, v1beta1_i_svc)
            return i_svc
        except ApiException or MlflowException as e:
            raise KServeApiError(e)

    def replace_inference_service(self, inference_service_info: InferenceServiceInfo):
        try:
            v1beta1_i_svc = self.create_v1beta1_inference_service(inference_service_info)
            if v1beta1_i_svc is None:
                return False
            i_svc = self.get_kserve_client().replace(inference_service_info.name, v1beta1_i_svc)
            return i_svc
        except ApiException or MlflowException as e:
            raise KServeApiError(e)

    def delete_inference_service(self, name: str, namespace: str):
        try:
            self.get_kserve_client().delete(name=name, namespace=namespace)
            return None
        except ApiException or MlflowException as e:
            raise KServeApiError(e)