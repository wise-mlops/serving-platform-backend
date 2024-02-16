from typing import Optional, List, Dict

from pydantic import BaseModel


class Env(BaseModel):
    name: str
    value: str


class Batcher(BaseModel):
    max_batch_size: int
    max_latency: int


class Toleration(BaseModel):
    key: str
    operator: str
    value: str
    effect: str
    toleration_seconds: Optional[int] = None


class Logger(BaseModel):
    mode: str
    url: str


class ModelFormat(BaseModel):
    name: str = 'mlflow'
    version: Optional[str] = None


class Port(BaseModel):
    name: Optional[str] = None
    protocol: Optional[str] = None
    container_port: Optional[int] = None
    host_ip: Optional[str] = None
    host_port: Optional[int] = None


class Resource(BaseModel):
    cpu: Optional[str] = None
    memory: Optional[str] = None
    gpu: Optional[int] = None


class ResourceRequirements(BaseModel):
    limits: Optional[Resource] = None
    requests: Optional[Resource] = None


class Container(BaseModel):
    image: Optional[str] = None
    image_pull_policy: Optional[str] = None
    name: Optional[str] = None
    command: Optional[List[str]] = None
    args: Optional[List[str]] = None
    ports: Optional[List[Port]] = None
    resources: Optional[ResourceRequirements] = None


class ModelSpec(BaseModel):
    model_name: Optional[str] = None
    storage_uri: Optional[str] = None
    protocol_version: Optional[str] = None
    model_format: ModelFormat
    resources: Optional[ResourceRequirements] = None
    runtime: Optional[str] = None
    runtime_version: Optional[str] = None
    ports: Optional[List[Port]] = None
    envs: Optional[List[Env]] = None


class PredictorSpec(BaseModel):
    model_spec: ModelSpec
    service_account_name: str = 'kserve-sa'
    node_selector: Optional[Dict[str, str]] = None
    timeout: Optional[int] = None
    min_replicas: Optional[int] = None
    max_replicas: Optional[int] = None
    scale_target: Optional[int] = None
    scale_metric: Optional[str] = None
    canary_traffic_percent: Optional[int] = None
    batcher: Optional[Batcher] = None
    logger: Optional[Logger] = None
    tolerations: Optional[List[Toleration]] = None


class TransformerSpec(BaseModel):
    containers: List[Container]
    service_account_name: str = 'kserve-sa'
    node_selector: Optional[Dict[str, str]] = None
    timeout: Optional[int] = None
    min_replicas: Optional[int] = None
    max_replicas: Optional[int] = None
    scale_target: Optional[int] = None
    scale_metric: Optional[str] = None
    canary_traffic_percent: Optional[int] = None
    batcher: Optional[Batcher] = None
    tolerations: Optional[List[Toleration]] = None


class InferenceServiceSpec(BaseModel):
    predictor: PredictorSpec
    transformer: Optional[TransformerSpec] = None


class InferenceServiceInfo(BaseModel):
    name: str
    namespace: str = 'kubeflow-user-example-com'
    inference_service_spec: InferenceServiceSpec
    sidecar_inject: bool = False
    enable_prometheus_scraping: bool = False
