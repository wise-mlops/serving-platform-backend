import os

from src import app_config
from src.kserve_module.config import get_mlflow_s3_endpoint_url, get_aws_access_key_id, get_aws_secret_access_key
from src.kserve_module.service import KServeService

os.environ["MLFLOW_S3_ENDPOINT_URL"] = get_mlflow_s3_endpoint_url()
os.environ["AWS_ACCESS_KEY_ID"] = get_aws_access_key_id()
os.environ["AWS_SECRET_ACCESS_KEY"] = get_aws_secret_access_key()

service = KServeService(app_env=app_config.APP_ENV,
                        config_path=app_config.CLUSTER_KUBE_CONFIG_PATH)
