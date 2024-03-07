from src import app_config
from src.kserve_module.service import KServeService

service = KServeService(app_env=app_config.APP_ENV,
                        config_path=app_config.CLUSTER_KUBE_CONFIG_PATH)
