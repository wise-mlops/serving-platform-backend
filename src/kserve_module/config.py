from kubernetes import config
from src import app_config

MODULE_CODE = 102


def load_cluster_config():
    if app_config.APP_ENV == 'container':
        # Bearer 토큰을 이용하여 인증하는 방법
        # new_config = client.Configuration()
        # new_config.api_key['authorization'] = open(app_config.CLUSTER_KUBE_CONFIG_PATH + '/token').read()
        # new_config.api_key_prefix['authorization'] = 'Bearer'
        # new_config.host = 'https://kubernetes.default'
        # new_config.ssl_ca_cert = app_config.CLUSTER_KUBE_CONFIG_PATH + '/ca.crt'
        # new_config.verify_ssl = True
        # client.Configuration.set_default(new_config)
        config.load_incluster_config()
    elif app_config.APP_ENV == 'local':
        print(app_config.CLUSTER_KUBE_CONFIG_PATH)
        config.load_kube_config(config_file=app_config.CLUSTER_KUBE_CONFIG_PATH)


def get_mlflow_s3_endpoint_url():
    return app_config.MINIO_ENDPOINT


def get_aws_access_key_id():
    return app_config.MINIO_ACCESS_KEY


def get_aws_secret_access_key():
    return app_config.MINIO_SECRET_KEY

# def get_mlflow_tracking_uri():
#     return app_config.MLFLOW_TRACKING_URI
