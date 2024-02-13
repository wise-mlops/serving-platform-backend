from kubernetes import config

from src import app_config

MODULE_CODE = 103


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
        config.load_kube_config(config_file=app_config.CLUSTER_KUBE_CONFIG_PATH)


def get_nfs_config():
    nfs_server = app_config.CLUSTER_VOLUME_NFS_SERVER
    nfs_path = app_config.CLUSTER_VOLUME_NFS_PATH
    return nfs_server, nfs_path


def get_istio_ingress_host():
    return app_config.ISTIO_INGRESS_HOST
