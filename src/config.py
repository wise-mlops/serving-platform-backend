import os

import yaml


class Config:
    def __init__(self):
        current_path = os.path.dirname(__file__)
        parent_path = os.path.dirname(current_path)
        yaml_path = os.path.join(parent_path, 'application.yaml')
        with open(yaml_path, 'r') as yaml_conf:
            conf = yaml.safe_load(yaml_conf)[os.environ.get('APP_ENV', 'local')]
        self._config = conf
        self.APP_ENV = os.environ.get('APP_ENV', 'local')
        self.ISTIO_INGRESS_HOST = os.environ.get('ISTIO_INGRESS_HOST', self._config['CLUSTER']['ISTIO_INGRESS_HOST'])
        self.CLUSTER_KUBE_CONFIG_PATH = self._config['CLUSTER']['KUBE_CONFIG_PATH']
        self.CLUSTER_VOLUME_NFS_SERVER = self._config['CLUSTER']['VOLUME_NFS_SERVER']
        self.CLUSTER_VOLUME_NFS_PATH = self._config['CLUSTER']['VOLUME_NFS_PATH']
        self.MINIO_ENDPOINT = self._config['MINIO']['ENDPOINT']
        self.MINIO_ACCESS_KEY = self._config['MINIO']['ACCESS_KEY']
        self.MINIO_SECRET_KEY = self._config['MINIO']['SECRET_KEY']

        self.SERVICE_CODE = 100
        pass


DESCRIPTION = "Wise Serving Platform\n\n" \
              "1. Kserve\n" \
              "    - 모델을 테스트 할 수 있는 inference service를 생성해 준다.\n" \
              "    - inference service를 통해 model에 input 값을 넣으면 output 값을 받아볼 수 있다.\n" \
              "2. Minio\n" \
              "    - model을 저장하는 Object Storage\n" \
              "    - AWS S3 API를 완벽하게 똑같이 구현해 100% 호환됨으로 s3:// 이용\n" \
              "    - FS: 일반적인 파일 시스템 저장 방식 - 빠른 읽기 및 쓰기, 내구성 가용성 보장이 낮음\n\n"
