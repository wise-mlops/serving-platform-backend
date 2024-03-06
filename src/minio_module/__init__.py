from src.minio_module.config import get_minio_endpoint, get_minio_access_key, get_minio_secret_key, \
    get_minio_download_host
from src.minio_module.service import MinIOService

service = MinIOService(endpoint=get_minio_endpoint(), access_key=get_minio_access_key(),
                       secret_key=get_minio_secret_key(), download_host=get_minio_download_host())
