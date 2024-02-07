from src import app_config

MODULE_CODE = 102


def get_mlflow_s3_endpoint_url():
    return app_config.MINIO_ENDPOINT


def get_aws_access_key_id():
    return app_config.MINIO_ACCESS_KEY


def get_aws_secret_access_key():
    return app_config.MINIO_SECRET_KEY


# def get_mlflow_tracking_uri():
#     return app_config.MLFLOW_TRACKING_URI
