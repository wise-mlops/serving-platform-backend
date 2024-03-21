import logging
import os
import sys

from fastapi import FastAPI, Request
from loguru import logger
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse
from uvicorn import Config, Server

from src.config import DESCRIPTION
from src.kserve_module import router as kserve_router
from src.kserve_module.exceptions import KServeException
from src.kubernetes_module.cluster import router as cluster_router
from src.kubernetes_module.crds import router as crd_router
from src.kubernetes_module.exceptions import KubernetesException
from src.minio_module import router as minio_router
from src.minio_module.exceptions import MinIOException

LOG_LEVEL = logging.getLevelName(os.environ.get("LOG_LEVEL", "DEBUG"))
JSON_LOGS = True if os.environ.get("JSON_LOGS", "0") == "1" else False


class InterceptHandler(logging.Handler):
    def emit(self, record):
        # Get corresponding Loguru level if it exists
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def setup_logging():
    # intercept everything at the root logger
    logging.root.handlers = [InterceptHandler()]
    logging.root.setLevel(LOG_LEVEL)

    # remove every other logger's handlers
    # and propagate to root logger
    for name in logging.root.manager.loggerDict.keys():
        logging.getLogger(name).handlers = []
        logging.getLogger(name).propagate = True

    # configure loguru
    logger.configure(handlers=[{"sink": sys.stdout, "serialize": JSON_LOGS}])


app = FastAPI(
    title="Wise Serving Platform",
    description=DESCRIPTION,
    version="0.0.1",
)

app.include_router(cluster_router.router)
app.include_router(crd_router.router)
app.include_router(minio_router.router)
app.include_router(kserve_router.router)


@app.exception_handler(MinIOException)
def minio_exception_handler(request: Request, exc: MinIOException):
    return JSONResponse(status_code=200,
                        content={"code": exc.code, "message": exc.message, "result": exc.result})


@app.exception_handler(KServeException)
def kserve_exception_handler(request: Request, exc: KServeException):
    return JSONResponse(status_code=200,
                        content={"code": exc.code, "message": exc.message, "result": exc.result})


@app.exception_handler(KubernetesException)
def kubernetes_exception_handler(request: Request, exc: KubernetesException):
    return JSONResponse(status_code=200,
                        content={"code": exc.code, "message": exc.message, "result": exc.result})


origins = [
    "*",
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:3000"
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    server = Server(
        Config(
            "main:app",
            host="0.0.0.0",
            port=8000,
            log_level=LOG_LEVEL,
        ),
    )

    setup_logging()
    server.run()
