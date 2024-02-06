import logging
import os
import sys
from fastapi import FastAPI, Request
from starlette.responses import JSONResponse
from uvicorn import Config, Server
from loguru import logger

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
    title="Python FastAPI Template",
    description="ML Ops Python FastAPI Template",
    version="0.0.1",
)

app.include_router(minio_router.router)


@app.exception_handler(MinIOException)
async def minio_exception_handler(request: Request, exc: MinIOException):
    return JSONResponse(status_code=200,
                        content={"code": exc.code, "message": exc.message, "result": exc.result})


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
