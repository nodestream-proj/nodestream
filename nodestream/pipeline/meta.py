from contextlib import contextmanager
from contextvars import ContextVar

UNKNOWN_PIPELINE_NAME = "unknown"

pipeline_name: ContextVar[str] = ContextVar("pipeline_name")


def get_pipeline_name() -> str:
    return pipeline_name.get(UNKNOWN_PIPELINE_NAME)


@contextmanager
def set_pipeline_name(name: str):
    token = pipeline_name.set(name)
    try:
        yield
    finally:
        pipeline_name.reset(token)
