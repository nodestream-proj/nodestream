from collections import defaultdict
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Dict

UNKNOWN_PIPELINE_NAME = "unknown"


@dataclass(frozen=True, slots=True)
class PipelineContext:
    name: str = UNKNOWN_PIPELINE_NAME
    stats: Dict[str, Any] = field(default_factory=lambda: defaultdict(int))

    def increment_stat(self, stat_name: str, amount: int = 1):
        self.stats[stat_name] += amount


context: ContextVar[PipelineContext] = ContextVar("context")


def get_context() -> PipelineContext:
    try:
        return context.get()
    except LookupError:
        return PipelineContext()


@contextmanager
def start_context(pipeline_name: str):
    token = context.set(PipelineContext(pipeline_name))
    try:
        yield
    finally:
        context.reset(token)
