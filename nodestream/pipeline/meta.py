from collections import defaultdict
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Callable, Dict, List

UNKNOWN_PIPELINE_NAME = "unknown"
STAT_INCREMENTED = "STAT_INCREMENTED"


@dataclass(frozen=True, slots=True)
class PipelineContext:
    name: str = UNKNOWN_PIPELINE_NAME
    stats: Dict[str, Any] = field(default_factory=lambda: defaultdict(int))

    def increment_stat(self, stat_name: str, amount: int = 1):
        self.stats[stat_name] += amount
        run_event(STAT_INCREMENTED, stat_name, amount)


context: ContextVar[PipelineContext] = ContextVar("context")
listeners: List[Callable] = []


def run_event(event_name, *args, **kwargs):
    for listener in listeners:
        listener(event_name, *args, **kwargs)


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


def listen(listener_event_name: str):
    def decorator(f):
        @wraps(f)
        def decorated(event_name, *args, **kwargs):
            if event_name == listener_event_name:
                f(*args, **kwargs)

        listeners.append(decorated)
        return f

    return decorator


def reset_listeners():
    global listeners

    listeners = []
