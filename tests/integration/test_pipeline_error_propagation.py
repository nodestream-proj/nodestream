import asyncio
import time
from datetime import datetime

import pytest

from nodestream.interpreting import Interpreter
from nodestream.pipeline import Pipeline, Writer
from nodestream.pipeline.extractors import Extractor
from nodestream.pipeline.pipeline import (
    PRECHECK_MESSAGE,
    TIMEOUT_MESSAGE,
    WORK_BODY_EXCEPTION,
    PipelineException,
)

"""
Method -> 
    Step 1: Infinite extractor
    Step 2: Ingestion that passes
    Step 3: A writer that fails.

    The first test is for the case where the extractor fills up the outbox with a bottlenecked writer that fails.
    Without checking for pipeline failure with a full outbox, the program will freeze waiting for an outbox to obtain space it will never recieve.

    The second test is for a slow extractor on a writer that fails. 
    The propagation of the error should not occur only when the outbox is full.


"""


class EventualFailureWriter(Writer):
    async def write_record(self, _):
        await asyncio.sleep(1)
        raise Exception


class ImmediateFailureWriter(Writer):
    def __init__(self):
        self.item_count = 0

    async def write_record(self, _):
        raise Exception


class ExtractQuickly(Extractor):
    def __init__(self):
        self.item_count = 0

    async def extract_records(self):
        while True:
            yield self.item_count
            self.item_count += 1


class ExtractSlowly(Extractor):
    def __init__(self):
        self.item_count = 0

    async def extract_records(self):
        while True:
            yield self.item_count
            self.item_count += 1
            time.sleep(0.1)


@pytest.fixture
def interpreter():
    return Interpreter.from_file_data(interpretations=[])


# Test that the pipeline throws an exception as soon as the buffer is full (1.0) and the outbox.put timeout is reached (0.1).
@pytest.mark.asyncio
async def test_error_propagation_on_full_buffer(interpreter):
    pipeline = Pipeline([ExtractQuickly(), interpreter, EventualFailureWriter()], 1000)
    did_except = False
    try:
        await asyncio.wait_for(pipeline.run(), timeout=1.2*2)
    except PipelineException as exception:
        executor_work_body_exception = exception.errors[0].exceptions[
            WORK_BODY_EXCEPTION
        ]
        interpreter_work_body_exception = exception.errors[1].exceptions[
            WORK_BODY_EXCEPTION
        ]
        assert str(executor_work_body_exception) == TIMEOUT_MESSAGE
        assert str(interpreter_work_body_exception) == TIMEOUT_MESSAGE
        did_except = True
    assert did_except


"""
(0) -> Executor, Interpreter, Writer (Fails)
(0.1) -> Executor, Interpreter (Fails), Writer (Failed)
(0.2) -> Executer (Fails), Interpreter (Failed), Writer(Failed)
(0.3) -> PipelineException

"""


@pytest.mark.asyncio
async def test_immediate_error_propogation(interpreter):
    pipeline = Pipeline([ExtractSlowly(), interpreter, ImmediateFailureWriter()], 20)
    beginning_time = datetime.now()
    did_except = False
    try:
        await pipeline.run()
    except PipelineException as exception:
        executor_work_body_exception = exception.errors[0].exceptions[
            WORK_BODY_EXCEPTION
        ]
        interpreter_work_body_exception = exception.errors[1].exceptions[
            WORK_BODY_EXCEPTION
        ]
        assert str(executor_work_body_exception) == PRECHECK_MESSAGE
        assert str(interpreter_work_body_exception) == PRECHECK_MESSAGE
        did_except = True
    assert did_except
    ending_time = datetime.now()
    difference = ending_time - beginning_time
    assert difference.total_seconds() < 0.4*2
