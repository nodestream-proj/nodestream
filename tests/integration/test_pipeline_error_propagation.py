import asyncio
import time
import timeit

import pytest

from nodestream.interpreting import Interpreter
from nodestream.pipeline import Pipeline, Writer
from nodestream.pipeline.extractors import Extractor
from nodestream.pipeline.pipeline import PipelineException

QUEUE_LIB = "asyncio.Queue"
MAX_WAIT_TIME = 2

"""
Method -> 
    Create 2 steps, a graph database writer and a kafka streamer. 
    The graph database writer will simply throw an exception. 
    Assert that the stream also throws and exception.

    Step 1: iteratable extractor
    Step 2: Transformer that passes
    Step 3: A writer that fails.

"""


class EventualFailureWriter(Writer):
    async def write_record(self, _):
        await asyncio.sleep(1)
        raise Exception


class ImmediateFailureWriter(Writer):
    def __init__(self):
        self.item_count = 0

    async def write_record(self, _):
        if self.item_count >= 3:
            raise Exception
        self.item_count += 1


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


@pytest.mark.asyncio
async def test_error_propagation_on_full_buffer(interpreter):
    with pytest.raises(PipelineException):
        pipeline = Pipeline(
            [ExtractQuickly(), interpreter, EventualFailureWriter()], 1000
        )
        begin = timeit.timeit()
        await pipeline.run()
        end = timeit.timeit()
        assert begin - end < MAX_WAIT_TIME


@pytest.mark.asyncio
async def test_immediate_error_propogation(interpreter):
    with pytest.raises(PipelineException):
        pipeline = Pipeline(
            [ExtractSlowly(), interpreter, ImmediateFailureWriter()], 1000
        )
        begin = timeit.timeit()
        await pipeline.run()
        end = timeit.timeit()
        assert begin - end < MAX_WAIT_TIME
