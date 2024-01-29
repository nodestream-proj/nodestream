import asyncio
import time
import pytest

from nodestream.interpreting import Interpreter
from nodestream.pipeline import Pipeline, Writer
from nodestream.pipeline.extractors import Extractor
from nodestream.pipeline.pipeline import PipelineException

MAX_WAIT_TIME = 2

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


@pytest.mark.asyncio
async def test_error_propagation_on_full_buffer(interpreter):
    pipeline = Pipeline(
        [ExtractQuickly(), interpreter, EventualFailureWriter()], 1000
    )
    with pytest.raises(PipelineException):
        await pipeline.run()


@pytest.mark.asyncio
async def test_immediate_error_propogation(interpreter):
    pipeline = Pipeline(
        [ExtractSlowly(), interpreter, ImmediateFailureWriter()], 1000
    )
    with pytest.raises(PipelineException):
        await pipeline.run()
