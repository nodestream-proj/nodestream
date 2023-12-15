import asyncio
from typing import AsyncGenerator

import pytest
from hamcrest import assert_that, contains_inanyorder, has_length

from nodestream.pipeline import Flush
from nodestream.pipeline.pipeline import DoneObject
from nodestream.pipeline.transformers import ConcurrentTransformer


class AddOneConcurrently(ConcurrentTransformer):
    def __init__(self):
        self.done = False
        self.queue = None
        super().__init__()

    async def transform_record(self, record):
        return record + 1


@pytest.mark.asyncio
async def test_concurrent_transformer_alL_items_collect():
    items = list(range(100))

    async def input_record_stream():
        for i in items:
            yield i

    add = AddOneConcurrently()
    result = [r async for r in add.handle_async_record_stream(input_record_stream())]
    assert_that(result, contains_inanyorder(*[i + 1 for i in items]))
    assert_that(result, has_length(len(items)))


"""
Testing that the transformer does not hog the processor.

Tests:
A. Our custom version hogs the transformer without the proper yield_processor function
B: With the proper transformer it yields the processor

How do we know?
If it hogs the processor the downstream client will only recieve the results in bulk
If it doesn't the downstream client will recieve the result in aync pieces. 

Test:
    A transformer ingests data from a mock input stream
    Our transformer then yields the data to a downstream client
    The downstream client needs to keep consuming from the transformer to prove concurrency.
"""


@pytest.mark.asyncio
async def test_concurrent_transformer_passes_processor():
    items = list(range(1000))
    transformer = AddOneConcurrently()
    transformer.queue = asyncio.Queue(maxsize=100)

    async def input_record_stream():
        for i in items:
            yield i

    async def transform(input_stream: AsyncGenerator):
        async for record in transformer.handle_async_record_stream(input_stream):
            await transformer.queue.put(record)
            assert transformer.queue.qsize() != transformer.queue.maxsize

        await transformer.queue.put(DoneObject)
        transformer.done = True
        return

    async def downstream_client():
        results = []
        while not transformer.done or not transformer.queue.empty():
            value = await transformer.queue.get()
            if value is DoneObject:
                break
            results.append(value)
        return results

    _, results = await asyncio.gather(
        transform(input_record_stream()), downstream_client()
    )
    assert_that(results, contains_inanyorder(*[i + 1 for i in items]))
    assert_that(results, has_length(len(items)))


@pytest.mark.asyncio
async def test_concurrent_transformer_worker_cleanup(mocker):
    add = AddOneConcurrently()
    add.thread_pool = mocker.Mock()
    await add.finish()
    add.thread_pool.shutdown.assert_called_once_with(wait=True)


@pytest.mark.asyncio
async def test_concurrent_transformer_flush(mocker):
    async def input_record_stream():
        yield 1
        yield Flush
        yield 2

    add = AddOneConcurrently()
    result = [r async for r in add.handle_async_record_stream(input_record_stream())]
    assert_that(result, contains_inanyorder(2, 3, Flush))
