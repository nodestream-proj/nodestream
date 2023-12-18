import asyncio

import pytest
from hamcrest import assert_that, contains_inanyorder, has_length

from nodestream.pipeline import Flush
from nodestream.pipeline.transformers import ConcurrentTransformer


class AddOneConcurrently(ConcurrentTransformer):
    def __init__(self):
        self.done = False
        self.queue_size = 0
        super().__init__()

    async def transform_record(self, record):
        return record + 1


class AddOneConcurrentlyGreedy(AddOneConcurrently):
    async def yield_processor(self):
        pass


ITEM_COUNT = 100


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
async def test_greedy_concurrent_transformer_does_not_pass_processor():
    items = list(range(ITEM_COUNT))
    transformer = AddOneConcurrentlyGreedy()

    async def input_record_stream():
        for i in items:
            yield i

    async def transform():
        async for _ in transformer.handle_async_record_stream(input_record_stream()):
            transformer.queue_size += 1
        transformer.done = True

    async def downstream_client():
        should_continue = True
        while should_continue:
            if transformer.queue_size > 0:
                assert transformer.queue_size >= ITEM_COUNT
            transformer.queue_size -= 1
            should_continue = not transformer.done
            await asyncio.sleep(0)

    await asyncio.gather(transform(), downstream_client())


@pytest.mark.asyncio
async def test_normal_concurrent_transformer_passes_processor():
    items = list(range(ITEM_COUNT))
    transformer = AddOneConcurrently()

    async def input_record_stream():
        for i in items:
            yield i

    async def transform():
        async for _ in transformer.handle_async_record_stream(input_record_stream()):
            transformer.queue_size += 1
        transformer.done = True

    async def downstream_client():
        should_continue = True
        while should_continue:
            assert transformer.queue_size < ITEM_COUNT
            transformer.queue_size -= 1
            should_continue = not transformer.done
            await asyncio.sleep(0)

    await asyncio.gather(transform(), downstream_client())


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
