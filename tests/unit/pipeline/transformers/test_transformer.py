import asyncio

import pytest
from hamcrest import assert_that, contains_inanyorder, has_length

from nodestream.pipeline import Flush
from nodestream.pipeline.transformers import (
    ConcurrentTransformer,
    PassTransformer,
    SwitchTransformer,
    Transformer,
)
from nodestream.pipeline.value_providers import JmespathValueProvider


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


class addNTransformer(Transformer):
    def __init__(self, N):
        self.n = N

    async def transform_record(self, record):
        yield dict(type=record["type"], value=record["value"] + self.n)


TEST_PROVIDER = JmespathValueProvider.from_string_expression("type")
TEST_CASES = {
    "first": {
        "implementation": "tests.unit.pipeline.transformers.test_transformer:addNTransformer",
        "arguments": {"N": 1},
    },
    "second": {
        "implementation": "tests.unit.pipeline.transformers.test_transformer:addNTransformer",
        "arguments": {"N": 2},
    },
}
DEFAULT_CASE = {
    "implementation": "tests.unit.pipeline.transformers.test_transformer:addNTransformer",
    "arguments": {"N": 3},
}

TEST_DATA = [
    {"type": "first", "value": 0},
    {"type": "second", "value": 0},
    {"type": "third", "value": 0},
]

TEST_RESULTS_WITH_DEFAULT = [
    {"type": "first", "value": 1},
    {"type": "second", "value": 2},
    {"type": "third", "value": 3},
]

TEST_RESULTS_WITH_NO_DEFAULT = [
    {"type": "first", "value": 1},
    {"type": "second", "value": 2},
    {"type": "third", "value": 0},
]


async def switch_input_record_stream():
    for record in TEST_DATA:
        yield record


@pytest.mark.asyncio
async def test_switch_transformer_with_default():
    switch_transformer = SwitchTransformer.from_file_data(
        switch_on=TEST_PROVIDER, cases=TEST_CASES, default=DEFAULT_CASE
    )

    results = [
        r
        async for r in switch_transformer.handle_async_record_stream(
            switch_input_record_stream()
        )
    ]
    assert results == TEST_RESULTS_WITH_DEFAULT


@pytest.mark.asyncio
async def test_switch_transformer_without_default():
    switch_transformer = SwitchTransformer.from_file_data(
        switch_on=TEST_PROVIDER, cases=TEST_CASES
    )
    results = [
        r
        async for r in switch_transformer.handle_async_record_stream(
            switch_input_record_stream()
        )
    ]
    assert results == TEST_RESULTS_WITH_NO_DEFAULT
