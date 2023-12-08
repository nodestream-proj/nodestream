import pytest
from hamcrest import assert_that, contains_inanyorder, has_length

from nodestream.pipeline import Flush
from nodestream.pipeline.transformers import ConcurrentTransformer


class AddOneConcurrently(ConcurrentTransformer):
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
