import asyncio

import pytest

from nodestream.pipeline.channel import channel


@pytest.mark.asyncio
async def test_channel_simple_case_racing():
    input, output = channel(100)

    async def producer():
        for i in range(1000):
            await output.put(i)
        await output.done()

    async def consumer():
        results = []
        while True:
            record = await input.get()
            if record is None:
                break
            results.append(record)

        return results

    results, _ = await asyncio.gather(consumer(), producer())
    assert results == list(range(1000))


@pytest.mark.asyncio
async def test_channel_consumer_done():
    input, output = channel(100)

    async def producer():
        for i in range(1000):
            if not await output.put(i):
                return False

    async def consumer():
        input.done()

    _, result = await asyncio.gather(consumer(), producer())
    assert result is False
