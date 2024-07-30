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


@pytest.mark.asyncio
async def test_channel_times_out_waiting_for_space():
    input, output = channel(10)

    async def producer():
        results = [await output.put(i) for i in range(12)]
        return any(r is False for r in results)

    async def consumer():
        # wait until the producer has filled the queue to capacity
        # so that we can be sure that the producer is blocked
        # waiting for space in the queue
        while input.channel.queue.qsize() < 10:
            await asyncio.sleep(0.1)

        # Be extra sure that the producer is blocked by waiting
        # for a bit longer before calling done on the input channel
        await asyncio.sleep(0.5)

        # Call done so the input channel will give up waiting for space
        input.done()

    assert await asyncio.gather(consumer(), producer()) == [None, True]
