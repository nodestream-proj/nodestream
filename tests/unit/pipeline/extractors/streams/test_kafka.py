from unittest.mock import MagicMock, create_autospec, patch
import aiokafka

import pytest
from aiokafka import ConsumerRecord
from hamcrest import assert_that, equal_to, not_

from nodestream.pipeline.extractors.streams import KafkaStreamConnector


@pytest.fixture
def connector():
    return KafkaStreamConnector(
        "localhost:9092", "test-topic", max_records=1, poll_timeout_ms=10000
    )


@pytest.mark.asyncio
async def test_connect(connector, mocker):
    mocker.patch("nodestream.pipeline.extractors.streams.kafka.AIOKafkaConsumer.start")

    await connector.connect()
    assert_that(connector.consumer, not_(equal_to(None)))
    connector.consumer.start.assert_called_once()


@pytest.mark.asyncio
async def test_disconnect(connector, mocker):
    connector.consumer = mocker.AsyncMock()
    await connector.disconnect()
    connector.consumer.stop.assert_called_once()


@pytest.mark.asyncio
async def test_poll(connector, mocker):
    async def iterator(record):
        yield record
    test_record = ConsumerRecord(
        key="",
        topic="test-topic",
        value="test-value",
        partition=None,
        offset=0,
        timestamp=0,
        timestamp_type=1,
        checksum=1,
        serialized_key_size=30,
        serialized_value_size=30,
        headers=None,
    )
    connector.consumer = iterator(test_record)
    result = await connector.poll()
    assert_that(result, equal_to(["test-value"]))
