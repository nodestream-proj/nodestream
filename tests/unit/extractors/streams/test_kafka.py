import pytest
from hamcrest import assert_that, equal_to, not_

from nodestream.extractors.streams import KafkaStreamConnector


@pytest.fixture
def connector():
    return KafkaStreamConnector("localhost:9092", "test-topic")


@pytest.mark.asyncio
async def test_connect(connector, mocker):
    mocker.patch("nodestream.extractors.streams.kafka.AIOKafkaConsumer.start")

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
    connector.consumer = mocker.AsyncMock()
    connector.consumer.getmany.return_value = {
        mocker.Mock(topic="test-topic", partition=0): [mocker.Mock(value="test-value")]
    }
    result = [record async for record in connector.poll(1, 1)]
    assert_that(result, equal_to(["test-value"]))
    connector.consumer.getmany.assert_called_once_with(max_records=1, timeout_ms=1000)
