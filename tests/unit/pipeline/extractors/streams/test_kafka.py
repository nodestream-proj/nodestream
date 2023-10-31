import pytest
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
    connector.consumer = mocker.AsyncMock()
    connector.consumer.getmany.return_value = {
        mocker.Mock(topic="test-topic", partition=0): [mocker.Mock(value="test-value")]
    }
    results = [record for record in await connector.poll()]
    assert_that([res.value for res in results], equal_to(["test-value"]))
    connector.consumer.getmany.assert_called_once_with(timeout_ms=10000)
