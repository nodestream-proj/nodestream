from dataclasses import dataclass

import pytest
from confluent_kafka import KafkaException
from hamcrest import assert_that, calling, equal_to, not_, raises
from hamcrest.core.core.future import future_raising, resolved

from nodestream.pipeline.extractors.streams import KafkaStreamConnector


@dataclass
class MockKafkaMessage:
    msg: str
    error: str

    def __init__(self, msg, error):
        self.message = msg
        self.error_message = error

    def value(self):
        return self.message

    def partition(self):
        return "test-partition"

    def topic(self):
        return "test-topic"

    def error(self):
        return self.error_message


@pytest.fixture
def connector():
    return KafkaStreamConnector("localhost:9092", "test-topic", max_records=1)


@pytest.mark.asyncio
async def test_connect(connector, mocker):
    mocker.patch("nodestream.pipeline.extractors.streams.kafka.Consumer")

    await connector.connect()
    assert_that(connector.consumer, not_(equal_to(None)))
    connector.consumer.subscribe.assert_called_once()


@pytest.mark.asyncio
async def test_disconnect(connector, mocker):
    connector.consumer = mocker.AsyncMock()
    await connector.disconnect()
    connector.consumer.close.assert_called_once()


@pytest.mark.asyncio
async def test_poll(connector, mocker):
    connector.consumer = mocker.Mock()
    connector.consumer.poll.return_value = MockKafkaMessage("test-value", None)
    result = await connector.poll()
    assert_that(result, equal_to(["test-value"]))


@pytest.mark.asyncio
async def test_poll_error(connector, mocker):
    connector.consumer = mocker.Mock()
    connector.consumer.poll.return_value = MockKafkaMessage(None, "error")
    # check that polling errors don't raise out of the poller
    assert_that(await resolved(connector.poll()), not_(future_raising(KafkaException)))


@pytest.mark.asyncio
async def test_poll_infinite_items_terminates(connector, mocker):
    connector.consumer = mocker.Mock()
    connector.consumer.poll.return_value = MockKafkaMessage("test-value", None)
    connector.max_records = 10
    result = await connector.poll()
    assert_that(result, equal_to(["test-value"] * 10))


@pytest.mark.asyncio
async def test_process_message(connector, mocker):
    connector.consumer = mocker.Mock()
    test_msg = MockKafkaMessage("test-message", None)
    assert_that(connector.process_message(test_msg), equal_to("test-message"))


@pytest.mark.asyncio
async def test_process_message_error(connector, mocker):
    connector.consumer = mocker.Mock()
    test_msg = MockKafkaMessage(None, "error")
    assert_that(
        calling(connector.process_message).with_args(test_msg), raises(KafkaException)
    )
