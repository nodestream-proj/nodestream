import pytest
from testcontainers.kafka import KafkaContainer

import pytest_asyncio
from aiokafka.producer import AIOKafkaProducer

from nodestream.pipeline.extractors.streams import StreamExtractor, KafkaStreamConnector


@pytest.fixture
def kakfa_container():
    with KafkaContainer() as kafka:
        yield kafka


@pytest_asyncio.fixture
async def kakfa_producer(kakfa_container):
    producer = AIOKafkaProducer(
        bootstrap_servers=kakfa_container.get_bootstrap_server()
    )
    await producer.start()
    yield producer
    await producer.stop()


@pytest.fixture
def produce_records_on_topic(kakfa_producer):
    async def _produce_records_on_topic(topic, records):
        for record in records:
            await kakfa_producer.send_and_wait(topic, record)
        await kakfa_producer.flush()

    return _produce_records_on_topic


@pytest.fixture
def connector(kakfa_container):
    return KafkaStreamConnector(
        bootstrap_servers=[kakfa_container.get_bootstrap_server()]
    )


async def take_until_flush(extractor):
    records = []
    async for record in extractor.extract_records():
        

@pytest.mark.asyncio
async def test_kafka_stream_connector(connector, produce_records_on_topic):
    topic = "test_topic"
    records = [{"i": i} for i in range(10)]
    await produce_records_on_topic(topic, records)
    