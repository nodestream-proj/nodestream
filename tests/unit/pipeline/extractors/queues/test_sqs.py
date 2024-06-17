from dataclasses import dataclass

import pytest

from moto import mock_sqs
from pytest_mock import mocker

from nodestream.pipeline.extractors.credential_utils import AwsClientFactory
from nodestream.pipeline.extractors.queues.sqs import SQSQueueConnector

@pytest.fixture
def mock_client():
    with mock_sqs():
        import boto3
        yield boto3.client('sqs', region_name='us-east-1')


@mock_sqs
@pytest.fixture
def sqs_queue(mock_client):
    response = mock_client.create_queue(QueueName='queue1')
    url = response['QueueUrl']
    mock_client.send_message(QueueUrl=url, MessageBody='test-message-1')
    mock_client.send_message(QueueUrl=url, MessageBody='test-message-2')
    yield url

@mock_sqs
@pytest.fixture
def subject(sqs_queue, mock_client):
    return SQSQueueConnector(
        queue_url=sqs_queue,
        max_batch_size=10,
        max_batches=10,
        sqs_client=mock_client,
    )

@pytest.mark.asyncio
async def test_poll(subject):
    results = await subject.poll()
    assert results == ['test-message-1', 'test-message-2']

@pytest.mark.asyncio
async def test_poll_batch_1(subject):
    subject.max_batch_size = 1
    results = await subject.poll()
    assert results == ['test-message-1']
    results = await subject.poll()
    assert results == ['test-message-2']
    assert await subject.poll() == []

@pytest.mark.asyncio
async def test_poll_delete_false(subject):
    subject.max_batch_size = 10
    subject.max_batches = 1
    subject.delete_after_read = False
    subject.sqs_client.set_queue_attributes(
        QueueUrl=subject.queue_url,
        Attributes={
            'VisibilityTimeout': '0'
        }
    )
    results = await subject.poll()
    assert results == ['test-message-1', 'test-message-2']
    results = await subject.poll()
    assert results == ['test-message-1', 'test-message-2']
