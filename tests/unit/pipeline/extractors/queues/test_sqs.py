from unittest.mock import patch

import pytest
from moto import mock_aws

from nodestream.pipeline.extractors.queues.sqs import SQSQueueConnector


@pytest.fixture
def mock_client():
    with mock_aws():
        import boto3

        yield boto3.client("sqs", region_name="us-east-1")


@pytest.fixture
def sqs_queue(mock_client):
    with mock_aws():
        response = mock_client.create_queue(QueueName="queue1")
        url = response["QueueUrl"]
        mock_client.send_message(QueueUrl=url, MessageBody="test-message-1")
        mock_client.send_message(QueueUrl=url, MessageBody="test-message-2")
    return url


@pytest.fixture
def sqs_queue_no_messages(mock_client):
    with mock_aws():
        response = mock_client.create_queue(QueueName="queue1")
    url = response["QueueUrl"]
    return url


@pytest.fixture
def subject(sqs_queue, mock_client):
    with mock_aws():
        yield SQSQueueConnector(
            queue_url=sqs_queue,
            max_batch_size=10,
            max_batches=10,
            sqs_client=mock_client,
        )


@pytest.fixture
def subject_no_messages(sqs_queue_no_messages, mock_client):
    with mock_aws():
        yield SQSQueueConnector(
            queue_url=sqs_queue_no_messages,
            max_batch_size=10,
            max_batches=10,
            sqs_client=mock_client,
        )


@pytest.mark.asyncio
@patch("nodestream.pipeline.extractors.queues.sqs.AwsClientFactory")
async def test_from_file_data_poll(mock_aws_client_factory, sqs_queue, mock_client):
    mock_aws_client_factory_instance = mock_aws_client_factory.return_value
    mock_aws_client_factory_instance.make_client.return_value = mock_client

    queue_url = sqs_queue
    message_system_attribute_names = ["SenderId", "SentTimestamp"]
    message_attribute_names = ["CustomAttributeName"]
    max_batch_size = 5
    delete_after_read = True
    max_batches = 5
    aws_client_args = {"region_name": "us-east-1"}

    # Invoke the from_file_data method to get an instance of SQSQueueConnector
    sqs_connector = SQSQueueConnector.from_file_data(
        queue_url=queue_url,
        message_system_attribute_names=message_system_attribute_names,
        message_attribute_names=message_attribute_names,
        max_batch_size=max_batch_size,
        delete_after_read=delete_after_read,
        max_batches=max_batches,
        **aws_client_args,
    )

    # Verify that the properties are correctly assigned
    assert sqs_connector.queue_url == queue_url
    assert (
        sqs_connector.message_system_attribute_names == message_system_attribute_names
    )
    assert sqs_connector.message_attribute_names == message_attribute_names
    assert sqs_connector.max_batch_size == max_batch_size
    assert sqs_connector.delete_after_read == delete_after_read
    assert sqs_connector.max_batches == max_batches

    mock_aws_client_factory.assert_called_once_with(**aws_client_args)
    mock_aws_client_factory_instance.make_client.assert_called_once_with("sqs")

    # Verify that the client works as expected for poll.
    assert sqs_connector.sqs_client is mock_client
    results = await sqs_connector.poll()
    assert results == ["test-message-1", "test-message-2"]


@pytest.mark.asyncio
async def test_poll(subject):
    results = await subject.poll()
    assert results == ["test-message-1", "test-message-2"]


@pytest.mark.asyncio
async def test_poll_batch_1(subject):
    subject.max_batch_size = 1
    subject.max_batches = 1
    results = await subject.poll()
    assert results == ["test-message-1"]
    results = await subject.poll()
    assert results == ["test-message-2"]
    assert await subject.poll() == []


@pytest.mark.asyncio
async def test_poll_delete_false(subject):
    subject.max_batch_size = 10
    subject.max_batches = 1
    subject.delete_after_read = False
    subject.sqs_client.set_queue_attributes(
        QueueUrl=subject.queue_url, Attributes={"VisibilityTimeout": "0"}
    )
    results = await subject.poll()
    assert results == ["test-message-1", "test-message-2"]
    results = await subject.poll()
    assert results == ["test-message-1", "test-message-2"]


@pytest.mark.asyncio
async def test_poll_no_messages(subject_no_messages):
    subject_no_messages.max_batch_size = 10
    subject_no_messages.max_batches = 1
    subject_no_messages.delete_after_read = False
    subject_no_messages.sqs_client.set_queue_attributes(
        QueueUrl=subject_no_messages.queue_url, Attributes={"VisibilityTimeout": "0"}
    )
    results = await subject_no_messages.poll()
    assert results == []
