import copy

import boto3
import pytest
from hamcrest import assert_that, equal_to
from moto import mock_aws

from nodestream.pipeline.extractors.stores.aws.dynamodb_extractor import (
    DynamoDBExtractor,
    DynamoRecord,
)

DESCRIPTOR_HANDLER_TESTS = [
    ({"S": "TestString"}, "TestString"),
    ({"N": "100"}, 100),
    ({"N": "1.25"}, 1.25),
    ({"B": bytes(b"a31n3c3")}, b"a31n3c3"),
    ({"NULL": True}, None),
    ({"NULL": False}, None),
    ({"BOOL": False}, False),
    ({"BOOL": True}, True),
    ({"SS": ["StringA", "StringB"]}, {"StringA", "StringB"}),
    ({"NS": ["100", "200", "1.0"]}, {100, 200, 1.0}),
    ({"BS": [bytes(b"abcdef"), bytes(b"xyz123")]}, {b"abcdef", b"xyz123"}),
    (
        {"M": {"key1": {"S": "TestString"}, "key2": {"NS": ["100", "1.0"]}}},
        {"key1": "TestString", "key2": {100, 1.0}},
    ),
    (
        {"L": [{"S": "TestString"}, {"BS": [bytes(b"abcdef"), bytes(b"xyz123")]}]},
        ["TestString", {b"abcdef", b"xyz123"}],
    ),
]


@pytest.mark.parametrize("dynamo_data,result", DESCRIPTOR_HANDLER_TESTS)
def test_data_unmarshaller(dynamo_data, result):
    dynamo_data = DynamoRecord.from_raw_dynamo_record({"key": dynamo_data})
    assert dynamo_data.record_data == {"key": result}


TEST_DYNAMO_OBJECT_A = {"N": "100"}
TEST_DYNAMO_OBJECT_B = {
    "M": {"key1": {"S": "TestString"}, "key2": {"NS": ["100", "1.0"]}}
}

TEST_DYNAMO_RECORD = {"number": TEST_DYNAMO_OBJECT_A, "metadata": TEST_DYNAMO_OBJECT_B}


def test_dynamo_record_class_initialization_from_raw_data():
    dynamo_record = DynamoRecord.from_raw_dynamo_record(TEST_DYNAMO_RECORD)
    assert dynamo_record.record_data == {
        "number": 100,
        "metadata": {"key1": "TestString", "key2": {100, 1.0}},
    }


def test_dynamo_extractor_initialization_without_args(mocker):
    client_mock = mocker.patch(
        "nodestream.pipeline.extractors.credential_utils.AwsClientFactory.make_client"
    )
    extractor = DynamoDBExtractor.from_file_data(
        "table_name",
    )
    assert extractor.effective_parameters == {
        "TableName": "table_name",
        "PaginationConfig": {
            "PageSize": 100,
        },
    }
    client_mock.assert_called_once_with("dynamodb")


def test_dynamo_extractor_initialization_with_args(mocker):
    client_mock = mocker.patch(
        "nodestream.pipeline.extractors.credential_utils.AwsClientFactory.make_client"
    )
    extractor = DynamoDBExtractor.from_file_data(
        table_name="table_name",
        scan_filter={
            "number": {
                "AttributeValueList": [{"N": "90"}],
                "ComparisonOperator": "GE",
            }
        },
        projection_expression="test_projection_expression",
        filter_expression="test_filter_expression",
        assume_role_arn="test_role",
        assume_role_external_id="test_arn",
        region_name="test_region",
        limit=100,
    )
    assert extractor.effective_parameters == {
        "TableName": "table_name",
        "ScanFilter": {
            "number": {
                "AttributeValueList": [{"N": "90"}],
                "ComparisonOperator": "GE",
            }
        },
        "ProjectionExpression": "test_projection_expression",
        "FilterExpression": "test_filter_expression",
        "PaginationConfig": {
            "PageSize": 100,
        },
    }
    client_mock.assert_called_once_with("dynamodb")


TEST_DATA = [
    {"number": {"N": "100"}},
    {"number": {"N": "101"}},
    {"number": {"N": "102"}},
    {"number": {"N": "103"}},
    {"number": {"N": "104"}},
    {"number": {"N": "105"}},
]

TEST_RESULTS = [
    {"number": 100},
    {"number": 101},
    {"number": 102},
    {"number": 103},
    {"number": 104},
    {"number": 105},
]


@pytest.fixture
def mock_dynamodb_client(mocker):
    return mocker.MagicMock()


@pytest.fixture
def dynamodb_extractor(mock_dynamodb_client):
    return DynamoDBExtractor(
        client=mock_dynamodb_client,
        table_name="test_table",
        limit=100,
        scan_filter={},
        projection_expression=None,
        filter_expression=None,
    )


def test_dynamodb_extractor_scan_table(
    mocker, dynamodb_extractor, mock_dynamodb_client
):
    mock_paginator = mocker.MagicMock()
    mock_dynamodb_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [
        {"Items": [{"attribute1": {"S": "value1"}}]},
        {"Items": [{"attribute2": {"N": "123"}}]},
    ]

    items = list(dynamodb_extractor.scan_table())
    assert items == [{"attribute1": {"S": "value1"}}, {"attribute2": {"N": "123"}}]


@pytest.mark.asyncio
async def test_dynamodb_extractor_extract_records(
    mocker, dynamodb_extractor, mock_dynamodb_client
):
    mock_paginator = mocker.MagicMock()
    mock_dynamodb_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [
        {"Items": [{"attribute1": {"S": "value1"}}]},
        {"Items": [{"attribute2": {"N": "123"}}]},
    ]

    records = [record async for record in dynamodb_extractor.extract_records()]
    assert records == [{"attribute1": "value1"}, {"attribute2": 123}]


TABLE_NAME = "test_table"
LARGE_TEST_DATA = [{"number": {"N": str(i)}} for i in range(1, 201)]


@pytest.fixture
def dynamodb_client():
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-west-2")
        client.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {"AttributeName": "number", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "number", "AttributeType": "N"},
            ],
            ProvisionedThroughput={
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 5,
            },
        )
        for item in LARGE_TEST_DATA:
            client.put_item(TableName=TABLE_NAME, Item=item)
        yield client


async def fetch_records(geneator, n=None):
    records = []
    async for record in geneator:
        records.append(record)
        if n and len(records) == n:
            break
    return records


@pytest.mark.asyncio
async def test_dynamodb_extractor_pagination_and_checkpoint(dynamodb_client):
    extractor = DynamoDBExtractor(
        client=dynamodb_client, table_name=TABLE_NAME, limit=25, scan_filter={}
    )
    copied_extractor = copy.copy(extractor)

    # Extract first batch of records
    first_batch = await fetch_records(extractor.extract_records(), 50)

    # Make a checkpoint
    checkpoint = await extractor.make_checkpoint()
    assert checkpoint["last_evaluated_key"] is not None

    # Resume from checkpoint
    await copied_extractor.resume_from_checkpoint(checkpoint)

    # Get the rest of the records
    result = await fetch_records(copied_extractor.extract_records())
    all_records = first_batch + result

    assert_that(len(all_records), equal_to(200))
