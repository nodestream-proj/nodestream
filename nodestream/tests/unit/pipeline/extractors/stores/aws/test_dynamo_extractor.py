from unittest.mock import patch

import pytest

from nodestream.pipeline.extractors.stores.aws.dynamodb_extractor import (
    ATTRIBUTE_TYPE_DESCRIPTOR_HANDLERS,
    DynamoData,
    DynamoDBExtractor,
    DynamoRecord,
    convert_array,
    convert_map,
    convert_scalar,
    format_boolean,
    format_bytes,
    format_nested_value,
    format_null,
    format_number,
    leave_untouched,
)


class TestObject:
    pass


def test_leave_untouched():
    assert leave_untouched(TestObject) == TestObject


def test_format_number():
    assert format_number("100") == 100
    assert format_number("1.25") == 1.25


def test_format_nested_value():
    assert format_nested_value({"S": "SomeString"}) == "SomeString"


def test_format_boolean():
    assert format_boolean("True") is True
    assert format_boolean("False") is False


def test_format_null():
    assert format_null("True") is None
    assert format_null("False") == "False"


def test_format_bytes():
    assert format_bytes("100") == b"100"
    assert format_bytes("a31n3c3") == b"a31n3c3"


def test_convert_scalar():
    assert convert_scalar(TestObject, leave_untouched) == TestObject


def test_convert_array():
    assert convert_array([TestObject, TestObject, TestObject], leave_untouched) == [
        TestObject,
        TestObject,
        TestObject,
    ]


def test_convert_map():
    assert convert_map({"key1": TestObject, "key2": TestObject}, leave_untouched) == {
        "key1": TestObject,
        "key2": TestObject,
    }


DESCRIPTOR_HANDLER_TESTS = [
    ({"S": "TestString"}, "TestString"),
    ({"N": "100"}, 100),
    ({"N": "1.25"}, 1.25),
    ({"B": "a31n3c3"}, b"a31n3c3"),
    ({"NULL": "True"}, None),
    ({"NULL": "False"}, "False"),
    ({"BOOL": "True"}, True),
    ({"BOOL": "False"}, False),
    ({"SS": ["StringA", "StringB"]}, ["StringA", "StringB"]),
    ({"NS": ["100", "200", "1.0"]}, [100, 200, 1.0]),
    ({"BS": ["abcdef", "xyz123"]}, [b"abcdef", b"xyz123"]),
    (
        {"M": {"key1": {"S": "TestString"}, "key2": {"NS": ["100", "1.0"]}}},
        {"key1": "TestString", "key2": [100, 1.0]},
    ),
    (
        {"L": [{"S": "TestString"}, {"BS": ["abcdef", "xyz123"]}]},
        ["TestString", [b"abcdef", b"xyz123"]],
    ),
]


@pytest.mark.parametrize("dynamo_data,result", DESCRIPTOR_HANDLER_TESTS)
def test_descriptor_handler(dynamo_data, result):
    data_type, raw_data_value = list(dynamo_data.items())[0]
    result_handler, type_handler = ATTRIBUTE_TYPE_DESCRIPTOR_HANDLERS[data_type]
    assert result_handler(raw_data_value, type_handler) == result


TEST_DYNAMO_OBJECT_A = {"N": "100"}
TEST_DYNAMO_OBJECT_B = {
    "M": {"key1": {"S": "TestString"}, "key2": {"NS": ["100", "1.0"]}}
}


def test_dynamo_data_class_initialization_from_raw_data():
    dynamo_data_a = DynamoData.from_raw_dynamo_data(TEST_DYNAMO_OBJECT_A)
    assert dynamo_data_a.data_type == "N"
    assert dynamo_data_a.raw_data_value == "100"
    assert dynamo_data_a.python_format == 100
    dynamo_data_b = DynamoData.from_raw_dynamo_data(TEST_DYNAMO_OBJECT_B)
    assert dynamo_data_b.data_type == "M"
    assert dynamo_data_b.raw_data_value == {
        "key1": {"S": "TestString"},
        "key2": {"NS": ["100", "1.0"]},
    }
    assert dynamo_data_b.python_format == {"key1": "TestString", "key2": [100, 1.0]}


TEST_DYNAMO_RECORD = {"number": TEST_DYNAMO_OBJECT_A, "metadata": TEST_DYNAMO_OBJECT_B}


def test_dynamo_record_class_initialization_from_raw_data():
    dynamo_record = DynamoRecord.from_raw_dynamo_record(TEST_DYNAMO_RECORD)
    assert dynamo_record.record_data == {
        "number": DynamoData.from_raw_dynamo_data(TEST_DYNAMO_OBJECT_A),
        "metadata": DynamoData.from_raw_dynamo_data(TEST_DYNAMO_OBJECT_B),
    }
    assert dynamo_record.python_format == {
        "number": 100,
        "metadata": {"key1": "TestString", "key2": [100, 1.0]},
    }


def test_dynamo_extractor_initialization_without_args():
    with patch(
        "nodestream.pipeline.extractors.credential_utils.AwsClientFactory.make_client"
    ) as client_mock:
        extractor = DynamoDBExtractor.from_file_data(
            "table_name",
        )
        assert extractor.effective_parameters == {
            "TableName": "table_name",
            "Limit": 100,
        }
        client_mock.assert_called_once_with("dynamodb")


def test_dynamo_extractor_initialization_with_args():
    with patch(
        "nodestream.pipeline.extractors.credential_utils.AwsClientFactory.make_client"
    ) as client_mock:
        extractor = DynamoDBExtractor.from_file_data(
            table_name="table_name",
            limit=100,
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
        )
        assert extractor.effective_parameters == {
            "TableName": "table_name",
            "Limit": 100,
            "ScanFilter": {
                "number": {
                    "AttributeValueList": [{"N": "90"}],
                    "ComparisonOperator": "GE",
                }
            },
            "ProjectionExpression": "test_projection_expression",
            "FilterExpression": "test_filter_expression",
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


class MockResponder:
    def __init__(self, items):
        self.items = items
        self.times_called = 0

    def scan(self, **parameters):
        self.times_called += 1
        index = 0
        if "ExclusiveStartKey" in parameters:
            index = parameters["ExclusiveStartKey"]
        limit = parameters["Limit"]
        return_index = index + limit

        response = {}
        if return_index < len(self.items):
            response.update({"LastEvaluatedKey": return_index})

        # If we have records remaining we provide the "LastEvaluatedKey"
        response.update({"Items": self.items[index:return_index]})
        return response


def test_dynamo_extractor_table_scanner():
    extractor = DynamoDBExtractor(
        client=MockResponder(TEST_DATA),
        table_name="table_name",
        limit=2,
        scan_filter={},
        projection_expression=None,
        filter_expression=None,
    )
    result = [item for item in extractor.scan_table()]
    assert extractor.client.times_called == 3
    assert result == TEST_DATA


@pytest.mark.asyncio
async def test_dynamo_extractor_extract_records():
    extractor = DynamoDBExtractor(
        client=MockResponder(TEST_DATA),
        table_name="table_name",
        limit=2,
        scan_filter={},
        projection_expression=None,
        filter_expression=None,
    )
    result = [item async for item in extractor.extract_records()]
    assert extractor.client.times_called == 3
    assert result == TEST_RESULTS
