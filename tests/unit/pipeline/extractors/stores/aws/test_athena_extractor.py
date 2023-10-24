from decimal import Decimal

import pytest
from hamcrest import assert_that, equal_to, has_items


@pytest.mark.parametrize(
    "target_row_type,input_value,expected_value",
    [
        ("tinyint", "1", 1),
        ("smallint", "1", 1),
        ("integer", "1", 1),
        ("bigint", "1", 1),
        ("double", "1.123", 1.123),
        ("float", "1.123", 1.123),
        ("decimal", "1.123", Decimal("1.123")),
        ("char", "a", "a"),
        ("string", "a", "a"),
        ("boolean", "true", True),
        ("boolean", "false", False),
        ("varchar", "a", "a"),
        ("some_random_type", "a", "a"),
    ],
)
def test_athena_row_converter(target_row_type, input_value, expected_value):
    from nodestream.pipeline.extractors.stores.aws.athena_extractor import (
        AthenaRowConverter,
    )

    metadata = {"Name": "test", "Type": target_row_type}
    always_the_same_metadata = {"Name": "some_static_value", "Type": "string"}
    converter = AthenaRowConverter([metadata, always_the_same_metadata])
    row = {
        "Data": [{"VarCharValue": input_value}, {"VarCharValue": "some_static_value"}]
    }
    assert_that(
        converter.convert_row(row),
        equal_to(
            {
                "test": expected_value,
                "some_static_value": "some_static_value",
            }
        ),
    )


@pytest.fixture
def athena_extractor(mocker):
    from nodestream.pipeline.extractors.stores.aws.athena_extractor import (
        AthenaExtractor,
    )

    return AthenaExtractor(
        query="select * from some_table",
        database="some_database",
        workgroup="some_workgroup",
        output_location="s3://some_bucket/some_prefix",
        poll_interval_seconds=0,
        page_size=500,
        client=mocker.Mock(),
    )


def test_athena_extractor_get_query_status(athena_extractor):
    athena_extractor.query_execution_id = "some_query_execution_id"
    athena_extractor.client.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {
                "State": "SUCCEEDED",
            },
        },
    }
    assert_that(athena_extractor.get_query_status(), equal_to("SUCCEEDED"))
    athena_extractor.client.get_query_execution.assert_called_once_with(
        QueryExecutionId="some_query_execution_id"
    )


def test_execute_query(athena_extractor):
    athena_extractor.client.start_query_execution.return_value = {
        "QueryExecutionId": "some_query_execution_id",
    }
    athena_extractor.execute_query()
    athena_extractor.client.start_query_execution.assert_called_once_with(
        QueryString="select * from some_table",
        QueryExecutionContext={"Database": "some_database"},
        ResultConfiguration={
            "OutputLocation": "s3://some_bucket/some_prefix",
        },
        WorkGroup="some_workgroup",
    )


def test_await_query_completion(athena_extractor, mocker):
    athena_extractor.get_query_status = mocker.Mock(
        side_effect=["QUEUED", "RUNNING", "SUCCEEDED"]
    )
    athena_extractor.await_query_completion()
    assert_that(athena_extractor.get_query_status.call_count, equal_to(3))


def test_await_query_completion_failed(athena_extractor, mocker):
    with pytest.raises(RuntimeError):
        athena_extractor.get_query_status = mocker.Mock(
            side_effect=["QUEUED", "RUNNING", "FAILED"]
        )
        athena_extractor.await_query_completion()
        assert_that(athena_extractor.get_query_status.call_count, equal_to(3))


def test_get_result_paginator(athena_extractor, mocker):
    athena_extractor.query_execution_id = "some_query_execution_id"
    athena_extractor.client.get_paginator.return_value = mocker.Mock()
    athena_extractor.get_result_paginator()
    athena_extractor.client.get_paginator.assert_called_once_with("get_query_results")


@pytest.mark.asyncio
async def test_extract_records(athena_extractor, mocker):
    athena_extractor.query_execution_id = "some_query_execution_id"
    athena_extractor.execute_query = mocker.Mock()
    athena_extractor.await_query_completion = mocker.Mock()
    athena_extractor.client.get_paginator = mocker.Mock()

    athena_extractor.client.get_paginator.return_value.paginate.return_value = [
        {
            "ResultSet": {
                "ResultSetMetadata": {
                    "ColumnInfo": [
                        {"Name": "col1", "Type": "string"},
                        {"Name": "col2", "Type": "string"},
                    ]
                },
                "Rows": [
                    {"Data": [{"VarCharValue": "col1"}, {"VarCharValue": "col2"}]},
                    {"Data": [{"VarCharValue": "1"}, {"VarCharValue": "2"}]},
                    {"Data": [{"VarCharValue": "3"}, {"VarCharValue": "4"}]},
                ],
            },
        },
        {
            "ResultSet": {
                "ResultSetMetadata": {
                    "ColumnInfo": [
                        {"Name": "col1", "Type": "string"},
                        {"Name": "col2", "Type": "string"},
                    ]
                },
                "Rows": [
                    {"Data": [{"VarCharValue": "5"}, {"VarCharValue": "6"}]},
                    {"Data": [{"VarCharValue": "7"}, {"VarCharValue": "8"}]},
                ],
            },
        },
    ]
    results = [r async for r in athena_extractor.extract_records()]
    expected_results = [
        {"col1": "1", "col2": "2"},
        {"col1": "3", "col2": "4"},
        {"col1": "5", "col2": "6"},
        {"col1": "7", "col2": "8"},
    ]
    assert_that(results, has_items(*expected_results))
