from logging import getLogger
from typing import Any, AsyncGenerator, Dict, Optional

from boto3.dynamodb.types import TypeDeserializer

from ...credential_utils import AwsClientFactory
from ...extractor import Extractor


class DynamoRecord:
    """
    Collection of attributes and their data.
    """

    @classmethod
    def from_raw_dynamo_record(cls, raw_dynamo_entry: Dict[str, Dict[str, Any]]):

        return DynamoRecord(
            {
                attribute_name: TypeDeserializer().deserialize(attribute_value)
                for attribute_name, attribute_value in raw_dynamo_entry.items()
            }
        )

    def __init__(self, record_data: Dict[str, Any]):
        self.record_data = record_data


class DynamoDBExtractor(Extractor):
    @classmethod
    def from_file_data(
        cls,
        table_name: str,
        limit: int = 100,
        scan_filter: Dict[str, Any] = {},
        projection_expression: str = None,
        filter_expression: str = None,
        **aws_client_args,
    ):
        client = AwsClientFactory(**aws_client_args).make_client("dynamodb")
        return cls(
            client=client,
            table_name=table_name,
            limit=limit,
            scan_filter=scan_filter,
            projection_expression=projection_expression,
            filter_expression=filter_expression,
        )

    def __init__(
        self,
        client,
        table_name,
        limit: int,
        scan_filter: Dict[str, Any],
        projection_expression: Optional[str] = None,
        filter_expression: Optional[str] = None,
    ) -> None:
        self.client = client
        self.logger = getLogger(self.__class__.__name__)
        tentative_parameters = {
            "TableName": table_name,
            "ScanFilter": scan_filter,
            "ProjectionExpression": projection_expression,
            "FilterExpression": filter_expression,
            "PaginationConfig": {
                "PageSize": limit,
            },
        }

        self.effective_parameters = {
            key: value for key, value in tentative_parameters.items() if value
        }

    def scan_table(self):
        paginator = self.client.get_paginator("scan")
        pages = paginator.paginate(**self.effective_parameters)
        for page in pages:
            yield from page["Items"]

    async def extract_records(self) -> AsyncGenerator[Any, Any]:
        for record in self.scan_table():
            yield DynamoRecord.from_raw_dynamo_record(record).record_data
