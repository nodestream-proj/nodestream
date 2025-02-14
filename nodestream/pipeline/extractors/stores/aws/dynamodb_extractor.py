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
        self.table_name = table_name
        self.limit = limit
        self.scan_filter = scan_filter
        self.projection_expression = projection_expression
        self.filter_expression = filter_expression
        self.last_evaluated_key = None

    @property
    def effective_parameters(self):
        tentative_parameters = {
            "TableName": self.table_name,
            "ScanFilter": self.scan_filter,
            "ProjectionExpression": self.projection_expression,
            "FilterExpression": self.filter_expression,
            "ExclusiveStartKey": self.last_evaluated_key,
            "PaginationConfig": {
                "PageSize": self.limit,
            },
        }

        return {key: value for key, value in tentative_parameters.items() if value}

    def scan_table(self):
        paginator = self.client.get_paginator("scan")
        pages = paginator.paginate(**self.effective_parameters)
        for page in pages:
            self.last_evaluated_key = page.get("LastEvaluatedKey")
            yield from page["Items"]

    async def extract_records(self) -> AsyncGenerator[Any, Any]:
        for record in self.scan_table():
            yield DynamoRecord.from_raw_dynamo_record(record).record_data

    async def resume_from_checkpoint(self, checkpoint_object):
        self.last_evaluated_key = checkpoint_object.get("last_evaluated_key")

    async def make_checkpoint(self):
        return {"last_evaluated_key": self.last_evaluated_key}
