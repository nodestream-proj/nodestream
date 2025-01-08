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
        self.tentative_parameters = {
            "TableName": table_name,
            "Limit": limit,
            "ScanFilter": scan_filter,
            "ProjectionExpression": projection_expression,
            "FilterExpression": filter_expression,
        }

        self.effective_parameters = self.create_effective_parameters()

    def create_effective_parameters(self):
        effective_parameters = {}
        for key, value in self.tentative_parameters.items():
            if value:
                effective_parameters[key] = value
        return effective_parameters

    def scan_table(self):
        should_continue = True
        while should_continue:
            response: dict = self.client.scan(**self.effective_parameters)
            yield from response["Items"]
            last_evaluated_key = response.get("LastEvaluatedKey", None)
            if last_evaluated_key is not None:
                should_continue = True
                self.effective_parameters.update(
                    {"ExclusiveStartKey": last_evaluated_key}
                )
            else:
                should_continue = False

    async def extract_records(self) -> AsyncGenerator[Any, Any]:
        for record in self.scan_table():
            yield DynamoRecord.from_raw_dynamo_record(record).record_data
