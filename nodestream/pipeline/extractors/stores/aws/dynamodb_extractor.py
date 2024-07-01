from logging import getLogger
from typing import Any, AsyncGenerator, Dict, List, Optional

from ...credential_utils import AwsClientFactory
from ...extractor import Extractor

"""
    Additional Type Format Handlers
"""


def leave_untouched(value: Any):
    return value


def format_number(value: str):
    if "." in value:
        return float(value)
    return int(value)


# Nested data that are not declared by a set will maintain their Dynamo format of { data_type: raw_data_value }
def format_nested_value(object: Dict[str, Any]):
    return DynamoData.from_raw_dynamo_data(object).python_format


def format_boolean(value: str):
    return value.lower() == "true"


def format_bytes(value: str):
    return value.encode("utf-8")


# Null objects come in the form {"NULL": "True"} or {"NULL": "False"}.
def format_null(value: str):
    return None if format_boolean(value) else value


"""
    Result Handlers
"""


def convert_scalar(value: Any, type_handler):
    return type_handler(value)


def convert_array(array: List[Any], type_handler):
    return [type_handler(value) for value in array]


def convert_map(mapping: Dict[str, Any], type_handler):
    return {key: type_handler(value) for key, value in mapping.items()}


DEFAULT_HANDLER = (convert_scalar, leave_untouched)

ATTRIBUTE_TYPE_DESCRIPTOR_HANDLERS = {
    "S": (convert_scalar, leave_untouched),  # String format
    "N": (convert_scalar, format_number),  # Number format (int or float)
    "B": (convert_scalar, format_bytes),  # Bytes format
    "NULL": (convert_scalar, format_null),  # Null format
    "BOOL": (convert_scalar, format_boolean),  # Boolean format
    "SS": (convert_array, leave_untouched),  # Set of Strings will be a List[str]
    "NS": (convert_array, format_number),  # Set of Numbers will be a List[str]
    "BS": (convert_array, format_bytes),  # Set of Bytes will be a List[str]
    "M": (convert_map, format_nested_value),  # Map will be a Dict[DynamoData]
    "L": (convert_array, format_nested_value),  # List will be a List[DynamoData]
}


class DynamoData:
    """
    Dynamo Data object is defined by the following key: value pair:
        {data_type: raw_data_value}
    """

    @classmethod
    def from_raw_dynamo_data(cls, raw_dynamo_attribute: Dict[str, Any]):
        return DynamoData(*list(raw_dynamo_attribute.items())[0])

    def __init__(self, data_type: str, raw_data_value: Any):
        self.data_type = data_type
        self.raw_data_value = raw_data_value

    @property
    def python_format(self) -> Any:
        result_handler, type_handler = ATTRIBUTE_TYPE_DESCRIPTOR_HANDLERS.get(
            self.data_type, DEFAULT_HANDLER
        )
        return result_handler(self.raw_data_value, type_handler)

    def __eq__(self, other: "DynamoData") -> bool:
        return (
            self.data_type == other.data_type
            and self.raw_data_value == other.raw_data_value
        )


class DynamoRecord:
    """
    Collection of attributes and their data.
    """

    @classmethod
    def from_raw_dynamo_record(cls, raw_dynamo_entry: Dict[str, Dict[str, Any]]):
        return DynamoRecord(
            {
                attribute_name: DynamoData.from_raw_dynamo_data(attribute_value)
                for attribute_name, attribute_value in raw_dynamo_entry.items()
            }
        )

    def __init__(self, record_data: Dict[str, DynamoData]):
        self.record_data = record_data

    @property
    def python_format(self) -> Dict[str, Any]:
        return {
            attribute_name: attribute_value.python_format
            for attribute_name, attribute_value in self.record_data.items()
        }


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
            yield DynamoRecord.from_raw_dynamo_record(record).python_format
