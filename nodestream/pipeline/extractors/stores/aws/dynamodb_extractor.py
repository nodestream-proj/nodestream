from logging import getLogger
from typing import Any, Dict, List, AsyncGenerator

from ...credential_utils import AwsClientFactory
from ...extractor import Extractor


class DynamoObject:
    """
        Dynamo objects generally come in the form {"attribute_type": value}
        Example:
            {"S", "some_string"}
            {"N", "12345"}
    """
    def __init__(self, object: Dict[str, Any]):
        self.attribute_type = list(object.keys())[0]
        self.value = list(object.values())[0]

    def items(self) -> tuple[str, Any]:
        return self.attribute_type, self.value
    
    def as_dynamo_representation(self):
        if isinstance(self.value, dict):
            return {self.attribute_type: {key: value.as_dynamo_representation() for key, value in self.value.items()}}
        elif isinstance(self.value, list):
            return {self.attribute_type: [value.as_dynamo_representation() for value in self.value ]}
        return {self.attribute_type: self.value}

def leave_untouched(value):
    return value

def convert_scalar(value, type_handler):
    return  None if type_handler is None else type_handler(value)

def convert_array(array: List[Any], type_handler):
    return [type_handler(value) for value in array]

def convert_map(mapping: Dict[str, Any], type_handler):
    return {key: type_handler(value) for key, value in mapping.items()}

def convert_number(value: str):
    if '.' in value:
        return float(value)
    return int(value)

DEFAULT_HANDLER = (convert_scalar, leave_untouched)

def convert_value(dynamo_object: Dict[str, Any]):
    attribute_type, value = DynamoObject(dynamo_object).items()
    result_handler, type_handler = ATTRIBUTE_TYPE_DESCRIPTOR_HANDLERS.get(attribute_type, DEFAULT_HANDLER)
    return result_handler(value, type_handler)


ATTRIBUTE_TYPE_DESCRIPTOR_HANDLERS = {
    "S": (convert_scalar, str),
    "N": (convert_scalar, convert_number),
    "B": (convert_scalar, bytes),
    "NULL": (convert_scalar, None),
    "BOOL": (convert_scalar, bool),
    "SS": (convert_array, str),
    "NS": (convert_array, convert_number),
    "BS": (convert_array, bytes),
    "M": (convert_map, convert_value),
    "L": (convert_array, convert_value)
}

class DynamoRowConverter:
    def __init__(self, item: Dict[str, Any]) -> None:
        self.item = item

    # { key: {"S": "string"}, secondary_key: { "M": {Stuff} }, tertiary_key: { "L": [Stuff] } }
    def convert_row(self):
        return {attribute_name: convert_value(attribute_value) for attribute_name, attribute_value in self.item.items()}


def verify_list_consistency(value_list: List[Any]):
    type_set = set([type(value) for value in value_list])
    if len(type_set > 1):
        return "L"
    return REVERSE_ORDER_DESCRIPTOR_HANDLERS[type_set[0]] + "S"

def boolean_to_dynamo_format_converter(value: None | bool):
    return True if value is None else False

def convert_value_to_dynamo_format(value: Any):
    value_type, handler = REVERSE_ORDER_DESCRIPTOR_HANDLERS[type(value)]
    if (value_type == "LIST"):
        value_type = verify_list_consistency(value)
    converted_value = handler(value)
    return DynamoObject({value_type: converted_value})

def recursive_dynamo_format_converter(value: Dict[str, Any] | List[Any]):
    if isinstance(value, Dict):
        return {key: convert_value_to_dynamo_format(item) for key, item in value.items()}
    return [convert_value_to_dynamo_format(item) for item in value]
    

REVERSE_ORDER_DESCRIPTOR_HANDLERS = {
    int: ("N", str),
    float: ("N", str),
    str: ("S", leave_untouched),
    bytes:( "B", leave_untouched),
    None: ("NULL", boolean_to_dynamo_format_converter),
    bool: ("BOOL", leave_untouched),
    dict: ("M", recursive_dynamo_format_converter),
    list: ("LIST", recursive_dynamo_format_converter)
}


def convert_value_list_to_dynamo_format(value_list: List[Any]):
    return_values = []
    for value in value_list:
        return_values.append(convert_value_to_dynamo_format(value))
    return return_values


class DynamoConditional:
    @classmethod
    def from_conditional_data(cls, conditional_data: Dict[str, Any]):
        return cls(**conditional_data)
    
    def __init__(self, AttributeValueList: List[Any], ComparisonOperator: str):
        self.AttributeValueList: List[DynamoObject] = convert_value_list_to_dynamo_format(AttributeValueList)
        self.ComparisonOperator = ComparisonOperator

    def generate_filter_format(self):
        return {'AttributeValueList': [dynamo_object.as_dynamo_representation() for dynamo_object in self.AttributeValueList], 'ComparisonOperator': self.ComparisonOperator}


class DynamoFilterConverter:
    @classmethod
    def from_file_data(cls, scan_filter: Dict[str, Any]):
        return cls({attribute_name: DynamoConditional.from_conditional_data(conditional_data) for attribute_name, conditional_data in scan_filter.items()})
    
    def __init__(self, conditionals: Dict[str, DynamoConditional]):
        self.conditionals = conditionals

    def generate_filter_converter(self):
        return {attribute_name: conditional.generate_filter_format() for attribute_name, conditional in self.conditionals.items()}
    

class DynamoDBExtractor(Extractor):
    @classmethod
    def from_file_data(
        cls,
        table: str,
        scan_filter: Dict[str, Any] = {},
        limit: int = 100,
        **aws_client_args,
    ):
        print(aws_client_args)
        client = AwsClientFactory(**aws_client_args).make_client("dynamodb")
        return cls(
            table=table,
            limit=limit,
            scan_filter=scan_filter,
            client=client,
        )

    def __init__(
        self,
        table,
        limit: int,
        scan_filter: Dict[str, Any],
        client,
    ) -> None:
        self.table = table
        self.client = client
        self.scan_filter = DynamoFilterConverter.from_file_data(scan_filter).generate_filter_converter()
        self.limit = limit
        self.logger = getLogger(self.__class__.__name__)

    def scan_table(self):
        items = []
        should_continue = True
        parameters = { "TableName": self.table, "Limit": self.limit}
        if self.scan_filter:
            parameters.update({"ScanFilter": self.scan_filter})

        while (should_continue):
            response: dict = self.client.scan(
                **parameters
            )
            items += response["Items"]
            last_evaluated_key = response.get("LastEvaluatedKey", None)
            if last_evaluated_key is not None:
                should_continue = True
                parameters.update({"ExclusiveStartKey": last_evaluated_key})
            else:
                should_continue = False
        return items

    async def extract_records(self) -> AsyncGenerator[Any, Any]:
        records = self.scan_table()
        converter = DynamoRowConverter
        for record in records:
            converted = converter(record).convert_row()
            print(converted, '\n\n')
            yield converted