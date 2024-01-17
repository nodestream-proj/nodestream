import json
from typing import List, Union

from ...model import JsonLikeDocument
from .transformer import Transformer


class ExpandJsonField(Transformer):
    @classmethod
    def from_file_data(cls, /, path: Union[str, List[str]]):
        if isinstance(path, str):
            path = [path]

        return cls(path)

    def __init__(self, path: List[str]) -> None:
        self.path = path

    def dig_to_path(self, record: JsonLikeDocument, path: List[str]):
        value_at_path = record
        for path_segment in path:
            value_at_path = value_at_path.get(path_segment, None)
            if value_at_path is None:
                return None

        return value_at_path

    def replace_json_value(self, innermost_dictionary, key):
        json_string = innermost_dictionary.get(key, None)
        if json_string is None:
            return
        innermost_dictionary[key] = json.loads(json_string)

    async def transform_record(self, record: JsonLikeDocument):
        last_segment = self.path[-1]
        innermost_dictionary = self.dig_to_path(record, self.path[:-1])
        if innermost_dictionary is None:
            return record
        self.replace_json_value(innermost_dictionary, last_segment)
        return record
