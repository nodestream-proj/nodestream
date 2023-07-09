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

    async def transform_record(self, record: JsonLikeDocument):
        item = record
        for path_segment in self.path[:-1]:
            record = item[path_segment]
        last_segment = self.path[-1]
        item[last_segment] = json.loads(item[last_segment])
        return record
