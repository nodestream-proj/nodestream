from typing import Any, Dict, Iterable

from ...model import TimeToLiveConfiguration
from ...schema.schema import GraphObjectType
from .extractor import Extractor


class TimeToLiveConfigurationExtractor(Extractor):
    def __init__(
        self, graph_object_type: str, configurations: Iterable[Dict[str, Any]]
    ) -> None:
        self.graph_object_type = GraphObjectType(graph_object_type)
        self.configurations = configurations

    async def extract_records(self):
        for config in self.configurations:
            yield TimeToLiveConfiguration(
                **config, graph_object_type=self.graph_object_type
            )
