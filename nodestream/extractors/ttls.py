from typing import Any, Dict, Iterable

from ..pipeline import Extractor
from ..model import GraphObjectType, TimeToLiveConfiguration


class TimeToLiveConfigurationExtractor(Extractor):
    def __init__(
        self, graph_type: str, configurations: Iterable[Dict[str, Any]]
    ) -> None:
        self.graph_type = GraphObjectType(graph_type)
        self.configurations = configurations

    async def extract_records(self):
        for config in self.configurations:
            yield TimeToLiveConfiguration(**config, graph_type=self.graph_type)
