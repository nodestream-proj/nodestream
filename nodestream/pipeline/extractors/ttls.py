from typing import Any, Dict, Iterable

from ...model import TimeToLiveConfiguration
from ...schema.schema import GraphObjectType
from .extractor import Extractor


class TimeToLiveConfigurationExtractor(Extractor):
    def __init__(
        self,
        graph_object_type: str,
        configurations: Iterable[Dict[str, Any]],
        global_expiry_in_hours: int = None,
    ) -> None:
        self.graph_object_type = GraphObjectType(graph_object_type)
        self.configurations = configurations
        self.global_expiry_in_hours = global_expiry_in_hours

    async def extract_records(self):
        for config in self.configurations:
            # Only when global_expiry_in_hours is set, will we have an overwrite of all ttl's
            if self.global_expiry_in_hours:
                config["expiry_in_hours"] = self.global_expiry_in_hours
            yield TimeToLiveConfiguration(
                **config,
                graph_object_type=self.graph_object_type,
            )
