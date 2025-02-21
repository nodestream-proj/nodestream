from typing import Any, Dict, Iterable

from ...model import TimeToLiveConfiguration
from ...schema import GraphObjectType
from .iterable import IterableExtractor


class TimeToLiveConfigurationExtractor(IterableExtractor):
    def __init__(
        self,
        graph_object_type: str,
        configurations: Iterable[Dict[str, Any]],
        override_expiry_in_hours: int = None,
    ) -> None:

        def make_config(config: Dict[str, Any]) -> None:
            if override_expiry_in_hours is not None:
                config["expiry_in_hours"] = override_expiry_in_hours
            return TimeToLiveConfiguration(
                **config,
                graph_object_type=GraphObjectType(graph_object_type),
            )

        super().__init__([make_config(config) for config in configurations])
