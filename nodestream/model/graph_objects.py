import datetime
import threading
from dataclasses import dataclass, field
from typing import Any, Tuple


def get_pipeline_name():
    return threading.current_thread().name



class PropertySet(dict):
    def add_property(self, property_key: str, property_value: Any):
        self[property_key] = property_value

    @classmethod
    def default_properties(cls) -> "PropertySet":
        pipeline_name = get_pipeline_name()
        now = datetime.utcnow()
        return cls(
            {
                "last_ingested_at": now,
                f"last_ingested_by_{pipeline_name}_at": now,
                f"was_ingested_by_{pipeline_name}": True,
            }
        )

    @classmethod
    def empty(cls) -> "PropertySet":
        return PropertySet()


@dataclass(slots=True)
class Node:
    """A `Node` is a entity that has a distinct identity."""

    type: str
    key_values: PropertySet
    properties: PropertySet = field(default_factory=PropertySet.default_properties)
    additional_types: Tuple[str] = field(default_factory=tuple)

    @property
    def has_valid_id(self) -> bool:
        # Return that some of the ID values are defined.
        return not all(value is None for value in self.source.identity_values.values())
    
    @property
    def is_valid(self) -> bool:
        return self.has_valid_id


@dataclass(slots=True)
class Relationship:
    key_values: PropertySet = field(default_factory=PropertySet.empty)
    properties: PropertySet = field(default_factory=PropertySet.default_properties)
    type: str
