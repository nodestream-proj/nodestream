from dataclasses import dataclass

from .ingest_strategy import IngestionStrategy
from .schema import GraphObjectType


@dataclass(frozen=True, slots=True)
class KeyIndex:
    """Defines an Index that is used as a Key on a Node Type."""

    type: str
    identity_keys: frozenset

    def ingest(self, strategy: IngestionStrategy):
        strategy.upsert_key_index(self)


@dataclass(frozen=True, slots=True)
class FieldIndex:
    """Defines an index that is used on a field for a Graph Object."""

    type: str
    field: str
    object_type: GraphObjectType

    def ingest(self, strategy: IngestionStrategy):
        strategy.upsert_field_index(self)

    @classmethod
    def for_ttl_timestamp(
        cls, type, object_type: GraphObjectType = GraphObjectType.NODE
    ):
        return cls(type, "last_ingested_at", object_type=object_type)
