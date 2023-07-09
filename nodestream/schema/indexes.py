from dataclasses import dataclass
from typing import TYPE_CHECKING

from .schema import GraphObjectType

if TYPE_CHECKING:
    from ..databases.ingest_strategy import IngestionStrategy


@dataclass(frozen=True, slots=True)
class KeyIndex:
    """Defines an Index that is used as a Key on a Node Type.

    `KeyIndex`es are created by Interpretations that attempt to create and use
    a node reference as a Primary Key. Usually this implies the creation of a
    uniqueness constraint in the database.

    See information on your Underlying Graph Database Adapter for information
    on how this is implemented.
    """

    type: str
    identity_keys: frozenset

    async def ingest(self, strategy: "IngestionStrategy"):
        await strategy.upsert_key_index(self)


@dataclass(frozen=True, slots=True)
class FieldIndex:
    """Defines an index that is used on a field for a Graph Object.

    `FieldIndex`es are created on fields that do not imply a uniqueness constraint and "just" need
    to be indexes for query performance.
    """

    type: str
    field: str
    object_type: GraphObjectType

    async def ingest(self, strategy: "IngestionStrategy"):
        await strategy.upsert_field_index(self)

    @classmethod
    def for_ttl_timestamp(
        cls, type, object_type: GraphObjectType = GraphObjectType.NODE
    ):
        return cls(type, "last_ingested_at", object_type=object_type)
