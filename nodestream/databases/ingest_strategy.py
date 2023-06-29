from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from ..subclass_registry import SubclassRegistry

if TYPE_CHECKING:
    from ..model import (
        IngestionHookRunRequest,
        Node,
        RelationshipWithNodes,
        TimeToLiveConfiguration,
    )
    from ..schema.indexes import FieldIndex, KeyIndex


INGESTION_STRATEGY_REGISTRY = SubclassRegistry()


@INGESTION_STRATEGY_REGISTRY.connect_baseclass
class IngestionStrategy(ABC):
    """An IngestionStrategy represents the methods taken to commit data to a Graph Database.

    Within nodestream, many components gather and intend to commit changes to a Graph Database.
    How that data is committed and how it is stored internally is decoupled through the `IngestionStrategy` interface.

    Generally, your usage of nodestream is decoupled from `IngestionStrategy` unless you intend to provide an implementation
    of your own database writer. The writer API will give you a `DesiredIngestion` or other `Ingestible` object that needs a
    instance of an `IngestionStrategy` to apply operations to.
    """

    @abstractmethod
    async def ingest_source_node(self, source: "Node"):
        """Given a provided instance of `Node`, ensure that it is committed to the GraphDatabase."""
        raise NotImplementedError

    @abstractmethod
    async def ingest_relationship(self, relationship: "RelationshipWithNodes"):
        """Given a provided instance of `SourceNode`, ensure that the provided `Relationship` is committed to the database."""
        raise NotImplementedError

    @abstractmethod
    async def run_hook(self, request: "IngestionHookRunRequest"):
        """Runs the provided request for an IngestHook given the context."""
        raise NotImplementedError

    @abstractmethod
    async def upsert_key_index(self, index: "KeyIndex"):
        """Create a Key Index Immediately for a given Node Type.

        See information on `KeyIndex` for more information.
        """
        raise NotImplementedError

    @abstractmethod
    async def upsert_field_index(self, index: "FieldIndex"):
        """Create a Key Index Immediately for a Object Type.

        See information on `FieldIndex` for more information.
        """
        raise NotImplementedError

    @abstractmethod
    async def perform_ttl_operation(self, config: "TimeToLiveConfiguration"):
        """Perform a TTL Operation.

        See Information on `TimeToLiveConfiguration` for more Information.
        """
        raise NotImplementedError

    async def flush(self):
        """Flush any pending operations to the database."""
        pass
