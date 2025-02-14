from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from ..subclass_registry import SubclassRegistry

if TYPE_CHECKING:
    from ..model import (
        IngestionHookRunRequest,
        Node,
        NodeCreationRule,
        RelationshipWithNodes,
        TimeToLiveConfiguration,
    )


INGESTION_STRATEGY_REGISTRY = SubclassRegistry()


@INGESTION_STRATEGY_REGISTRY.connect_baseclass
class IngestionStrategy(ABC):
    """An IngestionStrategy represents the methods taken to commit data to a Graph Database.

    Within nodestream, many components gather and intend to commit changes to a Graph Database.
    How that data is committed and how it is stored internally is decoupled through the `IngestionStrategy` interface.

    Generally, your usage of nodestream is decoupled from `IngestionStrategy` unless you intend to provide an implementation
    of your own database writer. The writer API will give you a `DesiredIngestion` or other `Ingestible` object that needs an
    instance of an `IngestionStrategy` to apply operations to.
    """

    @abstractmethod
    async def ingest_source_node(
        self, source: "Node", creation_rule: "NodeCreationRule"
    ):
        """This asynchronous function ingests a given 'source' Node into the GraphDatabase.

        Args:
        source (Node): The Node instance that needs to be committed into the GraphDatabase.
        creation_rule (NodeCreationRule): The rule governing the creation of the Node instance.

        Returns:
        None
        """
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
    async def perform_ttl_operation(self, config: "TimeToLiveConfiguration"):
        """Perform a TTL Operation.

        See Information on `TimeToLiveConfiguration` for more Information.
        """
        raise NotImplementedError

    async def flush(self):
        """Flush any pending operations to the database."""
        pass

    @abstractmethod
    async def finish(self):
        """Close connector by calling finish method from Step"""
        raise NotImplementedError
