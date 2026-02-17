import asyncio
from abc import ABC, abstractmethod
from logging import getLogger
from typing import AsyncGenerator, Callable, Coroutine, List

from ..model import Node, RelationshipWithNodes
from ..pipeline import Extractor
from ..pipeline.channel import DoneObject
from ..schema import Adjacency, Schema


class TypeRetriever(ABC):
    @abstractmethod
    def get_nodes_of_type(self, node_type: str) -> AsyncGenerator[Node, None]:
        raise NotImplementedError

    @abstractmethod
    def get_relationships_of_type_between(
        self, from_node_type: str, to_node_type: str, relationship_type: str
    ) -> AsyncGenerator[RelationshipWithNodes, None]:
        raise NotImplementedError


class Copier(Extractor):
    """Copies nodes and relationships sequentially, one type at a time."""

    def __init__(
        self,
        type_retriver: TypeRetriever,
        schema: Schema,
        node_types_to_copy: List[str],
        relationship_types_to_copy: List[str],
    ) -> None:
        self.type_retriever = type_retriver
        self.relationship_types = relationship_types_to_copy
        self.node_types = node_types_to_copy
        self.schema = schema
        self.logger = getLogger(__name__)
        self.logger.info(
            f"Copying {len(self.node_types)} node types and {len(self.relationship_types)} relationship types"
        )

    @classmethod
    def create(
        cls,
        type_retriever: TypeRetriever,
        schema: Schema,
        node_types: List[str],
        relationship_types: List[str],
        run_concurrently: bool = False,
        concurrency_limit: int = 10,
    ) -> "Copier":
        if run_concurrently:
            return ConcurrentCopier(
                type_retriever,
                schema,
                node_types,
                relationship_types,
                concurrency_limit=concurrency_limit,
            )
        return cls(type_retriever, schema, node_types, relationship_types)

    async def extract_records(self):
        for node_type in self.node_types:
            self.logger.info(f"Copying nodes of type {node_type}")
            async for node in self.type_retriever.get_nodes_of_type(node_type):
                yield self.convert_node_to_ingest(node)

        for rel_type in self.relationship_types:
            # Prefer schema-driven adjacency expansion when available so we can
            # fully specify from/to node types for the retriever. Note that it
            # is impossible to have a relationship without an adjacency, we only
            # support copyting from a nodestream schema.
            self.logger.info(f"Copying relationships of type {rel_type}")
            adjacencies: List[Adjacency] = list(
                self.schema.get_adjacencies_by_relationship_type(rel_type)
            )

            for adjacency in adjacencies:
                async for (
                    relationship
                ) in self.type_retriever.get_relationships_of_type_between(
                    adjacency.from_node_type,
                    adjacency.to_node_type,
                    adjacency.relationship_type,
                ):
                    yield self.convert_relationship_to_ingest(relationship)

    def reorganize_node_key_properties(self, node: Node):
        # This is a bit of a hack, but it's the only way to make sure that the
        # keys are in the right place. We need to remove the keys from the
        # properties and put them in the key_values because otherwise
        # we have to push a lot of work down to the database connector to ensure
        # that the keys are in the right place. Its easier to just do it here.
        type_def = self.schema.get_node_type_by_name(node.type)
        if type_def is None:
            return
        for key_name in type_def.keys:
            node.key_values[key_name] = node.properties[key_name]
            del node.properties[key_name]

    def convert_node_to_ingest(self, node: Node):
        self.reorganize_node_key_properties(node)
        return node.into_ingest()

    def convert_relationship_to_ingest(self, relationship: RelationshipWithNodes):
        self.reorganize_node_key_properties(relationship.from_node)
        self.reorganize_node_key_properties(relationship.to_node)
        return relationship.into_ingest()


class ConcurrentCopier(Copier):
    """Copier that runs concurrent fetch loops per type.

    All node types are fetched concurrently first (bounded by a semaphore),
    then all relationship types are fetched concurrently (bounded by the same
    limit).  The two groups are **never mixed** — nodes complete before
    relationships begin — so slow relationship pattern-scans cannot starve
    fast node label-scans.
    """

    def __init__(
        self,
        type_retriver: TypeRetriever,
        schema: Schema,
        node_types_to_copy: List[str],
        relationship_types_to_copy: List[str],
        concurrency_limit: int = 10,
    ) -> None:
        super().__init__(
            type_retriver, schema, node_types_to_copy, relationship_types_to_copy
        )
        # Ensure we always have at least one concurrent worker.
        self.concurrency_limit = max(1, concurrency_limit)

    async def extract_records(self):
        # Spin up independent producers per node / relationship type and
        # multiplex their outputs through a shared queue.
        queue: asyncio.Queue = asyncio.Queue()

        async def produce_nodes(node_type: str) -> None:
            self.logger.info(f"Copying nodes of type {node_type}")
            async for node in self.type_retriever.get_nodes_of_type(node_type):
                await queue.put(self.convert_node_to_ingest(node))

        async def produce_relationships(rel_type: str) -> None:
            self.logger.info(f"Copying relationships of type {rel_type}")
            adjacencies: List[Adjacency] = list(
                self.schema.get_adjacencies_by_relationship_type(rel_type)
            )
            for adjacency in adjacencies:
                async for (
                    relationship
                ) in self.type_retriever.get_relationships_of_type_between(
                    adjacency.from_node_type,
                    adjacency.to_node_type,
                    adjacency.relationship_type,
                ):
                    await queue.put(self.convert_relationship_to_ingest(relationship))

        async def run_bounded_producers(
            items: List[str],
            producer_fn: Callable[[str], Coroutine],
        ) -> None:
            # Use a semaphore to bound the number of concurrently running
            # producer coroutines.
            sem = asyncio.Semaphore(self.concurrency_limit)

            async def run_one(item: str) -> None:
                async with sem:
                    await producer_fn(item)

            if not items:
                return
            await asyncio.gather(*(run_one(item) for item in items))

        async def orchestrate() -> None:
            # First all node streams, then all relationship streams, each
            # bounded by the concurrency limit across types.
            await run_bounded_producers(self.node_types, produce_nodes)
            await run_bounded_producers(self.relationship_types, produce_relationships)
            await queue.put(DoneObject)

        orchestrator = asyncio.create_task(orchestrate())
        try:
            while True:
                item = await queue.get()
                if item is DoneObject:
                    break
                yield item
        finally:
            # Let the orchestrator finish; re-raises any producer exception.
            await orchestrator
