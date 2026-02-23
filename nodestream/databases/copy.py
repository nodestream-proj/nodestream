import asyncio
from abc import ABC, abstractmethod
from logging import getLogger
from typing import Any, AsyncGenerator, Coroutine, Dict, List

from ..metrics import Metric, Metrics
from ..model import Node, RelationshipWithNodes
from ..pipeline import Extractor
from ..pipeline.channel import DoneObject
from ..pipeline.step import StepContext
from ..schema import Adjacency, Schema

ORCHESTRATOR_QUEUE = Metric(
    "orchestrator_queue", "Number of items in the orchestrator queue", accumulate=False
)
ACTIVE_QUERIES = Metric(
    "active_queries",
    "Number of active database queries in the copier",
    accumulate=False,
)


class TypeRetriever(ABC):
    @abstractmethod
    async def preview_relationship_count(
        self, relationship_type: str
    ) -> Coroutine[int, Any, Any]:
        raise NotImplementedError

    @abstractmethod
    async def preview_node_count(self, node_type: str) -> Coroutine[int, Any, Any]:
        raise NotImplementedError

    @abstractmethod
    def get_nodes_of_type(self, node_type: str) -> AsyncGenerator[Node, None]:
        raise NotImplementedError

    @abstractmethod
    def get_relationships_of_type_between(
        self, from_node_type: str, to_node_type: str, relationship_type: str
    ) -> AsyncGenerator[RelationshipWithNodes, None]:
        raise NotImplementedError


def _node_progress_key(node_type: str) -> str:
    """Return a stable checkpoint key for a node-type producer."""
    return f"node:{node_type}"


def _adjacency_progress_key(adjacency: Adjacency) -> str:
    """Return a stable checkpoint key for an adjacency producer."""
    return (
        f"adj:{adjacency.from_node_type}"
        f":{adjacency.to_node_type}"
        f":{adjacency.relationship_type}"
    )


class Copier(Extractor):
    """Copies nodes and relationships sequentially, one type at a time.

    Supports checkpoint-based resumption.  The ``_progress`` dict maps each
    producer key (``node:<type>`` or ``adj:<from>:<to>:<rel>``) to the number
    of records that have been **yielded downstream**.  On resume the copier
    re-iterates through the ``TypeRetriever`` generators but skips records that
    were already yielded before the last checkpoint.
    """

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
        # Checkpoint state: maps producer key -> records yielded.
        self._progress: Dict[str, int] = {}
        # Preview counts populated during start(); used for progress logging.
        self._node_counts: Dict[str, int] = {}
        self._rel_counts: Dict[str, int] = {}
        self.logger.info(
            f"Copying {len(self.node_types)} node types and "
            f"{len(self.relationship_types)} relationship types"
        )

    # -- Checkpoint protocol --------------------------------------------------

    async def make_checkpoint(self):
        if not self._progress:
            return None
        return {"progress": dict(self._progress)}

    async def resume_from_checkpoint(self, checkpoint_object):
        self._progress = checkpoint_object.get("progress", {})
        self.logger.info(f"Resuming copy from checkpoint: {self._progress}")

    # -- Factory --------------------------------------------------------------

    @classmethod
    def create(
        cls,
        type_retriever: TypeRetriever,
        schema: Schema,
        node_types: List[str],
        relationship_types: List[str],
        run_concurrently: bool = False,
        concurrency_limit: int = 10,
        orchestrator_queue_size: int | None = None,
    ) -> "Copier":
        if run_concurrently:
            return ConcurrentCopier(
                type_retriever,
                schema,
                node_types,
                relationship_types,
                concurrency_limit=concurrency_limit,
                orchestrator_queue_size=orchestrator_queue_size,
            )
        return cls(type_retriever, schema, node_types, relationship_types)

    async def start(self, context: StepContext):
        await super().start(context)

        # Build histograms and sort descending so largest types are copied first.
        for node_type in self.node_types:
            self._node_counts[node_type] = await self.type_retriever.preview_node_count(
                node_type
            )

        for rel_type in self.relationship_types:
            self._rel_counts[rel_type] = (
                await self.type_retriever.preview_relationship_count(rel_type)
            )

        self.node_types = sorted(
            self.node_types, key=lambda t: self._node_counts[t], reverse=True
        )
        self.relationship_types = sorted(
            self.relationship_types, key=lambda t: self._rel_counts[t], reverse=True
        )

        # Log the sorted histogram.
        self.logger.info("Node type histogram (descending):")
        for node_type in self.node_types:
            self.logger.info(f"  {node_type}: {self._node_counts[node_type]}")
        self.logger.info("Relationship type histogram (descending):")
        for rel_type in self.relationship_types:
            self.logger.info(f"  {rel_type}: {self._rel_counts[rel_type]}")
        self.logger.info(
            f"Total nodes: {sum(self._node_counts.values())}, "
            f"Total relationships: {sum(self._rel_counts.values())}"
        )

    async def extract_records(self):
        for node_type in self.node_types:
            key = _node_progress_key(node_type)
            expected = self._node_counts.get(node_type, "?")
            skip_count = self._progress.get(key, 0)
            if skip_count:
                self.logger.info(
                    f"Resuming nodes of type {node_type} "
                    f"(expected ~{expected}, skipping {skip_count} already-copied)"
                )
            else:
                self.logger.info(
                    f"Copying nodes of type {node_type} (expected ~{expected})"
                )

            count = 0
            async for node in self.type_retriever.get_nodes_of_type(node_type):
                count += 1
                if count <= skip_count:
                    continue
                self._progress[key] = count
                yield self.convert_node_to_ingest(node)

        for rel_type in self.relationship_types:
            expected = self._rel_counts.get(rel_type, "?")
            adjacencies: List[Adjacency] = list(
                self.schema.get_adjacencies_by_relationship_type(rel_type)
            )

            for adjacency in adjacencies:
                key = _adjacency_progress_key(adjacency)
                skip_count = self._progress.get(key, 0)
                if skip_count:
                    self.logger.info(
                        f"Resuming relationships {adjacency.relationship_type} "
                        f"({adjacency.from_node_type} -> {adjacency.to_node_type}, "
                        f"expected ~{expected}), "
                        f"skipping {skip_count} already-copied"
                    )
                else:
                    self.logger.info(
                        f"Copying relationships of type {adjacency.relationship_type} "
                        f"({adjacency.from_node_type} -> {adjacency.to_node_type}, "
                        f"expected ~{expected})"
                    )

                count = 0
                async for (
                    relationship
                ) in self.type_retriever.get_relationships_of_type_between(
                    adjacency.from_node_type,
                    adjacency.to_node_type,
                    adjacency.relationship_type,
                ):
                    count += 1
                    if count <= skip_count:
                        continue
                    self._progress[key] = count
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

    Checkpoint support works by tagging every item placed onto the internal
    queue with its producer key.  The consumer loop (which is the point at
    which records are *yielded downstream*) updates ``_progress`` per-key.
    On resume each producer reads its skip-count from ``_progress`` and
    discards that many records before producing new ones.
    """

    def __init__(
        self,
        type_retriver: TypeRetriever,
        schema: Schema,
        node_types_to_copy: List[str],
        relationship_types_to_copy: List[str],
        concurrency_limit: int = 10,
        orchestrator_queue_size: int | None = None,
    ) -> None:
        super().__init__(
            type_retriver, schema, node_types_to_copy, relationship_types_to_copy
        )
        # Ensure we always have at least one concurrent worker.
        self.concurrency_limit = max(1, concurrency_limit)
        # Limit for the internal orchestrator queue; when None/0 it is unbounded.
        self.orchestrator_queue_size = orchestrator_queue_size or 0

    async def extract_records(self):
        # Spin up independent producers per node / relationship type and
        # multiplex their outputs through a shared queue.  Each item is a
        # (progress_key, ingest) tuple so the consumer can track per-producer
        # progress for checkpointing.
        queue: asyncio.Queue = asyncio.Queue(maxsize=self.orchestrator_queue_size)

        async def produce_nodes(node_type: str) -> None:
            key = _node_progress_key(node_type)
            expected = self._node_counts.get(node_type, "?")
            skip_count = self._progress.get(key, 0)
            if skip_count:
                self.logger.info(
                    f"Resuming nodes of type {node_type} "
                    f"(expected ~{expected}, skipping {skip_count} already-copied)"
                )
            else:
                self.logger.info(
                    f"Copying nodes of type {node_type} (expected ~{expected})"
                )

            count = 0
            async for node in self.type_retriever.get_nodes_of_type(node_type):
                count += 1
                if count <= skip_count:
                    continue
                Metrics.get().increment(ORCHESTRATOR_QUEUE)
                item = self.convert_node_to_ingest(node)
                await queue.put((key, item))

        async def produce_adjacency(adjacency: Adjacency) -> None:
            key = _adjacency_progress_key(adjacency)
            expected = self._rel_counts.get(adjacency.relationship_type, "?")
            skip_count = self._progress.get(key, 0)
            if skip_count:
                self.logger.info(
                    f"Resuming relationships {adjacency.relationship_type} "
                    f"({adjacency.from_node_type} -> {adjacency.to_node_type}, "
                    f"expected ~{expected}), "
                    f"skipping {skip_count} already-copied"
                )
            else:
                self.logger.info(
                    f"Copying relationships of type {adjacency.relationship_type} "
                    f"({adjacency.from_node_type} -> {adjacency.to_node_type}, "
                    f"expected ~{expected})"
                )

            count = 0
            async for (
                relationship
            ) in self.type_retriever.get_relationships_of_type_between(
                adjacency.from_node_type,
                adjacency.to_node_type,
                adjacency.relationship_type,
            ):
                count += 1
                if count <= skip_count:
                    continue
                Metrics.get().increment(ORCHESTRATOR_QUEUE)
                item = self.convert_relationship_to_ingest(relationship)
                await queue.put((key, item))

        async def run_bounded(coros: List[Coroutine]) -> None:
            # Use a semaphore to bound the number of concurrently running
            # producer coroutines.
            sem = asyncio.Semaphore(self.concurrency_limit)

            async def run_one(coro: Coroutine) -> None:
                async with sem:
                    Metrics.get().increment(ACTIVE_QUERIES)
                    try:
                        await coro
                    finally:
                        Metrics.get().decrement(ACTIVE_QUERIES)

            if not coros:
                return
            await asyncio.gather(*(run_one(c) for c in coros))

        async def orchestrate() -> None:
            # First all node streams, then all relationship streams, each
            # bounded by the concurrency limit.
            await run_bounded([produce_nodes(nt) for nt in self.node_types])

            # Flatten all adjacencies across all relationship types so each
            # adjacency is an independent producer bounded by the semaphore.
            all_adjacencies: List[Adjacency] = []
            for rel_type in self.relationship_types:
                all_adjacencies.extend(
                    self.schema.get_adjacencies_by_relationship_type(rel_type)
                )
            await run_bounded([produce_adjacency(adj) for adj in all_adjacencies])
            await queue.put(DoneObject)

        orchestrator = asyncio.create_task(orchestrate())
        try:
            while True:
                raw = await queue.get()
                if raw is DoneObject:
                    break
                key, item = raw
                Metrics.get().decrement(ORCHESTRATOR_QUEUE)
                self._progress[key] = self._progress.get(key, 0) + 1
                yield item
        finally:
            # Let the orchestrator finish; re-raises any producer exception.
            await orchestrator
