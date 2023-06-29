import asyncio
from dataclasses import asdict
from logging import getLogger

from ..model import (
    IngestionHookRunRequest,
    MatchStrategy,
    Node,
    RelationshipWithNodes,
    TimeToLiveConfiguration,
)
from ..schema.indexes import FieldIndex, KeyIndex
from .ingest_strategy import IngestionStrategy
from .operation_debouncer import OperationDebouncer
from .query_executor import QueryExecutor


class DebouncedIngestStrategy(IngestionStrategy, alias="debounced"):
    def __init__(self, query_executor: QueryExecutor) -> None:
        self.logger = getLogger(self.__class__.__name__)
        self.executor = query_executor
        self.debouncer = OperationDebouncer()
        self.hooks_saved_for_after_ingest = []

    async def ingest_source_node(self, source: Node):
        self.debouncer.debounce_node_operation(
            source, match_strategy=MatchStrategy.EAGER
        )

    async def ingest_relationship(self, relationship: RelationshipWithNodes):
        self.debouncer.debounce_node_operation(
            relationship.from_node, relationship.from_side_match_strategy
        )
        self.debouncer.debounce_node_operation(
            relationship.to_node, relationship.to_side_match_strategy
        )
        self.debouncer.debounce_relationship(relationship)

    async def run_hook(self, request: IngestionHookRunRequest):
        if request.before_ingest:
            await self.executor.execute_hook(request.hook)
        else:
            self.hooks_saved_for_after_ingest.append(request.hook)

    async def upsert_key_index(self, index: KeyIndex):
        self.logger.debug("Ensuring Index Created", extra=asdict(index))
        await self.executor.upsert_key_index(index)
        self.logger.info("Ensured Index Created", extra=asdict(index))

    async def upsert_field_index(self, index: FieldIndex):
        self.logger.debug("Ensuring Index Created", extra=asdict(index))
        await self.executor.upsert_field_index(index)
        self.logger.info("Ensured Index Created", extra=asdict(index))

    async def perform_ttl_operation(self, config: TimeToLiveConfiguration):
        self.logger.debug("Executing TTL", extra=asdict(config))
        await self.executor.perform_ttl_op(config)
        self.logger.info("Executed TTL", extra=asdict(config))

    def flush_nodes_updates(self):
        update_coroutines = (
            self.executor.upsert_nodes_in_bulk_with_same_operation(
                operation, node_group
            )
            for operation, node_group in self.debouncer.drain_node_groups()
        )
        return asyncio.gather(*update_coroutines)

    def flush_relationship_updates(self):
        update_coroutines = (
            self.executor.upsert_relationships_in_bulk_of_same_operation(
                rel_shape, rel_group
            )
            for rel_shape, rel_group in self.debouncer.drain_relationship_groups()
        )
        return asyncio.gather(*update_coroutines)

    async def flush(self):
        await self.flush_nodes_updates()
        await self.flush_relationship_updates()

        # Because we don't know what exactly the hooks do, we can't reliably parallelize
        # them because we could overwhelm the database so we are going to do them one at
        # a time.
        for hook in self.hooks_saved_for_after_ingest:
            await self.executor.execute_hook(hook)
