import asyncio
from dataclasses import asdict
from logging import getLogger

from ..model import (
    IngestionHookRunRequest,
    Node,
    NodeCreationRule,
    RelationshipWithNodes,
    TimeToLiveConfiguration,
)
from .ingest_strategy import IngestionStrategy
from .operation_debouncer import OperationDebouncer
from .query_executor import QueryExecutor


class DebouncedIngestStrategy(IngestionStrategy, alias="debounced"):
    def __init__(self, query_executor: QueryExecutor) -> None:
        self.logger = getLogger(self.__class__.__name__)
        self.executor = query_executor
        self.debouncer = OperationDebouncer()
        self.hooks_saved_for_after_ingest = []

    async def ingest_source_node(
        self, source: Node, creation_rule: NodeCreationRule = NodeCreationRule.EAGER
    ) -> None:
        self.debouncer.debounce_node_operation(source, node_creation_rule=creation_rule)

    async def ingest_relationship(self, relationship: RelationshipWithNodes):
        self.debouncer.debounce_node_operation(
            relationship.from_node, relationship.from_side_node_creation_rule
        )
        self.debouncer.debounce_node_operation(
            relationship.to_node, relationship.to_side_node_creation_rule
        )
        self.debouncer.debounce_relationship(relationship)

    async def run_hook(self, request: IngestionHookRunRequest):
        if request.before_ingest:
            await self.executor.execute_hook(request.hook)
        else:
            self.hooks_saved_for_after_ingest.append(request.hook)

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

    async def flush_relationship_updates(self):
        # Because databases tend to require exclusive locks on both nodes,
        # and these updates are very likely to be related to at least one of the nodes
        # in the relationship, we need to update relationships one operation type
        # at a time to avoid deadlocks.
        for rel_shape, rel_group in self.debouncer.drain_relationship_groups():
            await self.executor.upsert_relationships_in_bulk_of_same_operation(
                rel_shape, rel_group
            )

    async def flush(self):
        await self.flush_nodes_updates()
        await self.flush_relationship_updates()

        # Because we don't know what exactly the hooks do, we can't reliably parallelize
        # them because we could overwhelm the database so we are going to do them one at
        # a time.
        for hook in self.hooks_saved_for_after_ingest:
            await self.executor.execute_hook(hook)
        self.hooks_saved_for_after_ingest.clear()

    async def finish(self):
        """Close connector by calling finish method from Step"""
        await self.executor.finish()
