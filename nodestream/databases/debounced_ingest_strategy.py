import asyncio
import time
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
    def __init__(
        self,
        query_executor: QueryExecutor,
    ) -> None:
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

    async def flush_nodes_updates(self):
        await asyncio.gather(
            *(
                self.executor.upsert_nodes_in_bulk_with_same_operation(
                    operation, node_group
                )
                for operation, node_group in self.debouncer.drain_node_groups()
            )
        )

    async def flush_relationship_updates(self):
        await asyncio.gather(
            *(
                self.executor.upsert_relationships_in_bulk_of_same_operation(
                    rel_shape, rel_group
                )
                for rel_shape, rel_group in self.debouncer.drain_relationship_groups()
            )
        )

    async def flush(self):
        t0 = time.monotonic()
        await asyncio.gather(
            self.flush_nodes_updates(),
            self.flush_relationship_updates(),
        )
        t1 = time.monotonic()
        # Because we don't know what exactly the hooks do, we can't reliably parallelize
        # them because we could overwhelm the database so we are going to do them one at
        # a time.
        for hook in self.hooks_saved_for_after_ingest:
            await self.executor.execute_hook(hook)
        self.hooks_saved_for_after_ingest.clear()
        t2 = time.monotonic()

        self.logger.info(
            "Flush completed",
            extra={
                "flush_wall_time_s": round(t2 - t0, 3),
                "upserts_flush_s": round(t1 - t0, 3),
                "hooks_flush_s": round(t2 - t1, 3),
            },
        )

    async def finish(self):
        """Close connector by calling finish method from Step"""
        await self.executor.finish()
