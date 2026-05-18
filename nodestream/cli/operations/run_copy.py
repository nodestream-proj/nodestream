from typing import Dict, List, Optional

from ...databases import Copier, GraphDatabaseWriter
from ...pipeline import Pipeline
from ...pipeline.object_storage import ObjectStore
from ...pipeline.progress_reporter import PipelineProgressReporter
from ...project import Target
from ...schema import Schema
from ..commands.nodestream_command import NodestreamCommand
from .operation import Operation


class RunCopy(Operation):
    def __init__(
        self,
        from_target: Target,
        to_target: Target,
        schema: Schema,
        node_types: List[str],
        relationship_types: List[str],
        progress_reporter: Optional[PipelineProgressReporter] = None,
        batch_size: int = 1000,
        step_outbox_size: int = 10000,
        flush_concurrency: int = 1,
        connector_overrides: Optional[Dict[str, object]] = None,
        retriever_overrides: Optional[Dict[str, object]] = None,
    ) -> None:
        self.from_target = from_target
        self.to_target = to_target
        self.schema = schema
        self.node_types = node_types
        self.relationship_types = relationship_types
        self.progress_reporter = progress_reporter or PipelineProgressReporter()
        self.batch_size = batch_size
        self.step_outbox_size = step_outbox_size
        self.flush_concurrency = flush_concurrency
        self.connector_overrides = connector_overrides or {}
        self.retriever_overrides = retriever_overrides or {}

    async def perform(self, command: NodestreamCommand):
        pipeline = await self.build_pipeline()
        await pipeline.run(reporter=self.progress_reporter)

    async def build_pipeline(self) -> Pipeline:
        copier = await self.build_copier()
        writer = self.build_writer()
        return Pipeline(
            (copier, writer),
            step_outbox_size=self.step_outbox_size,
            object_store=ObjectStore.null(),
        )

    async def build_copier(self) -> Copier:
        retriever = self.from_target.make_type_retriever(
            node_types=self.node_types,
            relationship_types=self.relationship_types,
            **self.retriever_overrides,
        )
        schema = self.schema
        # If the schema has no node types defined (skipped for performance), infer
        # key fields and adjacency patterns from the live source database.
        if not list(schema.nodes) and hasattr(retriever, "build_schema_from_db"):
            schema = await retriever.build_schema_from_db(
                self.node_types, self.relationship_types
            )
        return Copier(retriever, schema)

    def build_writer(self) -> GraphDatabaseWriter:
        return self.to_target.make_writer(
            connector_overrides=self.connector_overrides,
            batch_size=self.batch_size,
            flush_concurrency=self.flush_concurrency,
        )
