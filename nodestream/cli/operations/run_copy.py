from typing import Dict, Optional

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
        self.progress_reporter = progress_reporter or PipelineProgressReporter()
        self.batch_size = batch_size
        self.step_outbox_size = step_outbox_size
        self.flush_concurrency = flush_concurrency
        self.connector_overrides = connector_overrides or {}
        self.retriever_overrides = retriever_overrides or {}

    async def perform(self, command: NodestreamCommand):
        await self.build_pipeline().run(reporter=self.progress_reporter)

    def build_pipeline(self) -> Pipeline:
        copier = self.build_copier()
        writer = self.build_writer()
        return Pipeline(
            (copier, writer),
            step_outbox_size=self.step_outbox_size,
            object_store=ObjectStore.null(),
        )

    def build_copier(self) -> Copier:
        retriever = self.from_target.make_type_retriever(
            schema=self.schema,
            **self.retriever_overrides,
        )
        return Copier(retriever)

    def build_writer(self) -> GraphDatabaseWriter:
        return self.to_target.make_writer(
            connector_overrides=self.connector_overrides,
            batch_size=self.batch_size,
            flush_concurrency=self.flush_concurrency,
        )
