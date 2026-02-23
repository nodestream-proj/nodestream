import hashlib
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
        run_concurrently: bool = False,
        concurrency_limit: int = 10,
        progress_reporter: Optional[PipelineProgressReporter] = None,
        batch_size: int = 1000,
        step_outbox_size: int = 10000,
        flush_concurrency: int = 1,
        connector_overrides: Optional[Dict[str, object]] = None,
        retriever_overrides: Optional[Dict[str, object]] = None,
        object_store: Optional[ObjectStore] = None,
    ) -> None:
        self.from_target = from_target
        self.to_target = to_target
        self.schema = schema
        self.node_types = node_types
        self.relationship_types = relationship_types
        self.run_concurrently = run_concurrently
        self.concurrency_limit = concurrency_limit
        self.progress_reporter = progress_reporter or PipelineProgressReporter()
        self.batch_size = batch_size
        self.step_outbox_size = step_outbox_size
        self.flush_concurrency = flush_concurrency
        self.connector_overrides = connector_overrides or {}
        self.retriever_overrides = retriever_overrides or {}
        self.object_store = object_store or ObjectStore.null()

    async def perform(self, command: NodestreamCommand):
        pipeline = self.build_pipeline()
        await pipeline.run(reporter=self.progress_reporter)

    def _checkpoint_namespace(self) -> str:
        """Derive a deterministic namespace from the copy configuration.

        The same source/destination/types always map to the same namespace so
        that a resumed run picks up the previous checkpoint.
        """
        key_parts = [
            self.from_target.name,
            self.to_target.name,
            ",".join(sorted(self.node_types)),
            ",".join(sorted(self.relationship_types)),
        ]
        key_string = "|".join(key_parts)
        digest = hashlib.sha256(key_string.encode()).hexdigest()[:16]
        return f"copy-{digest}"

    def build_pipeline(self) -> Pipeline:
        copier = self.build_copier()
        writer = self.build_writer()
        return Pipeline(
            (copier, writer),
            step_outbox_size=self.step_outbox_size,
            object_store=self.object_store.namespaced(self._checkpoint_namespace()),
        )

    def build_copier(self) -> Copier:
        return Copier.create(
            self.from_target.make_type_retriever(**self.retriever_overrides),
            self.schema,
            self.node_types,
            self.relationship_types,
            run_concurrently=self.run_concurrently,
            concurrency_limit=self.concurrency_limit,
            orchestrator_queue_size=self.step_outbox_size,
        )

    def build_writer(self) -> GraphDatabaseWriter:
        return self.to_target.make_writer(
            connector_overrides=self.connector_overrides,
            batch_size=self.batch_size,
            flush_concurrency=self.flush_concurrency,
        )
