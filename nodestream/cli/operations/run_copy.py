from typing import List

from ...databases import Copier, GraphDatabaseWriter
from ...pipeline import Pipeline
from ...pipeline.object_storage import ObjectStore
from ...pipeline.progress_reporter import PipelineProgressReporter
from ...project import Project, Target
from ..commands.nodestream_command import NodestreamCommand
from .operation import Operation


class RunCopy(Operation):
    def __init__(
        self,
        from_target: Target,
        to_target: Target,
        project: Project,
        node_types: List[str],
        relationship_types: List[str],
    ) -> None:
        self.from_target = from_target
        self.to_target = to_target
        self.project = project
        self.schema = self.project.get_schema()
        self.node_types = node_types
        self.relationship_types = relationship_types

    async def perform(self, command: NodestreamCommand):
        pipeline = self.build_pipeline()
        await pipeline.run(reporter=PipelineProgressReporter())

    def build_pipeline(self) -> Pipeline:
        copier = self.build_copier()
        writer = self.build_writer()
        return Pipeline(
            [copier, writer], step_outbox_size=10000, object_store=ObjectStore.null()
        )

    def build_copier(self) -> Copier:
        return Copier(
            self.from_target.make_type_retriever(),
            self.schema,
            self.node_types,
            self.relationship_types,
        )

    def build_writer(self) -> GraphDatabaseWriter:
        return self.to_target.make_writer()
