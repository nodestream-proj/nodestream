import json
from abc import ABC, abstractmethod
from typing import Iterable, List, Optional, Tuple

from ...project import PipelineDefinition, Project
from ..commands.nodestream_command import NodestreamCommand
from .operation import Operation


class OutputFormat(ABC):
    def __init__(self, command: NodestreamCommand) -> None:
        self.command = command

    @abstractmethod
    def output(self, matching_pipelines: Iterable[Tuple[str, PipelineDefinition]]):
        raise NotImplementedError


class TableOutputFormat(OutputFormat):
    HEADERS = ["scope", "name", "file", "annotations"]

    def get_data_rows(
        self, matching_pipelines: Iterable[Tuple[str, PipelineDefinition]]
    ) -> List[list]:
        return [
            [
                scope,
                definition.name,
                str(definition.file_path),
                "".join(definition.annotations.keys()),
            ]
            for scope, definition in matching_pipelines
        ]

    def output(self, matching_pipelines: Iterable[Tuple[str, PipelineDefinition]]):
        table = self.command.table(self.HEADERS, self.get_data_rows(matching_pipelines))
        table.render()


class JsonOutputFormat(OutputFormat):
    def output(self, matching_pipelines: Iterable[Tuple[str, PipelineDefinition]]):
        json_data = [
            pipeline.to_file_data(verbose=self.command.is_verbose)
            for _, pipeline in matching_pipelines
        ]
        self.command.write(json.dumps(json_data))


class ShowPipelines(Operation):
    def __init__(
        self, project: Project, scope_name: Optional[str], use_json: bool = False
    ) -> None:
        self.project = project
        self.scope_name = scope_name
        self.use_json = use_json

    async def perform(self, command: NodestreamCommand):
        self.get_output_format(command).output(self.get_matching_pipelines())

    def get_output_format(self, command: NodestreamCommand):
        cls = JsonOutputFormat if self.use_json else TableOutputFormat
        return cls(command)

    def get_matching_pipelines(self) -> Iterable[Tuple[str, PipelineDefinition]]:
        if self.scope_name:
            scopes = [self.project.scopes_by_name[self.scope_name]]
        else:
            scopes = self.project.scopes_by_name.values()

        for scope in scopes:
            for pipeline in scope.pipelines_by_name.values():
                yield scope.name, pipeline
