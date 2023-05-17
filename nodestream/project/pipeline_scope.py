from typing import List, Dict

from .pipeline_definition import PipelineDefinition
from .run_request import RunRequest


class PipelineScope:
    """A `PipelineScope` represents a collection of pipelines subordinate to a project."""

    def __init__(self, name: str, pipelines: List[PipelineDefinition]) -> None:
        self.name = name
        self.pipelines_by_name: Dict[str, PipelineDefinition] = {
            pipeline.name: pipeline for pipeline in pipelines
        }

    @classmethod
    def from_file_data(cls, scope_name, file_data):
        pipelines_data = file_data.pop("pipelines", [])
        annotations = file_data.pop("annotations", {})
        pipelines = [
            PipelineDefinition.from_file_data(pipeline_data, annotations)
            for pipeline_data in pipelines_data
        ]
        return cls(scope_name, pipelines)

    async def run_request(self, run_request: "RunRequest"):
        if (name := run_request.pipeline_name) not in self:
            return

        await run_request.execute_with_definition(self[name])

    def __getitem__(self, pipeline_name):
        return self.pipelines_by_name[pipeline_name]

    def __contains__(self, pipeline_name):
        return pipeline_name in self.pipelines_by_name
