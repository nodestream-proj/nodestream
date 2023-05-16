from typing import List, Dict

from .pipeline_definition import PipelineDefinition
from .run_request import RunRequest


class PipelineScope:
    def __init__(self, name: str, pipelines: List[PipelineDefinition]) -> None:
        self.name = name
        self.pipelines_by_name: Dict[str, PipelineDefinition] = {
            pipeline.name: pipeline for pipeline in pipelines
        }

    async def run_request(self, run_request: "RunRequest"):
        if (name := run_request.pipeline_name) not in self:
            return

        await run_request.execute_with_definition(self[name])

    def __iter__(self):
        return iter(self.pipelines_by_name.values())

    def __getitem__(self, pipeline_name):
        return self.pipelines_by_name[pipeline_name]

    def __contains__(self, pipeline_name):
        return pipeline_name in self.pipelines_by_name
