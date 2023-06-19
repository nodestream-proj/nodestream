from typing import Dict, Iterable, List

from ..exceptions import MissingExpectedPipelineError
from ..file_io import LoadsFromYaml, SavesToYaml
from ..model import (
    AggregatedIntrospectiveIngestionComponent,
    IntrospectiveIngestionComponent,
)
from .pipeline_definition import PipelineDefinition
from .run_request import RunRequest


class PipelineScope(
    AggregatedIntrospectiveIngestionComponent, LoadsFromYaml, SavesToYaml
):
    """A `PipelineScope` represents a collection of pipelines subordinate to a project."""

    def __init__(self, name: str, pipelines: List[PipelineDefinition]) -> None:
        self.name = name
        self.pipelines_by_name: Dict[str, PipelineDefinition] = {}
        for pipeline in pipelines:
            self.add_pipeline_definition(pipeline)

    @classmethod
    def from_file_data(cls, scope_name, file_data):
        pipelines_data = file_data.pop("pipelines", [])
        annotations = file_data.pop("annotations", {})
        pipelines = [
            PipelineDefinition.from_file_data(pipeline_data, annotations)
            for pipeline_data in pipelines_data
        ]
        return cls(scope_name, pipelines)

    @classmethod
    def describe_yaml_schema(cls):
        from schema import Optional, Or, Schema

        return Schema(
            {
                Optional("pipelines"): [
                    PipelineDefinition.describe_yaml_schema(),
                ],
                Optional("annotations"): {
                    str: Or(str, int, float, bool),
                },
            }
        )

    def to_file_data(self):
        return {
            "pipelines": [ppl.to_file_data() for ppl in self.pipelines_by_name.values()]
        }

    async def run_request(self, run_request: "RunRequest"):
        if (name := run_request.pipeline_name) not in self:
            return

        await run_request.execute_with_definition(self[name])

    def __getitem__(self, pipeline_name):
        return self.pipelines_by_name[pipeline_name]

    def __contains__(self, pipeline_name):
        return pipeline_name in self.pipelines_by_name

    def add_pipeline_definition(self, definition: PipelineDefinition):
        self.pipelines_by_name[definition.name] = definition

    def delete_pipeline(
        self,
        pipeline_name: str,
        remove_pipeline_file: bool = True,
        missing_ok: bool = True,
    ) -> bool:
        definition = self.pipelines_by_name.get(pipeline_name)
        if definition is None:
            if not missing_ok:
                raise MissingExpectedPipelineError(
                    "Attempted to delete pipeline that did not exist"
                )
            else:
                return False

        del self.pipelines_by_name[pipeline_name]
        if remove_pipeline_file:
            definition.remove_file(missing_ok=missing_ok)

        return True

    def all_subordinate_components(self) -> Iterable[IntrospectiveIngestionComponent]:
        return self.pipelines_by_name.values()
