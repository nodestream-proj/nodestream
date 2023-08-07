from importlib import resources
from typing import Dict, Iterable, List

from ..file_io import LoadsFromYaml, SavesToYaml
from ..schema.schema import (
    AggregatedIntrospectiveIngestionComponent,
    IntrospectiveIngestionComponent,
)
from .pipeline_definition import PipelineDefinition
from .run_request import RunRequest


class MissingExpectedPipelineError(ValueError):
    pass


class PipelineScope(
    AggregatedIntrospectiveIngestionComponent, LoadsFromYaml, SavesToYaml
):
    """A `PipelineScope` represents a collection of pipelines subordinate to a project."""

    def __init__(
        self, name: str, pipelines: List[PipelineDefinition], persist: bool = True
    ) -> None:
        self.persist = persist
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

    def all_subordinate_components(self) -> Iterable[IntrospectiveIngestionComponent]:
        return self.pipelines_by_name.values()

    async def run_request(self, run_request: "RunRequest"):
        """Execute a run request.

        If the pipeline does not exist, this is a no-op. Otherwise, the run request
        will be executed with the pipeline's definition. The run request will be
        executed asynchronously via its `execute_with_definition` method.

        Args:
            run_request: The run request to execute.
        """
        if (name := run_request.pipeline_name) not in self:
            return

        await run_request.execute_with_definition(self[name])

    def __getitem__(self, pipeline_name):
        return self.pipelines_by_name[pipeline_name]

    def __contains__(self, pipeline_name):
        return pipeline_name in self.pipelines_by_name

    def add_pipeline_definition(self, definition: PipelineDefinition):
        """Add a pipeline to the scope.

        Args:
            definition: The pipeline to add.
        """
        self.pipelines_by_name[definition.name] = definition

    def delete_pipeline(
        self,
        pipeline_name: str,
        remove_pipeline_file: bool = True,
        missing_ok: bool = True,
    ) -> bool:
        """Delete a pipeline from the scope.

        Args:
            pipeline_name: The name of the pipeline to delete.
            remove_pipeline_file: Whether to remove the pipeline's file from disk.
            missing_ok: Whether to raise an error if the pipeline does not exist.

        Returns:
            True if the pipeline was deleted, False if it did not exist.
        """
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

    @classmethod
    def from_resources(
        cls, name: str, package: resources.Package, persist: bool = False
    ) -> "PipelineScope":
        """Load a `PipelineScope` from a package's resources.

        Each `.yaml` file in the package's resources will be loaded as a pipeline.
        Internally, this uses `importlib.resources` to load the files and calls
        `PipelineDefinition.from_path` on each file. This means that the name of
        the pipeline will be the name of the file without the .yaml extension.

        Args:
            name: The name of the scope.
            package: The name of the package to load from.
            persist: Whether or not to save the scope when the project is saved.

        Returns:
            A `PipelineScope` instance with the pipelines defined in the package.
        """
        pipelines = [
            PipelineDefinition.from_path(f)
            for f in resources.files(package).iterdir()
            if f.suffix == ".yaml"
        ]
        return cls(name, pipelines, persist=persist)
