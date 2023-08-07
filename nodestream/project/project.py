from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, Type, TypeVar

from ..file_io import LoadsFromYamlFile, SavesToYamlFile
from ..pipeline import Step
from ..pluggable import Pluggable
from ..schema.schema import (
    AggregatedIntrospectiveIngestionComponent,
    GraphSchema,
    IntrospectiveIngestionComponent,
)
from .pipeline_definition import PipelineDefinition
from .pipeline_scope import PipelineScope
from .run_request import RunRequest

T = TypeVar("T", bound=Step)


class Project(
    AggregatedIntrospectiveIngestionComponent, LoadsFromYamlFile, SavesToYamlFile
):
    """A `Project` represents a collection of pipelines.

    A project is the top-level object in a nodestream project.
    It contains a collection of scopes, which in turn contain a collection of pipelines.
    When interacting with nodestream programmatically, you will typically interact with a project object.
    This is where pipeline execution begins and where all data about the project is stored.
    """

    @classmethod
    def describe_yaml_schema(cls):
        from schema import Optional, Schema

        return Schema(
            {
                Optional("scopes"): {
                    str: PipelineScope.describe_yaml_schema(),
                },
            }
        )

    @classmethod
    def from_file_data(cls, data) -> "Project":
        """Creates a project from file data.

        The file data should be a dictionary containing a "scopes" key,
        which should be a dictionary mapping scope names to scope file data and
        should be validated by the schema returned by `describe_yaml_schema`.

        This method should not be called directly. Instead, use `LoadsFromYamlFile.load_from_yaml_file`.

        Args:
            data (Dict): The file data to create the project from.

        Returns:
            Project: The project created from the file data.
        """
        scopes_data = data.pop("scopes", {})
        scopes = [
            PipelineScope.from_file_data(*scope_data)
            for scope_data in scopes_data.items()
        ]
        project = cls(scopes)
        for plugin in ProjectPlugin.all():
            plugin().activate(project)
        return project

    def to_file_data(self) -> dict:
        """Returns the file data for the project.

        This method should not be called directly. Instead, use `SavesToYamlFile.save_to_yaml_file`.

        Returns:
            Dict: The file data for the project.
        """
        return {
            "scopes": {
                scope.name: scope.to_file_data()
                for scope in self.scopes_by_name.values()
                if scope.persist
            },
        }

    def __init__(self, scopes: List[PipelineScope]):
        self.scopes_by_name: Dict[str, PipelineScope] = {}
        for scope in scopes:
            self.add_scope(scope)

    async def run(self, request: RunRequest):
        """Takes a run request and runs the appropriate pipeline.

        Args:
            request (RunRequest): The run request to run.
        """
        for scope in self.scopes_by_name.values():
            await scope.run_request(request)

    def add_scope(self, scope: PipelineScope):
        """Adds a scope to the project.

        Args:
            scope (PipelineScope): The scope to add.
        """
        self.scopes_by_name[scope.name] = scope

    def get_scopes_by_name(self, scope_name: Optional[str]) -> Iterable[PipelineScope]:
        """Returns the scopes with the given name.

        If `scope_name` is None, all scopes will be returned. If no scopes are found, an empty list will be returned.
        """
        if scope_name is None:
            return self.scopes_by_name.values()

        if scope_name not in self.scopes_by_name:
            return []

        return [self.scopes_by_name[scope_name]]

    def delete_pipeline(
        self,
        scope_name: Optional[str],
        pipeline_name: str,
        remove_pipeline_file: bool = True,
        missing_ok: bool = True,
    ):
        """Deletes a pipeline from the project.

        Args:
            scope_name (Optional[str]): The name of the scope containing the pipeline. If None, all scopes will be searched.
            pipeline_name (str): The name of the pipeline to delete.
            remove_pipeline_file (bool, optional): Whether to remove the pipeline file from disk. Defaults to True.
            missing_ok (bool, optional): Whether to raise an error if the pipeline is not found. Defaults to True.

        Raises:
            ValueError: If the pipeline is not found and `missing_ok` is False.
        """
        for scopes in self.get_scopes_by_name(scope_name):
            scopes.delete_pipeline(
                pipeline_name,
                remove_pipeline_file=remove_pipeline_file,
                missing_ok=missing_ok,
            )

    def get_schema(self, type_overrides_file: Optional[Path] = None) -> GraphSchema:
        """Returns a `GraphSchema` representing the project.

        If a `type_overrides_file` is provided, the schema will be updated with the overrides.
        Since nodestream does not know about the types of the data in the project, this is the
        only way to provide type information to the schema where it is not available natively.

        Every pipeline in the project will be loaded and introspected to build the schema.

        Args:
            type_overrides_file (Optional[Path], optional): A path to a YAML file containing type overrides. Defaults to None.

        Returns:
            GraphSchema: The schema representing the project.
        """
        schema = self.generate_graph_schema()
        if type_overrides_file is not None:
            schema.apply_type_overrides_from_file(type_overrides_file)
        return schema

    def all_subordinate_components(self) -> Iterable[IntrospectiveIngestionComponent]:
        return self.scopes_by_name.values()

    def dig_for_step_of_type(
        self, step_type: Type[T]
    ) -> Iterable[Tuple[PipelineDefinition, int, T]]:
        """Yields all steps of the given type in the project.

        Yields tuples of (pipeline_definition, step_index, step).

        Args:
            step_type (Type[T]): The type of step to look for.

        Yields:
            Tuple[PipelineDefinition, int, T]: The steps of the given type and their locations.
        """
        for scope in self.scopes_by_name.values():
            for pipeline_definition in scope.pipelines_by_name.values():
                pipeline_steps = (
                    pipeline_definition.initialize_for_introspection().steps
                )
                for idx, step in enumerate(pipeline_steps):
                    if isinstance(step, step_type):
                        yield pipeline_definition, idx, step


class ProjectPlugin(Pluggable):
    """A plugin that can be used to modify a project.

    Plugins are used to add functionality to projects. They are typically used to add new pipeline scopes
    or to modify existing scopes and pipelines. Plugins are activated when a project is loaded from disk.

    Plugins are registered by subclassing `ProjectPlugin` and implementing the `activate` method.
    Additionally, they must be use the `entry_points` mechanism to register themselves as a plugin.
    """

    entrypoint_name = "projects"

    def activate(self, project: Project):
        """Called when a project is loaded from disk.

        This is where the plugin should modify the project. For example, a plugin might add a new scope
        or modify an existing scope. The plugin should not modify the project file on disk. Instead, it
        should modify the project object in memory.
        """
        raise NotImplementedError
