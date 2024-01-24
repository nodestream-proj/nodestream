from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, Type, TypeVar

from yaml import SafeLoader

from ..file_io import (
    LazyLoadedTagSafeLoader,
    LoadsFromYaml,
    LoadsFromYamlFile,
    SavesToYamlFile,
)
from ..pipeline import Step
from ..pluggable import Pluggable
from ..schema.schema import (
    AggregatedIntrospectiveIngestionComponent,
    GraphSchema,
    IntrospectiveIngestionComponent,
)
from .pipeline_definition import PipelineDefinition
from .pipeline_scope import PipelineScope
from .plugin import PluginConfiguration
from .run_request import RunRequest
from .target import Target

T = TypeVar("T", bound=Step)


@dataclass
class Project(
    AggregatedIntrospectiveIngestionComponent, LoadsFromYamlFile, SavesToYamlFile
):
    """A `Project` represents a collection of pipelines.

    A project is the top-level object in a nodestream project.
    It contains a collection of scopes, which in turn contain a collection of pipelines.
    When interacting with nodestream programmatically, you will typically interact with a project object.
    This is where pipeline execution begins and where all data about the project is stored.
    """

    scopes_by_name: Dict[str, PipelineScope] = field(default_factory=dict)
    plugins_by_name: Dict[str, PluginConfiguration] = field(default_factory=dict)
    targets_by_name: Dict[str, Target] = field(default_factory=dict)

    @classmethod
    def read_from_file(cls, file_path: Path) -> LoadsFromYaml:
        ProjectPlugin.execute_before_project_load(file_path)
        return super().read_from_file(file_path)

    @classmethod
    def get_loader(cls) -> Type[SafeLoader]:
        """Get the YAML loader to use when reading this object from a file.

        Returns:
            The YAML loader to use when reading this object from a file.
        """
        return LazyLoadedTagSafeLoader

    @classmethod
    def describe_yaml_schema(cls):
        from schema import Optional, Schema

        return Schema(
            {
                Optional("scopes"): {
                    str: PipelineScope.describe_yaml_schema(),
                },
                Optional("plugins"): [
                    PluginConfiguration.describe_yaml_schema(),
                ],
                Optional("targets"): {
                    str: {str: object},
                },
            }
        )

    @classmethod
    def from_file_data(cls, data) -> "Project":
        """Creates a project from file data.

        The file data should be a dictionary containing a "scopes" key,
        which should be a dictionary mapping scope names to scope file data and
        should be validated by the schema returned by `describe_yaml_schema`.

        This method should not be called directly. Instead, use `NodestreamProjectFileSafeLoader.load_from_yaml_file`.

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

        plugins_list = data.pop("plugins", {})
        plugins = [
            PluginConfiguration.from_file_data(plugins_data)
            for plugins_data in plugins_list
        ]

        targets = data.pop("targets", {})
        target_cfgs = {name: Target(name, value) for name, value in targets.items()}

        project = cls(targets_by_name=target_cfgs)
        for plugin in plugins:
            project.add_plugin(plugin)
        for scope in scopes:
            project.add_scope(scope)
        ProjectPlugin.execute_activate(project)
        ProjectPlugin.execute_after_project_load(project)
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
            "plugins": [
                plugin.to_file_data() for plugin in self.plugins_by_name.values()
            ],
            "targets": {
                name: target.to_file_data()
                for name, target in self.targets_by_name.items()
            },
        }

    def get_target_by_name(self, target_name: str) -> Optional[Target]:
        """Returns the target with the given name.

        Args:
            target_name (str): The name of the target to return.

        Returns:
            Optional[Target]: The target with the given name, or None if no target was found.
        """
        return self.targets_by_name.get(target_name)

    async def run(self, request: RunRequest) -> int:
        """Takes a run request and runs the appropriate pipeline.

        Args:
            request (RunRequest): The run request to run.
        """
        pipelines_ran = 0
        for scope in self.scopes_by_name.values():
            pipelines_ran += await scope.run_request(request)
        return pipelines_ran

    async def get_snapshot_for(self, pipeline_name: str) -> list:
        """Returns the output of the pipeline with the given name to be used as a snapshot.

        This method is intended for testing purposes only. It will run the pipeline and return the results.
        The pipeline is run with the `test` annotation, so components that you want to run must be annotated with `test` or not annotated at all.

        Args:
            pipeline_name (str): The name of the pipeline to get a snapshot for.

        Returns:
            str: The snapshot of the pipeline.
        """
        run_request = RunRequest.for_testing(pipeline_name, results := [])
        await self.run(run_request)
        return results

    def add_scope(self, scope: PipelineScope):
        """Adds a scope to the project.

        Args:
            scope (PipelineScope): The scope to add.
        """
        self.scopes_by_name[scope.name] = scope

    def add_plugin(self, plugin: PluginConfiguration):
        """Adds a plugin to the project.

        Args:
            plugin (PluginConfiguration): The pluginto add.
        """

        self.plugins_by_name[plugin.name] = plugin

    def add_plugin_scope_from_pipeline_resources(self, name: str, package: str):
        """Adds a plugin from external pipeline resources to the project.

        Args:
            name (str): The plugin name.
            package (str): the package location from the plugin repo root.
        """
        plugin_from_resources = PluginConfiguration.from_resources(
            name=name, package=package
        )
        self.add_plugin_scope(name, plugin_from_resources)

    def add_plugin_scope(self, name: str, plugin: PluginConfiguration):
        project_plugin_configuration = self.plugins_by_name.get(name)
        if project_plugin_configuration:
            plugin.update_configurations(project_plugin_configuration)
            scope = plugin.make_scope()
            self.add_scope(scope)

    def get_scopes_by_name(self, scope_name: Optional[str]) -> Iterable[PipelineScope]:
        """Returns the scopes with the given name.

        If `scope_name` is None, all scopes will be returned. If no scopes are found, an empty list will be returned.
        """
        if scope_name is None:
            return self.scopes_by_name.values()

        if scope_name not in self.scopes_by_name:
            return []

        return [self.scopes_by_name[scope_name]]

    def get_all_pipelines(self) -> Iterable[PipelineDefinition]:
        """Returns all pipeline objects in the project."""
        for scope in self.scopes_by_name.values():
            for pipeline in scope.pipelines_by_name.values():
                yield pipeline

    def get_pipeline_by_name(self, pipeline_name: str) -> PipelineDefinition:
        """Returns pipeline object in the project."""
        for scope in self.scopes_by_name.values():
            pipeline = scope.pipelines_by_name.get(pipeline_name, None)
            if pipeline is not None:
                return pipeline

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

    _active: List["ProjectPlugin"] = None

    @classmethod
    def all_active(cls) -> List["ProjectPlugin"]:
        """Returns all instances of all registered plugins."""
        if cls._active is None:
            cls._active = [plugin() for plugin in cls.all()]
        return cls._active

    @classmethod
    def execute_before_project_load(cls, project_file: Path):
        """Executes the `before_project_load` method on all registered plugins."""
        for plugin in cls.all_active():
            plugin.before_project_load(project_file)

    @classmethod
    def execute_after_project_load(cls, project: Project):
        """Executes the `after_project_load` method on all registered plugins."""
        for plugin in cls.all_active():
            plugin.after_project_load(project)

    @classmethod
    def execute_activate(cls, project: Project):
        """Executes the `activate` method on all registered plugins."""
        for plugin in cls.all_active():
            plugin.activate(project)

    def before_project_load(self, project_file: Path):
        """Called before a project is loaded from disk."""
        pass

    def after_project_load(self, project: Project):
        """Called after a project is loaded from disk and after each plugin is activated."""
        pass

    def activate(self, project: Project):
        """Called when a project is loaded from disk.

        This is where the plugin should modify the project. For example, a plugin might add a new scope
        or modify an existing scope. The plugin should not modify the project file on disk. Instead, it
        should modify the project object in memory.
        """
        pass
