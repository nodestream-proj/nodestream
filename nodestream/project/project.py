import importlib
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, Type, TypeVar

from ..file_io import LoadsFromYamlFile, SavesToYamlFile
from ..model import (
    AggregatedIntrospectiveIngestionComponent,
    GraphSchema,
    IntrospectiveIngestionComponent,
)
from ..pipeline import Step
from .pipeline_definition import PipelineDefinition
from .pipeline_scope import PipelineScope
from .run_request import RunRequest

T = TypeVar("T", bound=Step)


class Project(
    AggregatedIntrospectiveIngestionComponent, LoadsFromYamlFile, SavesToYamlFile
):
    """A `Project` represents a collection of pipelines."""

    @classmethod
    def describe_yaml_schema(cls):
        from schema import Optional, Schema

        return Schema(
            {
                Optional("scopes"): {
                    str: PipelineScope.describe_yaml_schema(),
                },
                Optional("imports"): [str],
            }
        )

    @classmethod
    def from_file_data(cls, data) -> "Project":
        scopes_data = data.pop("scopes", {})
        scopes = [
            PipelineScope.from_file_data(*scope_data)
            for scope_data in scopes_data.items()
        ]
        imports = data.pop("imports", [])
        return cls(scopes, imports)

    def to_file_data(self):
        return {
            "scopes": {
                scope.name: scope.to_file_data()
                for scope in self.scopes_by_name.values()
            },
            "imports": self.imports,
        }

    def __init__(self, scopes: List[PipelineScope], imports: List[str]):
        self.scopes_by_name: Dict[str, PipelineScope] = {}
        for scope in scopes:
            self.add_scope(scope)
        self.imports = imports

    def ensure_modules_are_imported(self):
        for module in self.imports:
            importlib.import_module(module)

    async def run(self, request: RunRequest):
        for scope in self.scopes_by_name.values():
            await scope.run_request(request)

    def add_scope(self, scope: PipelineScope):
        self.scopes_by_name[scope.name] = scope

    def get_scopes_by_name(self, scope_name: Optional[str]) -> Iterable[PipelineScope]:
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
        for scopes in self.get_scopes_by_name(scope_name):
            scopes.delete_pipeline(
                pipeline_name,
                remove_pipeline_file=remove_pipeline_file,
                missing_ok=missing_ok,
            )

    def get_schema(self, type_overrides_file: Optional[Path] = None) -> GraphSchema:
        schema = self.generate_graph_schema()
        if type_overrides_file is not None:
            schema.apply_type_overrides_from_file(type_overrides_file)
        return schema

    def all_subordinate_components(self) -> Iterable[IntrospectiveIngestionComponent]:
        return self.scopes_by_name.values()

    def dig_for_step_of_type(
        self, step_type: Type[T]
    ) -> Iterable[Tuple[PipelineDefinition, int, T]]:
        for scope in self.scopes_by_name.values():
            for pipeline_definition in scope.pipelines_by_name.values():
                pipeline_steps = (
                    pipeline_definition.initialize_for_introspection().steps
                )
                for idx, step in enumerate(pipeline_steps):
                    if isinstance(step, step_type):
                        yield pipeline_definition, idx, step
