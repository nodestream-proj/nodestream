import importlib
from pathlib import Path
from typing import Dict, List, Optional, Iterable

from yaml import safe_load

from ..exceptions import MissingProjectFileError
from ..utilities import pretty_print_yaml_to_file
from .pipeline_scope import PipelineScope
from .run_request import RunRequest


class Project:
    """A `Project` represents a collection of pipelines."""

    @classmethod
    def from_file(cls, path: Path) -> "Project":
        if not path.exists:
            raise MissingProjectFileError(path)

        with open(path) as fp:
            return cls.from_file_data(safe_load(fp))

    @classmethod
    def from_file_data(cls, data) -> "Project":
        scopes_data = data.pop("scopes", {})
        scopes = [
            PipelineScope.from_file_data(*scope_data)
            for scope_data in scopes_data.items()
        ]
        imports = data.pop("imports", [])
        return cls(scopes, imports)

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

    def write_to_path(self, path: Path):
        pretty_print_yaml_to_file(path, self.as_dict())

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

    def as_dict(self):
        return {
            "scopes": {
                scope.name: scope.as_dict() for scope in self.scopes_by_name.values()
            },
            "imports": self.imports,
        }
