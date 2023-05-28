from typing import List
from pathlib import Path

from cleo.commands.command import Command

from ..operation import Operation
from ....project import Project, PipelineScope, PipelineDefinition


class GenerateProjectFile(Operation):
    def __init__(
        self,
        project_root: Path,
        pipelines: List[Path],
        source_modules: List[Path],
        database: str,
    ) -> None:
        self.pipelines = pipelines
        self.source_modules = source_modules
        self.project_root = project_root
        self.database = database

    async def perform(self, _: Command):
        imports = self.generate_import_directives()
        scope = self.generate_pipeline_scope()
        project = Project([scope], imports)
        project.write_to_path(self.project_root / "nodestream.yaml")

    def generate_import_directives(self) -> List[str]:
        project_imports = [
            str(path.relative_to(self.project_root).with_suffix(""))
            .replace("/", ".")
            .replace("\\", ".")
            for path in self.source_modules
            if "__init__" not in path.name
        ]
        project_imports.append(f"nodestream.databases.{self.database}")
        return project_imports

    def generate_pipeline_scope(self):
        pipeline_definitions = [
            PipelineDefinition.from_path(path.relative_to(self.project_root))
            for path in self.pipelines
        ]
        return PipelineScope("default", pipeline_definitions)
