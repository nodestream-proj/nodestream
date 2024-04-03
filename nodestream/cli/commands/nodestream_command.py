import asyncio
from pathlib import Path
from typing import TYPE_CHECKING

from cleo.commands.command import Command
from cleo.io.outputs.output import Verbosity

from ...pluggable import Pluggable
from ...project import Project
from ...schema.migrations import ProjectMigrations

if TYPE_CHECKING:
    from ..operations import Operation

DEFAULT_PROJECT_FILE = Path("nodestream.yaml")


class NodestreamCommand(Command, Pluggable):
    entrypoint_name = "commands"

    def handle(self):
        return asyncio.run(self.handle_async())

    async def handle_async(self):
        raise NotImplementedError

    async def run_operation(self, operation: "Operation"):
        self.line(
            f"<info>Running: {operation.name}</info>", verbosity=Verbosity.VERBOSE
        )
        return await operation.perform(self)

    def get_project_path(self) -> Path:
        path = self.option("project")
        return DEFAULT_PROJECT_FILE if path is None else Path(path)

    def get_migrations_path(self) -> Path:
        return self.get_project_path().parent / "migrations"

    def get_project(self) -> Project:
        return Project.read_from_file(self.get_project_path())

    def get_migrations(self) -> ProjectMigrations:
        return ProjectMigrations.from_directory(self.get_migrations_path())

    @property
    def has_json_logging_set(self) -> bool:
        return self.option("json")

    @property
    def scope(self) -> str:
        return self.option("scope")

    @property
    def is_verbose(self) -> bool:
        return self.io.output.is_verbose()

    @property
    def is_very_verbose(self) -> bool:
        return self.io.output.is_very_verbose()
