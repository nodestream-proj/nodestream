from typing import List
from pathlib import Path

from cleo.commands.command import Command

from ..operation import Operation


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

    async def perform(self, command: Command):
        return await super().perform(command)
