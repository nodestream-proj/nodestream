from pathlib import Path

from cleo.helpers import argument, option

from .async_command import AsyncCommand
from ..operations.generation import (
    GeneratePipelineScaffold,
    GenerateProjectFile,
    GeneratePythonScaffold,
)


class NewProject(AsyncCommand):
    name = "new"
    description = "Generate a New Nodestream"
    arguments = [
        argument("project", "the name of the project to create"),
    ]
    options = [
        option("db", "d", "The Database to Configre", flag=False, default="neo4j"),
    ]

    async def handle_async(self):
        project_root = Path(self.argument("project"))
        db = self.option("db")
        python_files = await self.run_operation(GeneratePythonScaffold(project_root))
        pipelines = await self.run_operation(GeneratePipelineScaffold(project_root, db))
        await self.run_operation(
            GenerateProjectFile(project_root, pipelines, python_files, db)
        )
