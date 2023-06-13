from pathlib import Path

from cleo.helpers import argument

from ..operations import (
    CommitProjectToDisk,
    GeneratePipelineScaffold,
    GenerateProject,
    GeneratePythonScaffold,
)
from .nodestream_command import NodestreamCommand
from .shared_options import DATABASE_NAME_OPTION


class New(NodestreamCommand):
    name = "new"
    description = "Generate a New Nodestream"
    arguments = [
        argument("project", "the name of the project to create"),
    ]
    options = [DATABASE_NAME_OPTION]

    async def handle_async(self):
        project_root = Path(self.argument("project"))
        db = self.option("database")
        python_files = await self.run_operation(GeneratePythonScaffold(project_root))
        pipelines = await self.run_operation(GeneratePipelineScaffold(project_root, db))
        gen_project = GenerateProject(project_root, pipelines, python_files, db)
        project = await self.run_operation(gen_project)
        commit_to_disk = CommitProjectToDisk(project, project_root / "nodestream.yaml")
        await self.run_operation(commit_to_disk)
        self.line(f"<info>Created new project at '{project_root}'</info>")
