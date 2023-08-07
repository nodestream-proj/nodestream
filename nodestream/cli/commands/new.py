from pathlib import Path

from cleo.helpers import argument

from ..operations import RunProjectCookiecutter
from .nodestream_command import NodestreamCommand
from .shared_options import DATABASE_NAME_OPTION


class New(NodestreamCommand):
    name = "new"
    description = "Generate a new nodestream project"
    arguments = [
        argument("name", "the name of the project to create"),
        argument("path", "the location of the project to create", optional=True),
    ]
    options = [DATABASE_NAME_OPTION]

    async def handle_async(self):
        project_name = self.argument("name")
        project_path = self.argument("path")
        project_path = Path(self.argument("path")) if project_path else Path.cwd()
        db = self.option("database")
        generate = RunProjectCookiecutter(project_name, project_path, db)
        await self.run_operation(generate)
