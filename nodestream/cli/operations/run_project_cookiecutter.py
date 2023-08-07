from cookiecutter.main import cookiecutter

from ..commands.nodestream_command import NodestreamCommand
from .operation import Operation

PROJECT_COOKIECUTTER_URL = "gh:nodestream-proj/nodestream-project-cookiecutter"


class RunProjectCookiecutter(Operation):
    def __init__(self, project_name, project_path, database):
        self.project_name = project_name
        self.project_path = project_path
        self.database = database

    async def perform(self, _: NodestreamCommand):
        from ..application import get_version

        cookiecutter(
            PROJECT_COOKIECUTTER_URL,
            no_input=True,
            output_dir=self.project_path,
            extra_context={
                "project_name": self.project_name,
                "database": self.database,
                "nodestream_version": get_version(),
            },
        )
