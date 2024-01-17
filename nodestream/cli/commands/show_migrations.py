from collections import defaultdict
from typing import List, Dict

from ...project import Project
from ...schema.migrations import ProjectMigrations
from .nodestream_command import NodestreamCommand
from .shared_options import PROJECT_FILE_OPTION, TARGETS_OPTION
from ..operations import InitializeProject


class ShowMigrations(NodestreamCommand):
    name = "migrations show"
    description = (
        "List all migrations for the current project and their state on each target."
    )
    options = [PROJECT_FILE_OPTION, TARGETS_OPTION]

    def get_target_names(self, project: Project) -> List[str]:
        return self.option("target") or project.targets_by_name.keys()

    async def handle_async(self):
        project = await self.run_operation(InitializeProject())
        migrations = ProjectMigrations.from_directory(self.get_migrations_path())
        target_names = self.get_target_names(project)

        headers = ["Migration", "Operations"] + list(target_names)
        status_by_migration: Dict[str, Dict[str, str]] = defaultdict(dict)

        for target_name in target_names:
            migrator = project.get_target_by_name(target_name).make_migrator()
            async for migration, pending in migrations.determine_pending(migrator):
                status_by_migration[migration.name][target_name] = (
                    "❌" if pending else "✅"
                )

        rows = [
            [
                migration.name,
                str(len(migration.operations)),
                *status_by_migration[migration.name].values(),
            ]
            for migration in migrations.graph.get_ordered_migration_plan()
        ]

        table = self.table(headers, rows)
        table.render()
