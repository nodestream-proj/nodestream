from collections import defaultdict
from typing import Dict, Iterable, List

from ...project import Project
from ...schema.migrations import ProjectMigrations
from .nodestream_command import NodestreamCommand
from .shared_options import PROJECT_FILE_OPTION, TARGETS_OPTION


class ShowMigrations(NodestreamCommand):
    name = "migrations show"
    description = (
        "List all migrations for the current project and their state on each target."
    )
    options = [PROJECT_FILE_OPTION, TARGETS_OPTION]

    def get_target_names(self, project: Project) -> List[str]:
        return self.option("target") or project.targets_by_name.keys()

    async def get_migration_status_by_target(
        self,
        project: Project,
        target_names: Iterable[str],
        migrations: ProjectMigrations,
    ) -> Dict[str, Dict[str, str]]:
        migration_status_by_target = defaultdict(dict)

        for target_name in target_names:
            target = project.get_target_by_name(target_name)
            migrator = target.make_migrator()
            async for migration, pending in migrations.determine_pending(migrator):
                migration_status_by_target[migration.name][target.name] = pending

        return migration_status_by_target

    def make_migration_status_row(self, migration_name: str, statuses: Dict[str, bool]):
        def format_status(pending: bool):
            return "❌" if pending else "✅"

        return [
            migration_name,
            *[format_status(statuses[target]) for target in sorted(statuses)],
        ]

    async def generate_output_table(self):
        project = self.get_project()
        migrations = self.get_migrations()
        target_names = list(sorted(self.get_target_names(project)))
        headers = ["Migration"] + target_names
        status_by_migration = await self.get_migration_status_by_target(
            project, target_names, migrations
        )
        rows = [
            self.make_migration_status_row(
                migration_name, status_by_migration[migration_name]
            )
            for migration_name in sorted(status_by_migration)
        ]
        return headers, rows

    async def handle_async(self):
        headers, rows = await self.generate_output_table()
        table = self.table(headers, rows)
        table.render()
