from typing import Type

from ...audits import Audit
from ..operations import InitializeProject, RunAudit
from .nodestream_command import NodestreamCommand
from .shared_options import PROJECT_FILE_OPTION


class AuditCommand(NodestreamCommand):
    audit: Type[Audit] = Audit
    options = [PROJECT_FILE_OPTION]

    async def handle_async(self):
        project = await self.run_operation(InitializeProject())
        return await self.run_operation(RunAudit(project, self.audit))
