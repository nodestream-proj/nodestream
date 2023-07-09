from typing import Type

from ...project.audits import Audit
from ..operations import InitializeProject, RunAudit
from .nodestream_command import NodestreamCommand
from .shared_options import PROJECT_FILE_OPTION


class AuditCommand(NodestreamCommand):
    audit: Type[Audit] = Audit
    options = [PROJECT_FILE_OPTION]

    async def handle_async(self):
        project = await self.run_operation(InitializeProject())
        return await self.run_operation(RunAudit(project, self.audit))

    @classmethod
    def for_audit(cls, audit_cls: Type[Audit]) -> Type["AuditCommand"]:
        class NewAuditCommand(AuditCommand):
            audit = audit_cls
            name = f"audit {audit_cls.name}"
            description = audit_cls.description

        return NewAuditCommand
