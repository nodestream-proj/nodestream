from typing import Type

from ...project import Project
from ...project.audits import Audit, AuditPrinter
from ..commands.nodestream_command import NodestreamCommand
from .operation import Operation

DEFAULT_SCOPE_NAME = "default"


class CommandAuditPrinter(AuditPrinter):
    def __init__(self, command: NodestreamCommand):
        self.command = command

    def print_success(self, message: str) -> None:
        self.command.line(f"<info>{message}</info>")

    def print_failure(self, message: str) -> None:
        self.command.line(f"<error>{message}</error>")

    def print_warning(self, message: str) -> None:
        self.command.line(f"<comment>{message}</comment>")


class RunAudit(Operation):
    def __init__(self, project: Project, audit_cls: Type[Audit]) -> None:
        self.project = project
        self.audit_cls = audit_cls

    def make_audit(self, command: NodestreamCommand) -> Audit:
        return self.audit_cls(CommandAuditPrinter(command))

    async def perform(self, command: NodestreamCommand):
        audit = self.make_audit(command)
        await audit.run(self.project)
        return audit.failure_count
