from ..project import Project
from .audit_printer import AuditPrinter


class Audit:
    def __init__(self, printer: AuditPrinter) -> None:
        self.printer = printer
        self.success_count = 0
        self.warning_count = 0
        self.failure_count = 0

    async def run(self, project: Project) -> None:
        pass

    def success(self, message: str) -> None:
        self.success_count += 1
        self.printer.print_success(message)

    def failure(self, message: str) -> None:
        self.failure_count += 1
        self.printer.print_failure(message)

    def warning(self, message: str) -> None:
        self.warning_count += 1
        self.printer.print_warning(message)
