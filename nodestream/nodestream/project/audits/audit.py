from ...pluggable import Pluggable
from ..project import Project
from .audit_printer import AuditPrinter


class Audit(Pluggable):
    name = "unnamed"
    description = "Checks something about a project"
    entrypoint_name = "audits"

    def __init__(self, printer: AuditPrinter) -> None:
        self.printer = printer
        self.success_count = 0
        self.warning_count = 0
        self.failure_count = 0

    async def run(self, project: Project) -> None:
        pass

    def success(self, message: str) -> None:
        """Print a success message.

        The total number of successes will be tracked and is accessible via the `success_count` attribute.

        Args:
            message: The message to print.
        """
        self.success_count += 1
        self.printer.print_success(message)

    def failure(self, message: str) -> None:
        """Print a failure message.

        The total number of failures will be tracked and is accessible via the `failure_count` attribute.
        Non zero failure counts will cause the audit to fail and the command line to exit with a non-zero exit
        code. Specifically, the exit code will be the number of failures.

        Args:
            message: The message to print.
        """
        self.failure_count += 1
        self.printer.print_failure(message)

    def warning(self, message: str) -> None:
        """Print a warning message.

        The total number of warnings will be tracked and is accessible via the `warnings_count`
        attribute.
        """
        self.warning_count += 1
        self.printer.print_warning(message)
