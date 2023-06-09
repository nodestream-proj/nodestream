class AuditPrinter:
    """Prints audit messages to an output source. This is the default implementation that uses print to output to the console."""

    def print_success(self, message: str) -> None:
        print(message)

    def print_failure(self, message: str) -> None:
        print(message)

    def print_warning(self, message: str) -> None:
        print(message)
