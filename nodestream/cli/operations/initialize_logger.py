from typing import Any

from cleo.commands.command import Command

from ...logging import configure_logging_with_json_defaults
from .operation import Operation


class InitializeLogger(Operation):
    async def perform(self, command: Command) -> Any:
        if self.has_json_logging_set(command):
            configure_logging_with_json_defaults()
