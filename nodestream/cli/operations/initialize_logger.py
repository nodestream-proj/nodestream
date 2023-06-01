from typing import Any

from ...logging import configure_logging_with_json_defaults
from ..commands.nodestream_command import NodestreamCommand
from .operation import Operation


class InitializeLogger(Operation):
    async def perform(self, command: NodestreamCommand) -> Any:
        if command.has_json_logging_set:
            configure_logging_with_json_defaults()
