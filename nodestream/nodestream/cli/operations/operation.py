import re
from typing import Any

from ..commands.nodestream_command import NodestreamCommand


class Operation:
    @property
    def name(self) -> str:
        class_name = self.__class__.__name__
        return " ".join(re.findall(r"[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))", class_name))

    async def perform(self, command: NodestreamCommand) -> Any:
        raise NotImplementedError
