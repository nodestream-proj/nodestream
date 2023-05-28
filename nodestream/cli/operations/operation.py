import re

from abc import ABC, abstractmethod
from typing import Any


from cleo.commands.command import Command


class Operation(ABC):
    @property
    def name(self) -> str:
        class_name = self.__class__.__name__
        return " ".join(re.findall(r"[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))", class_name))

    @abstractmethod
    async def perform(self, command: Command) -> Any:
        raise NotImplementedError

    def has_json_logging_set(self, command):
        return command.option("json-logging")
