from enum import Enum


class GraphObjectType(str, Enum):
    RELATIONSHIP = "RELATIONSHIP"
    NODE = "NODE"

    def __str__(self) -> str:
        return self.value
