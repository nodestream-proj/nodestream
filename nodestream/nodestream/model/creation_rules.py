from enum import Enum


class NodeCreationRule(str, Enum):
    EAGER = "EAGER"
    MATCH_ONLY = "MATCH_ONLY"
    FUZZY = "FUZZY"

    def prevent_creation(self) -> "NodeCreationRule":
        if self == NodeCreationRule.EAGER:
            return NodeCreationRule.MATCH_ONLY
        return self


class RelationshipCreationRule(str, Enum):
    EAGER = "EAGER"
    CREATE = "CREATE"
