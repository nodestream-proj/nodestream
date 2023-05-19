from enum import Enum


class MatchStrategy(str, Enum):
    EAGER = "EAGER"
    MATCH_ONLY = "MATCH_ONLY"
    FUZZY = "FUZZY"

    def prevent_creation(self) -> "MatchStrategy":
        if self == MatchStrategy.EAGER:
            return MatchStrategy.MATCH_ONLY
        return self
