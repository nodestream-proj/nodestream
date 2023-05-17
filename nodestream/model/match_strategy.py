from enum import Enum


class MatchStrategy(str, Enum):
    EAGER = "EAGER"
    MATCH_ONLY = "MATCH_ONLY"
    FUZZY = "FUZZY"
