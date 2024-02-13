from difflib import SequenceMatcher
from typing import Iterable


class StringSuggester:
    """A utility for suggesting alternative strings based on similarity to a given string."""

    def __init__(self, all_strings: Iterable[str]):
        self.all_strings = list(all_strings)

    def suggest_closest(self, string: str) -> str:
        """Suggest the string that is most similar to the given string.

        Args:
            string: The string to compare to the list of strings.

        Returns:
            The string from the list of strings that is most similar to the given string.
        """
        return max(
            self.all_strings,
            key=lambda s: SequenceMatcher(None, string, s).ratio(),
        )
