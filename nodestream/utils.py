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
        best_string = self.all_strings[0]
        best_score = SequenceMatcher(None, string, best_string).ratio()
        for s in self.all_strings[1:]:
            new_score = SequenceMatcher(None, string, s).ratio()
            if new_score > best_score:
                best_string = s
                best_score = new_score
        return best_string
