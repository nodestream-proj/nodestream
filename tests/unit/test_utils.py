import pytest

from nodestream.utils import StringSuggester

FRUITS = ["apple", "banana", "cherry"]


@pytest.mark.parametrize(
    "strings,input,expected",
    [
        (FRUITS, "banan", "banana"),
        (FRUITS, "cheri", "cherry"),
        (FRUITS, "appl", "apple"),
        (FRUITS, "app", "apple"),
        (FRUITS, "ban", "banana"),
        (FRUITS, "cher", "cherry"),
    ],
)
def test_suggest_closest(strings, input, expected):
    suggester = StringSuggester(strings)
    assert suggester.suggest_closest(input) == expected
