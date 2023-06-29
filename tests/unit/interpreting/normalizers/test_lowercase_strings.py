import pytest
from hamcrest import assert_that, equal_to

from nodestream.normalizers import LowercaseStrings


@pytest.mark.parametrize(
    "input_value,expected_value",
    [
        ("SomERandomCaps", "somerandomcaps"),
        ("ALL_CAPS", "all_caps"),
        ("123456", "123456"),
        ("", ""),
        (None, None),
        ([1, 2, 3], [1, 2, 3]),
    ],
)
def test_lowercase_strings_normalization(input_value, expected_value):
    subject = LowercaseStrings()
    assert_that(subject.normalize_value(input_value), equal_to(expected_value))
