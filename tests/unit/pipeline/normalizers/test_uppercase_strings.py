import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline.normalizers import UppercaseStrings


@pytest.mark.parametrize(
    "input_value,expected_value",
    [
        ("SomERandomCaps", "SOMERANDOMCAPS"),
        ("ALL_CAPS", "ALL_CAPS"),
        ("all_lower", "ALL_LOWER"),
        ("123456", "123456"),
        ("", ""),
        (None, None),
        ([1, 2, 3], [1, 2, 3]),
    ],
)
def test_uppercase_strings_normalization(input_value, expected_value):
    subject = UppercaseStrings()
    assert_that(subject.normalize(input_value), equal_to(expected_value))
