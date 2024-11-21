import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline.normalizers import RemoveTrailingDots


@pytest.mark.parametrize(
    "input_value,expected_value",
    [
        ("some.dots....", "some.dots"),
        ("no_dots", "no_dots"),
        (".some.dots.", ".some.dots"),
        ("", ""),
        (None, None),
        ([1, 2, 3], [1, 2, 3]),
    ],
)
def test_lowercase_strings_normalization(input_value, expected_value):
    subject = RemoveTrailingDots()
    assert_that(subject.normalize(input_value), equal_to(expected_value))
