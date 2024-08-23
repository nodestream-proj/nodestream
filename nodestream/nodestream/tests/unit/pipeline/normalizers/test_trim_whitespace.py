import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline.normalizers import TrimWhitespace


@pytest.mark.parametrize(
    "input_value,expected_value",
    [
        (" some space   ", "some space"),
        ("right only   ", "right only"),
        ("  left only.", "left only."),
        ("", ""),
        (None, None),
        ([1, 2, 3], [1, 2, 3]),
    ],
)
def test_lowercase_strings_normalization(input_value, expected_value):
    subject = TrimWhitespace()
    assert_that(subject.normalize_value(input_value), equal_to(expected_value))
