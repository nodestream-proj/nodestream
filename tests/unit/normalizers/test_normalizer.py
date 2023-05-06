from typing import Any

import pytest
from hamcrest import assert_that, equal_to, instance_of

from nodestream.normalizers import InvalidFlagError, Normalizer


class Test(Normalizer, name="test"):
    def normalize_value(self, value: Any) -> Any:
        return value


def test_arugment_flag():
    assert_that(Test.arugment_flag(), equal_to("do_test"))


def test_by_flag_name():
    assert_that(Normalizer.by_flag_name("do_test"), instance_of(Test))


def test_by_flag_name_missing():
    with pytest.raises(InvalidFlagError):
        Normalizer.by_flag_name("do_not_there_at_all")


def test_by_invalid_flag_name():
    with pytest.raises(InvalidFlagError):
        Normalizer.by_flag_name("not_a_valid_one")
