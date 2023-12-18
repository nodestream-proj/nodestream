import pytest
from hamcrest import assert_that, equal_to

from nodestream.file_io import LazyLoadedArgument


@pytest.mark.parametrize(
    "value,expected",
    [
        # Identity Cases
        (1, 1),
        (None, None),
        ("foo", "foo"),
        ({"foo": "bar"}, {"foo": "bar"}),
        # Simple Lazy Loaded Cases
        (LazyLoadedArgument("env", "USERNAME_FROM_ENV"), "bob"),
        # Nested Lazy Loaded Cases
        ({"foo": LazyLoadedArgument("env", "USERNAME_FROM_ENV")}, {"foo": "bob"}),
        ([LazyLoadedArgument("env", "USERNAME_FROM_ENV")], ["bob"]),
        # Deeply Nested Lazy Loaded Cases
        (
            {"foo": [LazyLoadedArgument("env", "USERNAME_FROM_ENV")]},
            {"foo": ["bob"]},
        ),
        (
            {"foo": {"bar": LazyLoadedArgument("env", "USERNAME_FROM_ENV")}},
            {"foo": {"bar": "bob"}},
        ),
    ],
)
def test_lazy_loaded_argument_resolver_strategy(mocker, value, expected):
    mocker.patch.dict("os.environ", {"USERNAME_FROM_ENV": "bob"})
    assert_that(LazyLoadedArgument.resolve_if_needed(value), equal_to(expected))
