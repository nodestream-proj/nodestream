import pytest
import yaml
from hamcrest import assert_that, equal_to, equal_to_ignoring_whitespace

from nodestream.file_io import LazyLoadedArgument, LazyLoadedTagSafeLoader


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


DATA_TYPES_AS_YAML = """
delayed: !delayed
    value: !env 'USERNAME_FROM_ENV'
lazy: !env 'USERNAME_FROM_ENV'
regular: value
"""

DATA_AS_PYTHON = {
    "regular": "value",
    "lazy": LazyLoadedArgument("env", "USERNAME_FROM_ENV"),
    "delayed": LazyLoadedArgument(
        "delayed", LazyLoadedArgument("env", "USERNAME_FROM_ENV")
    ),
}


def test_delayed_tag_load_in():
    loaded = yaml.load(DATA_TYPES_AS_YAML, Loader=LazyLoadedTagSafeLoader)
    assert_that(loaded, DATA_AS_PYTHON)


def test_delayed_tag_load_out():
    as_yaml_str = yaml.safe_dump(DATA_AS_PYTHON, indent=2, sort_keys=True)
    assert_that(as_yaml_str, equal_to_ignoring_whitespace(DATA_TYPES_AS_YAML))


def test_delayed_tag_load_roundtrip():
    as_yaml_str = yaml.safe_dump(DATA_AS_PYTHON, indent=2, sort_keys=True)
    loaded = yaml.load(as_yaml_str, Loader=LazyLoadedTagSafeLoader)
    assert_that(loaded, DATA_AS_PYTHON)


def test_delayed_value_resolution(mocker):
    mocker.patch.dict("os.environ", {"USERNAME_FROM_ENV": "bob"})
    loaded = yaml.load(DATA_TYPES_AS_YAML, Loader=LazyLoadedTagSafeLoader)
    assert_that(loaded["lazy"].get_value(), equal_to("bob"))
    assert_that(
        loaded["delayed"].get_value(), LazyLoadedArgument("env", "USERNAME_FROM_ENV")
    )
    assert_that(loaded["delayed"].get_value().get_value(), equal_to("bob"))


def test_wrap_unloaded_tag_with_mapping_node():
    """wrap_unloaded_tag should construct a dict when the node is a MappingNode."""
    yaml_str = "config: !myconfig\n  key: value\n  other: 123\n"
    loaded = yaml.load(yaml_str, Loader=LazyLoadedTagSafeLoader)
    arg = loaded["config"]
    assert isinstance(arg, LazyLoadedArgument)
    assert_that(arg.tag, equal_to("myconfig"))
    assert_that(arg.value, equal_to({"key": "value", "other": 123}))


def test_wrap_unloaded_tag_with_sequence_node():
    """wrap_unloaded_tag should construct a list when the node is a SequenceNode."""
    yaml_str = "items: !mylist\n  - a\n  - b\n  - c\n"
    loaded = yaml.load(yaml_str, Loader=LazyLoadedTagSafeLoader)
    arg = loaded["items"]
    assert isinstance(arg, LazyLoadedArgument)
    assert_that(arg.tag, equal_to("mylist"))
    assert_that(arg.value, equal_to(["a", "b", "c"]))
