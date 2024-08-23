import pytest
from hamcrest import assert_that, equal_to

from nodestream.subclass_registry import (
    AlreadyInRegistryError,
    MissingFromRegistryError,
    SubclassRegistry,
)

TEST_REGISTRY = SubclassRegistry()


@TEST_REGISTRY.connect_baseclass
class TestClass:
    pass


class ChildClass(TestClass, alias="child"):
    pass


def test_remembers_subclasses_by_name():
    assert_that(TEST_REGISTRY.get("child"), equal_to(ChildClass))
    assert_that(TEST_REGISTRY.name_for(ChildClass), equal_to("child"))


def test_raises_errors_when_invalid_named_subclass():
    with pytest.raises(
        MissingFromRegistryError, match="Did you forget to install a plugin?"
    ):
        TEST_REGISTRY.get("not_there")


def test_raises_errors_when_name_is_resused():
    with pytest.raises(AlreadyInRegistryError):

        class _(TestClass, alias="child"):
            pass


def test_does_not_raise_errors_when_name_is_resused_with_ignore_overrides():
    TEST_REGISTRY.ignore_overrides = True

    class Override(TestClass, alias="child"):
        pass

    assert_that(TEST_REGISTRY.get("child"), equal_to(Override))
