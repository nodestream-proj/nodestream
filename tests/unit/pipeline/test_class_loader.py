from typing import Optional

import pytest
from hamcrest import assert_that, equal_to, none

from nodestream.pipeline.class_loader import (
    ClassLoader,
    InvalidClassPathError,
    PipelineComponentInitializationError,
)


class SimpleClass:
    def __init__(self, argument, from_factory: Optional[str] = None) -> None:
        self.argument = argument
        self.from_factory = from_factory


class SimpleClassWithFactories(SimpleClass):
    @classmethod
    def from_file_data(cls, argument: str):
        return cls(argument=argument, from_factory="from_file_data")

    @classmethod
    def another_factory(cls, argument: str):
        return cls(argument=argument, from_factory="another_factory")


@pytest.fixture
def subject():
    return ClassLoader()


def test_class_loader_constructor(subject):
    result = subject.load_class(
        implementation="tests.unit.pipeline.test_class_loader:SimpleClass",
        arguments={"argument": "test"},
    )
    assert_that(result.__class__.__name__, equal_to("SimpleClass"))
    assert_that(result.argument, equal_to("test"))
    assert_that(result.from_factory, none())


def test_class_loader_declartive_init(subject):
    result = subject.load_class(
        implementation="tests.unit.pipeline.test_class_loader:SimpleClassWithFactories",
        arguments={"argument": "test"},
    )
    assert_that(result.__class__.__name__, equal_to("SimpleClassWithFactories"))
    assert_that(result.argument, equal_to("test"))
    assert_that(result.from_factory, equal_to("from_file_data"))


def test_class_loader_another_factory(subject):
    result = subject.load_class(
        implementation="tests.unit.pipeline.test_class_loader:SimpleClassWithFactories",
        arguments={"argument": "test"},
        factory="another_factory",
    )
    assert_that(result.__class__.__name__, equal_to("SimpleClassWithFactories"))
    assert_that(result.argument, equal_to("test"))
    assert_that(result.from_factory, equal_to("another_factory"))


def test_class_loader_invalid_module(subject):
    with pytest.raises(InvalidClassPathError):
        subject.load_class(implementation="tests.does_not_exist:ClassNameDoesNotMatter")


def test_class_loader_invalid_path_format(subject):
    with pytest.raises(InvalidClassPathError):
        subject.load_class(implementation="tests.unit.pipeline.test_class_loader")


def test_class_loader_invalid_path_missing_class(subject):
    with pytest.raises(InvalidClassPathError):
        subject.load_class(
            implementation="tests.unit.pipeline.test_class_loader:ClassDoesNotExist"
        )


def test_class_loader_invalid_path_invalid_arugment(subject):
    with pytest.raises(PipelineComponentInitializationError):
        subject.load_class(
            implementation="tests.unit.pipeline.test_class_loader:SimpleClass",
            arguments={"not_a_valid_argument": True},
        )


def test_class_loader_invalid_type_constraint(subject):
    with pytest.raises(TypeError):
        subject.class_constratint = SimpleClassWithFactories
        subject.load_class(
            implementation="tests.unit.pipeline.test_class_loader:SimpleClass",
            arguments={"argument": "test"},
        )


def test_class_loader_valid_subclass(subject):
    subject.class_constratint = SimpleClass
    subject.load_class(
        implementation="tests.unit.pipeline.test_class_loader:SimpleClassWithFactories",
        arguments={"argument": "test"},
        factory="another_factory",
    )
