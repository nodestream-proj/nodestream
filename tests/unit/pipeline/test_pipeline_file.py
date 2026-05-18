from pathlib import Path
from unittest.mock import patch

import pytest
from hamcrest import assert_that, contains_string, equal_to, has_length, instance_of

from nodestream.file_io import LazyLoadedArgument
from nodestream.interpreting import Interpreter
from nodestream.pipeline.pipeline_file_loader import (
    PipelineFile,
    PipelineFileContents,
    PipelineInitializationArguments,
    ScopeConfig,
    StepDefinition,
)
from nodestream.pipeline.step import PassStep
from nodestream.schema import ExpandsSchema


def test_basic_file_load():
    loader = PipelineFile(Path("tests/unit/pipeline/fixtures/simple_pipeline.yaml"))
    result = loader.load_pipeline()
    assert_that(result.steps, has_length(2))
    assert_that(result.steps[0], instance_of(PassStep))
    assert_that(result.steps[1], instance_of(PassStep))


def test_basic_file_load_with_annotations():
    loader = PipelineFile(Path("tests/unit/pipeline/fixtures/tagged_pipeline.yaml"))
    result = loader.load_pipeline(PipelineInitializationArguments(annotations=["good"]))
    assert_that(result.steps, has_length(2))
    assert_that(result.steps[0], instance_of(PassStep))
    assert_that(result.steps[1], instance_of(PassStep))


def test_init_args_for_testing():
    init_args = PipelineInitializationArguments.for_testing()
    assert_that(init_args.annotations, has_length(1))
    assert_that(init_args.annotations[0], "test")


@pytest.mark.parametrize(
    "user_annotations,step_annotations,expected",
    [
        (set(), set(), True),
        (set(), {"good"}, True),
        ({"good"}, set(), True),
        ({"good"}, {"good"}, True),
        ({"good"}, {"bad"}, False),
        ({"good", "bad"}, {"good"}, True),
        ({"good", "bad"}, {"bad"}, True),
        ({"good"}, {"good", "bad"}, True),
        ({"good", "bad"}, {"good", "bad", "ugly"}, True),
        ({"good", "bad", "ugly"}, {"good", "bad"}, True),
    ],
)
def test_should_be_loaded(user_annotations, step_annotations, expected):
    definition = StepDefinition(
        implementation_path="test", annotations=step_annotations
    )
    assert_that(definition.should_be_loaded(user_annotations), equal_to(expected))


def test_pipeline_file_loads_lazy():
    file_contents = PipelineFileContents.read_from_file(
        Path("tests/unit/pipeline/fixtures/config_pipeline.yaml")
    )
    assert_that(
        file_contents.step_definitions[0].arguments,
        equal_to({"name": LazyLoadedArgument("config", "name")}),
    )


def test_pipeline_file_loads_config_when_set():
    init_args = PipelineInitializationArguments(
        effective_config_values=ScopeConfig({"name": "test"})
    )
    file_contents = PipelineFileContents.read_from_file(
        Path("tests/unit/pipeline/fixtures/config_pipeline.yaml")
    )
    loaded_pipeline = file_contents.initialize_with_arguments(init_args)
    assert_that(
        loaded_pipeline.steps[0].name,
        equal_to("test"),
    )


def test_object_store_namespaced():
    file_path = Path("tests/unit/pipeline/fixtures/simple_pipeline.yaml")
    file_loader = PipelineFile(file_path)
    pipeline = file_loader.load_pipeline()
    assert_that(
        pipeline.object_store.namespace.prefix,
        contains_string("simple_pipeline"),
    )


def test_null_object_store_log_message(caplog):
    """Loading a pipeline with default (null) ObjectStore should log a message."""
    import logging

    file_path = Path("tests/unit/pipeline/fixtures/simple_pipeline.yaml")
    file_loader = PipelineFile(file_path)
    with caplog.at_level(logging.INFO):
        file_loader.load_pipeline()
    assert any("null ObjectStore" in msg for msg in caplog.messages)


def test_load_pipeline_for_introspection():
    file_path = Path("tests/unit/pipeline/fixtures/simple_pipeline.yaml")
    file_loader = PipelineFile(file_path)
    pipeline = file_loader.load_pipeline_for_introspection()
    # schema_only=True: only ExpandsSchema steps are loaded; PassStep doesn't expand schema
    assert_that(pipeline.steps, has_length(0))


def test_load_pipeline_for_introspection_only_loads_schema_steps():
    """Mixed pipeline: PassStep (non-schema) and Interpreter (ExpandsSchema).
    Introspection must only load the Interpreter — PassStep must never be instantiated.
    """
    file_path = Path("tests/unit/pipeline/fixtures/mixed_pipeline.yaml")
    file_loader = PipelineFile(file_path)

    original_load = StepDefinition.load_step
    loaded_impls = []

    def tracking_load(self):
        loaded_impls.append(self.implementation_path)
        return original_load(self)

    with patch.object(StepDefinition, "load_step", tracking_load):
        pipeline = file_loader.load_pipeline_for_introspection()

    # Only the Interpreter should have been instantiated
    assert loaded_impls == ["nodestream.interpreting:Interpreter"], (
        f"Expected only Interpreter to be loaded, got: {loaded_impls}"
    )
    assert_that(pipeline.steps, has_length(1))
    assert_that(pipeline.steps[0], instance_of(Interpreter))


def test_expands_schema_returns_true_for_schema_step():
    defn = StepDefinition(implementation_path="nodestream.interpreting:Interpreter")
    assert defn.expands_schema() is True


def test_expands_schema_returns_false_for_non_schema_step():
    defn = StepDefinition(implementation_path="nodestream.pipeline.step:PassStep")
    assert defn.expands_schema() is False


def test_expands_schema_returns_false_for_bad_path():
    defn = StepDefinition(implementation_path="nonexistent.module:FakeClass")
    assert defn.expands_schema() is False


def test_schema_only_flag_does_not_affect_normal_load():
    """Normal (non-introspection) load must still instantiate all steps."""
    file_path = Path("tests/unit/pipeline/fixtures/mixed_pipeline.yaml")
    file_loader = PipelineFile(file_path)
    pipeline = file_loader.load_pipeline()
    assert_that(pipeline.steps, has_length(2))
