from pathlib import Path

import pytest
from hamcrest import assert_that, equal_to, has_length, instance_of

from nodestream.file_io import LazyLoadedArgument
from nodestream.pipeline.pipeline_file_loader import (
    PipelineFile,
    PipelineFileContents,
    PipelineInitializationArguments,
    ScopeConfig,
    StepDefinition,
)
from nodestream.pipeline.step import PassStep


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
        effecitve_config_values=ScopeConfig({"name": "test"})
    )
    file_contents = PipelineFileContents.read_from_file(
        Path("tests/unit/pipeline/fixtures/config_pipeline.yaml")
    )
    loaded_pipeline = file_contents.initialize_with_arguments(init_args)
    assert_that(
        loaded_pipeline.steps[0].name,
        equal_to("test"),
    )
