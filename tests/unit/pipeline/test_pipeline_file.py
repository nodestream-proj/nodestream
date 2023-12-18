from pathlib import Path

from hamcrest import assert_that, equal_to, has_length, instance_of

from nodestream.pipeline import PipelineFile, PipelineInitializationArguments
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


def test_unset_annotations():
    init_args = PipelineInitializationArguments(annotations=[])
    rest_of_step = {"implementation": "test"}
    step = {"annotations": ["good"], **rest_of_step}
    assert_that(init_args.step_is_tagged_properly(step), equal_to(True))
    assert_that(step, equal_to(rest_of_step))
