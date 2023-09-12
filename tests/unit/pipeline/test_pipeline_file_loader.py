from pathlib import Path

from hamcrest import assert_that, has_length, instance_of

from nodestream.pipeline import PipelineFileLoader, PipelineInitializationArguments
from nodestream.pipeline.step import PassStep
from nodestream.pipeline.scope_config import ScopeConfig


def test_basic_file_load():
    loader = PipelineFileLoader(
        Path("tests/unit/pipeline/fixtures/simple_pipeline.yaml")
    )
    result = loader.load_pipeline()
    assert_that(result.steps, has_length(2))
    assert_that(result.steps[0], instance_of(PassStep))
    assert_that(result.steps[1], instance_of(PassStep))


def test_basic_file_load_with_annotations():
    loader = PipelineFileLoader(
        Path("tests/unit/pipeline/fixtures/tagged_pipeline.yaml")
    )
    result = loader.load_pipeline(PipelineInitializationArguments(annotations=["good"]))
    assert_that(result.steps, has_length(2))
    assert_that(result.steps[0], instance_of(PassStep))
    assert_that(result.steps[1], instance_of(PassStep))


def test_init_args_for_testing():
    init_args = PipelineInitializationArguments.for_testing()
    assert_that(init_args.annotations, has_length(1))
    assert_that(init_args.annotations[0], "test")

def test_config_file_load():
    loader = PipelineFileLoader(
        Path("tests/unit/pipeline/fixtures/config_pipeline.yaml")
    )
    result = loader.load_pipeline(config=ScopeConfig({"TestValue": "nodestream.pipeline.step:PassStep"}))
    assert_that(result.steps, has_length(1))
    assert_that(result.steps[0], instance_of(PassStep))