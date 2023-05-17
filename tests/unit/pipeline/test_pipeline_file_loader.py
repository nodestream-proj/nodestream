from pathlib import Path

from hamcrest import assert_that, has_length, instance_of

from nodestream.pipeline import PipelineFileLoader, PipelineInitializationArguments
from nodestream.pipeline.step import PassStep


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
