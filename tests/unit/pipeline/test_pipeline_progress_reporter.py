import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline import IterableExtractor, Pipeline, PipelineProgressReporter


@pytest.mark.asyncio
async def test_pipeline_progress_reporter_calls_with_reporting_frequency(mocker):
    pipeline = Pipeline([IterableExtractor(range(100))])
    reporter = PipelineProgressReporter(reporting_frequency=10, callback=mocker.Mock())
    await pipeline.run(reporter)
    assert_that(reporter.callback.call_count, equal_to(10))


@pytest.mark.asyncio
async def test_pipeline_progress_reporter_for_testing(mocker):
    result = PipelineProgressReporter.for_testing([])
    assert_that(result.reporting_frequency, equal_to(1))
    assert_that(result.logger.name, equal_to("test"))
