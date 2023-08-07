import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline import IterableExtractor, Pipeline
from nodestream.project import PipelineProgressReporter


@pytest.mark.asyncio
async def test_pipeline_progress_reporter_calls_with_reporting_frequency(mocker):
    pipeline = Pipeline([IterableExtractor(range(100))])
    reporter = PipelineProgressReporter(reporting_frequency=10, callback=mocker.Mock())
    await reporter.execute_with_reporting(pipeline)
    assert_that(reporter.callback.call_count, equal_to(10))
