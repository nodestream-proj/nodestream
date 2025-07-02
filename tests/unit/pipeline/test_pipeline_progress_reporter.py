from unittest.mock import Mock

import pytest
from hamcrest import assert_that, equal_to

from nodestream.metrics import Metrics
from nodestream.pipeline import IterableExtractor, Pipeline, PipelineProgressReporter


@pytest.mark.asyncio
async def test_pipeline_progress_reporter_calls_with_reporting_frequency(mocker):
    pipeline = Pipeline([IterableExtractor(range(100))], 10, mocker.Mock())
    reporter = PipelineProgressReporter(reporting_frequency=10, callback=mocker.Mock())
    await pipeline.run(reporter)
    assert_that(reporter.callback.call_count, equal_to(10))


@pytest.mark.asyncio
async def test_pipeline_progress_reporter_for_testing(mocker):
    result = PipelineProgressReporter.for_testing([])
    assert_that(result.reporting_frequency, equal_to(1))
    assert_that(result.logger.name, equal_to("test"))


def test_pipeline_progress_reporter_with_time_interval_seconds(mocker):
    """Test that time_interval_seconds works correctly"""
    mock_callback = Mock()
    reporter = PipelineProgressReporter(
        time_interval_seconds=0.1, callback=mock_callback
    )

    mock_time = mocker.patch("nodestream.pipeline.progress_reporter.time.time")
    mock_time.side_effect = [0.15, 0.2]  # 150ms, 200ms

    metrics = Metrics()
    reporter.report(1, metrics)  # Should report (150ms >= 100ms from 0)
    reporter.report(2, metrics)  # Should not report (200ms - 150ms = 50ms < 100ms)

    assert_that(mock_callback.call_count, equal_to(1))


def test_pipeline_progress_reporter_without_time_interval_uses_frequency():
    """Test that None time_interval_seconds falls back to frequency-based reporting"""
    mock_callback = Mock()
    reporter = PipelineProgressReporter(
        time_interval_seconds=None, reporting_frequency=3, callback=mock_callback
    )

    metrics = Metrics()
    for i in range(1, 7):  # 1,2,3,4,5,6
        reporter.report(i, metrics)

    # Should report on multiples of 3: indices 3, 6
    assert_that(mock_callback.call_count, equal_to(2))
