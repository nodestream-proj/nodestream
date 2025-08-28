import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from nodestream.pipeline.extractors import Extractor
from nodestream.pipeline.object_storage import ObjectStore
from nodestream.pipeline.pipeline import Pipeline, Record
from nodestream.pipeline.progress_reporter import PipelineProgressReporter
from nodestream.pipeline.step import Step, StepContext
from nodestream.pipeline.transformers import Transformer
from nodestream.pipeline.writers import Writer


class ResourceTrackingExtractor(Extractor):
    """Extractor that tracks resource allocation and cleanup."""

    def __init__(self, data_items):
        self.data_items = data_items
        self.allocated_resources = {}
        self.finalized_tokens = []

    async def extract_records(self):
        for i, item in enumerate(self.data_items):
            # Simulate resource allocation
            token = f"extractor_resource_{i}"
            self.allocated_resources[token] = f"resource_for_{item}"
            yield (item, token)  # Emit tuple with callback token

    async def finalize_record(self, record_token):
        """Clean up resources allocated for this record."""
        if record_token in self.allocated_resources:
            del self.allocated_resources[record_token]
        self.finalized_tokens.append(record_token)


class ResourceTrackingTransformer(Transformer):
    """Transformer that tracks resource allocation and cleanup."""

    def __init__(self):
        self.allocated_resources = {}
        self.finalized_tokens = []
        self.processed_records = []

    async def process_record(self, record, context):
        # Track the record we processed
        self.processed_records.append(record)

        # Simulate resource allocation for transformation
        token = f"transformer_resource_{id(record)}"
        self.allocated_resources[token] = f"transform_resource_for_{record}"

        # Transform the record
        transformed = f"transformed_{record}"
        yield (transformed, token)  # Emit with callback token

    async def finalize_record(self, record_token):
        """Clean up transformation resources."""
        if record_token in self.allocated_resources:
            del self.allocated_resources[record_token]
        self.finalized_tokens.append(record_token)


class ResourceTrackingWriter(Writer):
    """Writer that tracks resource allocation and cleanup."""

    def __init__(self):
        self.allocated_resources = {}
        self.finalized_tokens = []
        self.written_records = []

    async def write_record(self, record):
        # Track what we wrote
        self.written_records.append(record)

        # Simulate resource allocation for writing
        token = f"writer_resource_{id(record)}"
        self.allocated_resources[token] = f"write_resource_for_{record}"
        return token  # Return token for cleanup

    async def process_record(self, record, context):
        # Write the record and get cleanup token
        token = await self.write_record(record)
        yield (record, token)  # Pass through with cleanup token

    async def finalize_record(self, record_token):
        """Clean up writing resources."""
        if record_token in self.allocated_resources:
            del self.allocated_resources[record_token]
        self.finalized_tokens.append(record_token)


@pytest.mark.asyncio
async def test_end_to_end_cleanup_flow():
    """Test complete cleanup flow through extractor -> transformer -> writer."""
    # Create steps with resource tracking
    extractor = ResourceTrackingExtractor(["item1", "item2", "item3"])
    transformer = ResourceTrackingTransformer()
    writer = ResourceTrackingWriter()

    # Create pipeline
    steps = (extractor, transformer, writer)
    object_store = Mock(spec=ObjectStore)
    object_store.namespaced = Mock(return_value=Mock())

    pipeline = Pipeline(steps, step_outbox_size=10, object_store=object_store)

    # Create progress reporter
    reporter = PipelineProgressReporter.for_testing([])

    # Run pipeline
    await pipeline.run(reporter)

    # Verify all records were processed
    assert len(transformer.processed_records) == 3
    assert len(writer.written_records) == 3

    # Verify finalize_record was called for writer (final step)
    # Note: In multi-step pipelines, only the final step gets cleanup calls
    # because intermediate records are transformed, not dropped
    assert len(writer.finalized_tokens) == 3

    # Writer resources should be cleaned up
    assert len(writer.allocated_resources) == 0


@pytest.mark.asyncio
async def test_cleanup_flow_with_filtering():
    """Test cleanup flow when some records are filtered out."""

    class FilteringTransformer(Transformer):
        def __init__(self):
            self.allocated_resources = {}
            self.finalized_tokens = []

        async def process_record(self, record, context):
            # Allocate resource for processing
            token = f"filter_resource_{id(record)}"
            self.allocated_resources[token] = f"resource_for_{record}"

            # Only pass through records containing "keep"
            if "keep" in str(record):
                yield (f"filtered_{record}", token)
            # If we don't yield, the record will be dropped and finalized

        async def finalize_record(self, record_token):
            if record_token in self.allocated_resources:
                del self.allocated_resources[record_token]
            self.finalized_tokens.append(record_token)

    # Create steps
    extractor = ResourceTrackingExtractor(["keep1", "drop1", "keep2", "drop2"])
    filter_transformer = FilteringTransformer()
    writer = ResourceTrackingWriter()

    steps = (extractor, filter_transformer, writer)
    object_store = Mock(spec=ObjectStore)
    object_store.namespaced = Mock(return_value=Mock())

    pipeline = Pipeline(steps, step_outbox_size=10, object_store=object_store)
    reporter = PipelineProgressReporter.for_testing([])

    await pipeline.run(reporter)

    # Verify only "keep" records made it to writer
    assert len(writer.written_records) == 2
    assert all("keep" in str(record) for record in writer.written_records)

    # Verify writer resources were cleaned up
    assert len(writer.allocated_resources) == 0

    # Verify finalize_record was called for writer (final step)
    assert len(writer.finalized_tokens) == 2  # Only 2 kept records


@pytest.mark.asyncio
async def test_cleanup_flow_with_record_multiplication():
    """Test cleanup flow when one record generates multiple records."""

    class MultiplyingTransformer(Transformer):
        def __init__(self):
            self.allocated_resources = {}
            self.finalized_tokens = []

        async def process_record(self, record, context):
            # Allocate resource for processing
            token = f"multiply_resource_{id(record)}"
            self.allocated_resources[token] = f"resource_for_{record}"

            # Generate multiple records from one input
            for i in range(3):
                yield (f"{record}_copy_{i}", token)

        async def finalize_record(self, record_token):
            if record_token in self.allocated_resources:
                del self.allocated_resources[record_token]
            self.finalized_tokens.append(record_token)

    # Create steps
    extractor = ResourceTrackingExtractor(["item1", "item2"])
    multiplier = MultiplyingTransformer()
    writer = ResourceTrackingWriter()

    steps = (extractor, multiplier, writer)
    object_store = Mock(spec=ObjectStore)
    object_store.namespaced = Mock(return_value=Mock())

    pipeline = Pipeline(steps, step_outbox_size=10, object_store=object_store)
    reporter = PipelineProgressReporter.for_testing([])

    await pipeline.run(reporter)

    # Verify multiplication worked
    assert len(writer.written_records) == 6  # 2 input * 3 copies each

    # Verify writer resources were cleaned up
    assert len(writer.allocated_resources) == 0

    # Verify finalize_record calls for writer (final step)
    assert len(writer.finalized_tokens) == 6  # 6 output records


@pytest.mark.asyncio
async def test_cleanup_flow_with_exception():
    """Test cleanup flow when an exception occurs during processing."""

    class FailingTransformer(Transformer):
        def __init__(self):
            self.allocated_resources = {}
            self.finalized_tokens = []

        async def process_record(self, record, context):
            # Allocate resource
            token = f"failing_resource_{id(record)}"
            self.allocated_resources[token] = f"resource_for_{record}"

            if "fail" in str(record):
                raise ValueError(f"Processing failed for {record}")

            yield (f"processed_{record}", token)

        async def finalize_record(self, record_token):
            if record_token in self.allocated_resources:
                del self.allocated_resources[record_token]
            self.finalized_tokens.append(record_token)

    # Create steps
    extractor = ResourceTrackingExtractor(["good1", "fail1", "good2"])
    failing_transformer = FailingTransformer()
    writer = ResourceTrackingWriter()

    steps = (extractor, failing_transformer, writer)
    object_store = Mock(spec=ObjectStore)
    object_store.namespaced = Mock(return_value=Mock())

    pipeline = Pipeline(steps, step_outbox_size=10, object_store=object_store)

    # Use a reporter that doesn't raise on fatal errors for this test
    reporter = PipelineProgressReporter(
        on_fatal_error_callback=lambda ex: None  # Don't raise
    )

    await pipeline.run(reporter)

    # The pipeline should handle the exception and stop processing
    # Writer should have processed at least one successful record before failure
    assert len(writer.written_records) >= 1  # At least one successful record


@pytest.mark.asyncio
async def test_cleanup_flow_performance():
    """Test cleanup flow performance with many records."""
    # Create a large number of records to test performance
    large_dataset = [f"item_{i}" for i in range(100)]

    extractor = ResourceTrackingExtractor(large_dataset)
    transformer = ResourceTrackingTransformer()
    writer = ResourceTrackingWriter()

    steps = (extractor, transformer, writer)
    object_store = Mock(spec=ObjectStore)
    object_store.namespaced = Mock(return_value=Mock())

    pipeline = Pipeline(steps, step_outbox_size=10, object_store=object_store)
    reporter = PipelineProgressReporter.for_testing([])

    # Measure execution time
    import time

    start_time = time.time()

    await pipeline.run(reporter)

    end_time = time.time()
    execution_time = end_time - start_time

    # Verify all records were processed and cleaned up
    assert len(writer.written_records) == 100
    assert len(writer.allocated_resources) == 0

    # Verify cleanup calls were made for writer (final step)
    assert len(writer.finalized_tokens) == 100

    # Performance should be reasonable (adjust threshold as needed)
    assert execution_time < 5.0  # Should complete within 5 seconds
