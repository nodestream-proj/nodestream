from hamcrest import assert_that, equal_to

from nodestream.interpreting.record_decomposers import RecordDecomposer
from nodestream.model import DesiredIngestion
from nodestream.pipeline.value_providers import ProviderContext

from ...stubs import StubbedValueProvider


def test_whole_record_iteration():
    context = ProviderContext({}, DesiredIngestion())
    subject = RecordDecomposer.from_iteration_arguments(None)
    assert_that(context, equal_to(next(subject.decompose_record(context))))


def test_iteration_record():
    expected_records = [1, 2, 3]
    context = ProviderContext({}, DesiredIngestion())
    value_provider = StubbedValueProvider(values=expected_records)
    subject = RecordDecomposer.from_iteration_arguments(value_provider)
    actual_records = [ctx.document for ctx in subject.decompose_record(context)]
    assert_that(actual_records, equal_to(expected_records))
