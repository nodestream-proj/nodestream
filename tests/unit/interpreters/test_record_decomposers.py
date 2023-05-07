from hamcrest import assert_that, equal_to

from nodestream.model import DesiredIngestion, InterpreterContext
from nodestream.model.record_decomposers import RecordDecomposer

from ..stubs import StubbedValueProvider


def test_whole_record_iteration():
    context = InterpreterContext({}, DesiredIngestion())
    subject = RecordDecomposer.from_iteration_arguments(None)
    assert context == next(subject.decompose_record(context))


def test_iteration_record():
    expected_records = [1, 2, 3]
    context = InterpreterContext({}, DesiredIngestion())
    value_provider = StubbedValueProvider(values=expected_records)
    subject = RecordDecomposer.from_iteration_arguments(value_provider)
    actual_records = [ctx.document for ctx in subject.decompose_record(context)]
    assert_that(actual_records, equal_to(expected_records))
