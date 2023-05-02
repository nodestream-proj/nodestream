from nodestream.interpreters.record_decomposers import RecordDecomposer
from nodestream.model import DesiredIngestion, IngestContext
from ..stubs import StubbedValueProvider


def test_whole_record_iteration():
    context = IngestContext({}, DesiredIngestion())
    subject = RecordDecomposer.from_iteration_arguments(None)
    assert context == next(subject.decompose_record(context))


def test_iteration_record():
    context = IngestContext({}, DesiredIngestion())
    value_provider = StubbedValueProvider(values=[1, 2, 3])
    subject = RecordDecomposer.from_iteration_arguments(value_provider)
    assert [
        IngestContext(1, DesiredIngestion()),
        IngestContext(2, DesiredIngestion()),
        IngestContext(3, DesiredIngestion()),
    ] == list(subject.decompose_record(context))
