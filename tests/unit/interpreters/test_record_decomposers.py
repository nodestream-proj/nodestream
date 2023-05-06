from nodestream.model import DesiredIngestion, InterpreterContext
from nodestream.model.record_decomposers import RecordDecomposer

from ..stubs import StubbedValueProvider


def test_whole_record_iteration():
    context = InterpreterContext({}, DesiredIngestion())
    subject = RecordDecomposer.from_iteration_arguments(None)
    assert context == next(subject.decompose_record(context))


def test_iteration_record():
    context = InterpreterContext({}, DesiredIngestion())
    value_provider = StubbedValueProvider(values=[1, 2, 3])
    subject = RecordDecomposer.from_iteration_arguments(value_provider)
    assert [
        InterpreterContext(1, DesiredIngestion()),
        InterpreterContext(2, DesiredIngestion()),
        InterpreterContext(3, DesiredIngestion()),
    ] == list(subject.decompose_record(context))
