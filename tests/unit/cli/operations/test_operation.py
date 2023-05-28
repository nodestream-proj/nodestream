from nodestream.cli.operations import Operation


class SimpleOperation(Operation):
    pass


def test_operation_default_name():
    assert "Simple Operation" == SimpleOperation().name
