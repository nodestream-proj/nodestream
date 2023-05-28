from ..stubs import SimpleOperation


def test_operation_default_name():
    assert "Simple Operation" == SimpleOperation().name
