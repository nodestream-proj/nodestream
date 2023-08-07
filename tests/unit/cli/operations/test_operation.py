from hamcrest import assert_that, equal_to

from ..stubs import SimpleOperation


def test_operation_default_name():
    assert_that(SimpleOperation().name, equal_to("Simple Operation"))
