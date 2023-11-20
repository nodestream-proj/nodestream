import pytest

from nodestream.compat import deprecated_arugment


def test_deprecated_argument(mocker):
    function_with_deprecated_argument = mocker.Mock()
    wrapped = deprecated_arugment("deprecated", "new")(
        function_with_deprecated_argument
    )
    with pytest.warns(DeprecationWarning):
        wrapped("foo", deprecated="bar")
    function_with_deprecated_argument.assert_called_with("foo", new="bar")


def test_deprecated_argument_with_converter(mocker):
    function_with_deprecated_argument = mocker.Mock()
    wrapped = deprecated_arugment("deprecated", "new", lambda x: x.upper())(
        function_with_deprecated_argument
    )
    with pytest.warns(DeprecationWarning):
        wrapped("foo", deprecated="bar")
    function_with_deprecated_argument.assert_called_with("foo", new="BAR")
