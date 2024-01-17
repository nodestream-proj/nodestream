from hamcrest import assert_that, equal_to

from nodestream.file_io import LazyLoadedArgument
from nodestream.project import Target


def test_target_make_writer(mocker):
    target = Target("test", {"a": "b"})
    mock_writer = mocker.patch("nodestream.databases.GraphDatabaseWriter")
    target.make_writer()
    mock_writer.from_file_data.assert_called_once_with(a="b")


def test_target_make_type_retriever(mocker):
    target = Target("test", {"a": "b"})
    mock_retriever = mocker.patch("nodestream.databases.DatabaseConnector")
    target.make_type_retriever()
    mock_retriever.from_database_args.assert_called_once_with(a="b")


def test_target_resolves_lazy_tags(mocker):
    target = Target("test", {"a": LazyLoadedArgument("env", "USERNAME_ENV")})
    mocker.patch("os.environ", {"USERNAME_ENV": "bob"})
    assert_that(target.resolved_connector_config, equal_to({"a": "bob"}))
