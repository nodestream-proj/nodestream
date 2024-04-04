from unittest.mock import ANY

from hamcrest import assert_that, equal_to, instance_of

from nodestream.databases.null import NullMigrator
from nodestream.file_io import LazyLoadedArgument
from nodestream.project import Target


def test_target_make_writer_default_writer_args(mocker):
    target = Target.from_file_data("test", {"a": "b"})
    mock_writer = mocker.patch("nodestream.databases.GraphDatabaseWriter")
    target.make_writer()
    mock_writer.from_connector.assert_called_once_with(connector=ANY)


def test_target_make_writer_custom_writer_args(mocker):
    target = Target.from_file_data(
        "test",
        {
            "a": "b",
            "batch_size": 500,
            "collect_stats": False,
            "ingest_strategy_name": "immediate",
        },
    )
    mock_writer = mocker.patch("nodestream.databases.GraphDatabaseWriter")
    target.make_writer()
    mock_writer.from_connector.assert_called_once_with(
        connector=ANY,
        ingest_strategy_name="immediate",
        collect_stats=False,
        batch_size=500,
    )


def test_make_migrator_with_writer_args(mocker):
    target = Target.from_file_data("test", {"database": "null", "batch_size": 500})
    result = target.make_migrator()
    assert_that(result, instance_of(NullMigrator))


def test_target_make_type_retriever(mocker):
    target = Target("test", {"a": "b"})
    mock_retriever = mocker.patch("nodestream.databases.DatabaseConnector")
    target.make_type_retriever()
    mock_retriever.from_database_args.assert_called_once_with(a="b")


def test_target_resolves_lazy_tags(mocker):
    target = Target("test", {"a": LazyLoadedArgument("env", "USERNAME_ENV")})
    mocker.patch("os.environ", {"USERNAME_ENV": "bob"})
    assert_that(target.resolved_connector_config, equal_to({"a": "bob"}))
