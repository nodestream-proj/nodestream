from nodestream.project import Target


def test_target_make_writer(mocker):
    target = Target("test", {"a": "b"})
    mock_writer = mocker.patch("nodestream.databases.GraphDatabaseWriter")
    target.make_writer()
    mock_writer.from_file_data.assert_called_once_with(a="b")
