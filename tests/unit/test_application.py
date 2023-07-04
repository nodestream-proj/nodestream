from pathlib import Path

from nodestream.application import Nodestream, ProjectPlugin, ApplicationPlugin
from nodestream.cli.commands import AuditCommand, NodestreamCommand
from nodestream.project.audits import Audit

from hamcrest import assert_that, same_instance, has_length, equal_to, instance_of


def test_application_get_project(mocker):
    read_from_file = mocker.patch("nodestream.project.Project.read_from_file")
    ProjectPlugin.all = mocker.Mock()
    ProjectPlugin.all.return_value = plugins = [mocker.Mock(), mocker.Mock()]

    result = Nodestream.default().get_project(Path("nodestream.yaml"))
    assert_that(result, equal_to(read_from_file.return_value))
    for plugin in plugins:
        plugin.activate.assert_called_once_with(read_from_file.return_value)


def test_application_run_cli(mocker):
    subject = Nodestream.default()
    subject.make_cli = mocker.Mock()
    subject.run_cli()
    subject.make_cli.return_value.run.assert_called_once()


def test_application_make_cli():
    subject = Nodestream.default()
    result = subject.make_cli()
    # Cleo adds three commands by default
    assert_that(result._commands, has_length(len(subject.commands) + 3))


def test_add_audit():
    class MyAudit(Audit):
        pass

    subject = Nodestream.default()
    subject.add_audit(MyAudit)
    assert_that(subject.commands[-1].audit, equal_to(MyAudit))


def test_add_command():
    class MyCommand(NodestreamCommand):
        pass

    subject = Nodestream.default()
    subject.add_command(MyCommand)
    assert_that(subject.commands[-1], instance_of(MyCommand))


def test_default_instance_configured_correctly():
    result = Nodestream.default()
    assert_that(result.commands, has_length(8))
    assert_that(
        {cmd for cmd in result.commands if isinstance(cmd, AuditCommand)}, has_length(2)
    )
    assert_that(
        {cmd for cmd in result.commands if not isinstance(cmd, AuditCommand)},
        has_length(6),
    )


def test_default_instance_reused():
    first_call = Nodestream.instance()
    second_call = Nodestream.instance()
    assert_that(first_call, same_instance(second_call))


def test_default_instance_with_plugins(mocker):
    ApplicationPlugin.all = mocker.Mock()
    ApplicationPlugin.all.return_value = plugins = [mocker.Mock(), mocker.Mock()]
    result = Nodestream.default()
    for plugin in plugins:
        plugin.activate.assert_called_once_with(result)
