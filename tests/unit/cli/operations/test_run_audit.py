import pytest

from nodestream.cli.operations.run_audit import RunAudit, CommandAuditPrinter


def test_command_printer_prints_success_message(mocker):
    command = mocker.Mock()
    printer = CommandAuditPrinter(command)

    printer.print_success("Success!")

    command.line.assert_called_once_with("<info>Success!</info>")


def test_command_printer_prints_failure_message(mocker):
    command = mocker.Mock()
    printer = CommandAuditPrinter(command)

    printer.print_failure("Failure!")

    command.line.assert_called_once_with("<error>Failure!</error>")


def test_command_printer_prints_warning_message(mocker):
    command = mocker.Mock()
    printer = CommandAuditPrinter(command)

    printer.print_warning("Warning!")

    command.line.assert_called_once_with("<comment>Warning!</comment>")


@pytest.mark.asyncio
async def test_run_audit_performs_audit(mocker):
    project = mocker.Mock()
    audit_cls = mocker.Mock()
    audit = mocker.AsyncMock()
    audit_cls.return_value = audit
    command = mocker.Mock()
    operation = RunAudit(project, audit_cls)

    await operation.perform(command)

    audit_cls.assert_called_once()
    audit.run.assert_called_once_with(project)
