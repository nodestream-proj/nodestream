from hamcrest import assert_that, equal_to

from nodestream.schema.migrations.io import MigratorInput, LoggingMigratorOutput


def test_logging_migrator_output_print_message(mocker):
    output = LoggingMigratorOutput()
    output.logger = mocker.Mock()
    message = "Hello, world!"
    output.print_message(message)
    output.logger.debug.assert_called_once_with(message)


def test_logging_migrator_output_print_exception(mocker):
    output = LoggingMigratorOutput()
    output.logger = mocker.Mock()
    exception = Exception("Hello, world!")
    output.print_exception(exception)
    output.logger.exception.assert_called_once_with(exception)


def test_migrator_input_ask_yes_no(mocker):
    input = MigratorInput()
    assert_that(input.ask_yes_no("Hello, world!"), equal_to(False))


def test_migrator_input_ask_property_renamed_asks_correct_question(mocker):
    input = MigratorInput()
    input.ask_yes_no = mocker.Mock(return_value=True)
    assert_that(
        input.ask_property_renamed("object_type", "old_property", "new_property"),
        equal_to(True),
    )
    input.ask_yes_no.assert_called_once_with(
        "Did you rename old_property to new_property on object_type?"
    )


def test_migrator_input_ask_type_renamed_asks_correct_question(mocker):
    input = MigratorInput()
    input.ask_yes_no = mocker.Mock(return_value=True)
    assert_that(
        input.ask_type_renamed("old_type", "new_type"),
        equal_to(True),
    )
    input.ask_yes_no.assert_called_once_with("Did you rename old_type to new_type?")
