import pytest
from hamcrest import assert_that, equal_to

from nodestream.audits import AuditPrinter, AuditReferencialIntegrity
from nodestream.interpreting import Interpreter
from nodestream.model import KeyIndex


@pytest.mark.asyncio
async def test_audit_referencial_integrity_fails_when_there_is_two_interpreters_with_different_keys_for_the_same_name(
    mocker,
):
    interpreter_one = mocker.Mock(Interpreter)
    interpreter_one.gather_used_indexes.return_value = [
        KeyIndex("Person", ["name", "age"]),
    ]
    interpreter_two = mocker.Mock(Interpreter)
    interpreter_two.gather_used_indexes.return_value = [
        KeyIndex("Person", ["name"]),
    ]
    project = mocker.Mock()
    project.dig_for_step_of_type.return_value = [
        [mocker.Mock(), 1, interpreter_one],
        [mocker.Mock(), 1, interpreter_two],
    ]

    audit = AuditReferencialIntegrity(AuditPrinter())
    await audit.run(project)

    assert_that(audit.failure_count, equal_to(1))
    assert_that(audit.success_count, equal_to(0))


@pytest.mark.asyncio
async def test_audit_referencial_integrity_succeeds_when_there_is_two_interpreters_with_the_same_keys_for_the_same_name(
    mocker,
):
    interpreter_one = mocker.Mock(Interpreter)
    interpreter_one.gather_used_indexes.return_value = [
        KeyIndex("Person", ["name", "age"]),
    ]
    interpreter_two = mocker.Mock(Interpreter)
    interpreter_two.gather_used_indexes.return_value = [
        KeyIndex("Person", ["name", "age"]),
    ]
    project = mocker.Mock()
    project.dig_for_step_of_type.return_value = [
        [mocker.Mock(), 1, interpreter_one],
        [mocker.Mock(), 1, interpreter_two],
    ]

    audit = AuditReferencialIntegrity(AuditPrinter())
    await audit.run(project)

    assert_that(audit.failure_count, equal_to(0))
    assert_that(audit.success_count, equal_to(1))
