import pytest
from hamcrest import assert_that, equal_to

from nodestream.model import TimeToLiveConfiguration
from nodestream.project.audits import AuditPrinter, AuditTimeToLiveConfigurations
from nodestream.schema import GraphObjectType


@pytest.mark.asyncio
async def test_audit_ttls_fails_when_there_is_no_ttl_for_a_node_type(
    mocker, basic_schema
):
    project = mocker.Mock()
    project.dig_for_step_of_type.return_value = []
    project.get_schema.return_value = basic_schema

    audit = AuditTimeToLiveConfigurations(AuditPrinter())
    await audit.run(project)

    # Since there are 2 node types and 2 relationship types,
    # we expect 4 failures because there are no TTLs for any of them.
    assert_that(audit.failure_count, equal_to(4))
    assert_that(audit.success_count, equal_to(0))


@pytest.mark.asyncio
async def test_audit_ttls_fails_when_there_is_no_ttl_for_a_relationship_type(
    mocker, basic_schema
):
    project = mocker.Mock()
    project.dig_for_step_of_type.return_value = []
    project.get_schema.return_value = basic_schema

    audit = AuditTimeToLiveConfigurations(AuditPrinter())
    await audit.run(project)

    assert_that(audit.failure_count, equal_to(4))
    assert_that(audit.success_count, equal_to(0))


@pytest.mark.asyncio
async def test_audit_ttls_succeeds_when_there_is_a_ttl_for_a_node_type(
    mocker, basic_schema
):
    async def extract_records():
        yield TimeToLiveConfiguration(GraphObjectType.NODE, "Person", 1000)

    project = mocker.Mock()
    project.dig_for_step_of_type.return_value = [
        [mocker.Mock(), 1, extractor := mocker.AsyncMock()]
    ]
    extractor.extract_records = extract_records
    project.get_schema.return_value = basic_schema

    audit = AuditTimeToLiveConfigurations(AuditPrinter())
    await audit.run(project)

    assert_that(audit.failure_count, equal_to(3))
    assert_that(audit.success_count, equal_to(0))


@pytest.mark.asyncio
async def test_audit_ttls_succeeds_when_there_is_a_ttl_for_a_relationship_type(
    mocker, basic_schema
):
    async def extract_records():
        yield TimeToLiveConfiguration(
            GraphObjectType.RELATIONSHIP, "BEST_FRIEND_OF", 1000
        )
        yield TimeToLiveConfiguration(
            GraphObjectType.RELATIONSHIP, "HAS_EMPLOYEE", 1000
        )
        yield TimeToLiveConfiguration(GraphObjectType.NODE, "Organization", 1000)
        yield TimeToLiveConfiguration(GraphObjectType.NODE, "Person", 1000)

    project = mocker.Mock()
    project.dig_for_step_of_type.return_value = [
        [mocker.Mock(), 1, extractor := mocker.AsyncMock()]
    ]
    extractor.extract_records = extract_records
    project.get_schema.return_value = basic_schema

    audit = AuditTimeToLiveConfigurations(AuditPrinter())
    await audit.run(project)

    assert_that(audit.failure_count, equal_to(0))
    assert_that(audit.success_count, equal_to(1))
