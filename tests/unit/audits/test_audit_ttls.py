import pytest
from hamcrest import assert_that, equal_to

from nodestream.audits import AuditPrinter, TTLAudit
from nodestream.model import (
    GraphObjectShape,
    GraphObjectType,
    GraphSchema,
    KnownTypeMarker,
    PropertyMetadataSet,
    TimeToLiveConfiguration,
)


@pytest.mark.asyncio
async def test_audit_ttls_fails_when_there_is_no_ttl_for_a_node_type(mocker):
    project = mocker.Mock()
    project.dig_for_step_of_type.return_value = []
    project.generate_graph_schema.return_value = GraphSchema(
        [
            GraphObjectShape(
                GraphObjectType.NODE,
                KnownTypeMarker("Person"),
                PropertyMetadataSet.from_names(),
            )
        ],
        [],
    )

    audit = TTLAudit(AuditPrinter())
    await audit.run(project)

    assert_that(audit.failure_count, equal_to(1))
    assert_that(audit.success_count, equal_to(0))


@pytest.mark.asyncio
async def test_audit_ttls_fails_when_there_is_no_ttl_for_a_relationship_type(mocker):
    project = mocker.Mock()
    project.dig_for_step_of_type.return_value = []
    project.generate_graph_schema.return_value = GraphSchema(
        [
            GraphObjectShape(
                GraphObjectType.RELATIONSHIP,
                KnownTypeMarker("KNOWS"),
                PropertyMetadataSet.from_names(),
            )
        ],
        [],
    )

    audit = TTLAudit(AuditPrinter())
    await audit.run(project)

    assert_that(audit.failure_count, equal_to(1))
    assert_that(audit.success_count, equal_to(0))


@pytest.mark.asyncio
async def test_audit_ttls_succeeds_when_there_is_a_ttl_for_a_node_type(mocker):
    async def extract_records():
        yield TimeToLiveConfiguration(GraphObjectType.NODE, "Person", 1000)

    project = mocker.Mock()
    project.dig_for_step_of_type.return_value = [
        [mocker.Mock(), 1, extractor := mocker.AsyncMock()]
    ]
    extractor.extract_records = extract_records
    project.generate_graph_schema.return_value = GraphSchema(
        [
            GraphObjectShape(
                GraphObjectType.NODE,
                KnownTypeMarker("Person"),
                PropertyMetadataSet.from_names(),
            )
        ],
        [],
    )

    audit = TTLAudit(AuditPrinter())
    await audit.run(project)

    assert_that(audit.failure_count, equal_to(0))
    assert_that(audit.success_count, equal_to(1))


@pytest.mark.asyncio
async def test_audit_ttls_succeeds_when_there_is_a_ttl_for_a_relationship_type(mocker):
    async def extract_records():
        yield TimeToLiveConfiguration(GraphObjectType.RELATIONSHIP, "KNOWS", 1000)

    project = mocker.Mock()
    project.dig_for_step_of_type.return_value = [
        [mocker.Mock(), 1, extractor := mocker.AsyncMock()]
    ]
    extractor.extract_records = extract_records
    project.generate_graph_schema.return_value = GraphSchema(
        [
            GraphObjectShape(
                GraphObjectType.RELATIONSHIP,
                KnownTypeMarker("KNOWS"),
                PropertyMetadataSet.from_names(),
            )
        ],
        [],
    )

    audit = TTLAudit(AuditPrinter())
    await audit.run(project)

    assert_that(audit.failure_count, equal_to(0))
    assert_that(audit.success_count, equal_to(1))
