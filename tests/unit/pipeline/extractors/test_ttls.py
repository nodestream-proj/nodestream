import pytest
from hamcrest import assert_that, equal_to

from nodestream.model import TimeToLiveConfiguration
from nodestream.pipeline.extractors import TimeToLiveConfigurationExtractor
from nodestream.schema.schema import GraphObjectType


@pytest.mark.asyncio
async def test_ttls_node_object_type():
    subject = TimeToLiveConfigurationExtractor(
        "NODE", configurations=[{"object_type": "MyNodeType", "expiry_in_hours": 48}]
    )
    result = [r async for r in subject.extract_records()]
    assert_that(
        result,
        equal_to(
            [
                TimeToLiveConfiguration(
                    graph_object_type=GraphObjectType.NODE,
                    object_type="MyNodeType",
                    expiry_in_hours=48,
                )
            ]
        ),
    )


@pytest.mark.asyncio
async def test_ttls_relationship_object_type():
    subject = TimeToLiveConfigurationExtractor(
        "RELATIONSHIP",
        configurations=[{"object_type": "MY_REL_TYPE", "expiry_in_hours": 48}],
    )
    result = [r async for r in subject.extract_records()]
    assert_that(
        result,
        equal_to(
            [
                TimeToLiveConfiguration(
                    graph_object_type=GraphObjectType.RELATIONSHIP,
                    object_type="MY_REL_TYPE",
                    expiry_in_hours=48,
                )
            ]
        ),
    )


@pytest.mark.asyncio
async def test_ttls_with_override():
    subject = TimeToLiveConfigurationExtractor(
        "RELATIONSHIP",
        configurations=[
            {"object_type": "MY_REL_TYPE", "expiry_in_hours": 48},
            {"object_type": "OTHER_REL_TYPE", "expiry_in_hours": 2},
        ],
        override_expiry_in_hours=4,
    )
    result = [r async for r in subject.extract_records()]
    assert_that(
        result,
        equal_to(
            [
                TimeToLiveConfiguration(
                    graph_object_type=GraphObjectType.RELATIONSHIP,
                    object_type="MY_REL_TYPE",
                    expiry_in_hours=4,
                ),
                TimeToLiveConfiguration(
                    graph_object_type=GraphObjectType.RELATIONSHIP,
                    object_type="OTHER_REL_TYPE",
                    expiry_in_hours=4,
                ),
            ]
        ),
    )
