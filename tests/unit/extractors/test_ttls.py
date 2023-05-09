import pytest
from hamcrest import assert_that, equal_to

from nodestream.model import TimeToLiveConfiguration, GraphObjectType
from nodestream.extractors.ttls import TimeToLiveConfigurationExtractor


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
                    graph_type=GraphObjectType.NODE,
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
                    graph_type=GraphObjectType.RELATIONSHIP,
                    object_type="MY_REL_TYPE",
                    expiry_in_hours=48,
                )
            ]
        ),
    )
