import sys

import pytest
from hamcrest import assert_that, equal_to, instance_of

from nodestream.pipeline.filters import (
    EnforceSchema,
    ExcludeWhenValuesMatchPossibilities,
    Filter,
    Schema,
    SchemaEnforcer,
    ValueMatchesRegexFilter,
    ValuesMatchPossibilitiesFilter,
    WarnSchema,
)

from ..stubs import StubbedValueProvider

PASSING_FILTER_CONFIGURATION = [
    {
        "value": StubbedValueProvider("test"),
        "possibilities": [StubbedValueProvider("A Miss"), StubbedValueProvider("test")],
    }
]

FAILING_FILTER_CONFIGURATION = [
    *PASSING_FILTER_CONFIGURATION,
    {
        "value": StubbedValueProvider("un-findable"),
        "possibilities": [StubbedValueProvider("not the right value")],
    },
]


@pytest.mark.asyncio
async def test_match_possibilities_successful():
    subject = ValuesMatchPossibilitiesFilter.from_file_data(
        fields=PASSING_FILTER_CONFIGURATION
    )
    result = await subject.filter_record({})
    assert_that(result, equal_to(False))


@pytest.mark.asyncio
async def test_match_possibilities_failing():
    subject = ValuesMatchPossibilitiesFilter.from_file_data(
        fields=FAILING_FILTER_CONFIGURATION
    )
    result = await subject.filter_record({})
    assert_that(result, equal_to(True))


@pytest.mark.asyncio
async def test_exclude_possibilities_successful():
    subject = ExcludeWhenValuesMatchPossibilities.from_file_data(
        fields=PASSING_FILTER_CONFIGURATION
    )
    result = await subject.filter_record({})
    assert_that(result, equal_to(True))


@pytest.mark.asyncio
async def test_exclude_possibilities__failing():
    subject = ExcludeWhenValuesMatchPossibilities.from_file_data(
        fields=FAILING_FILTER_CONFIGURATION
    )
    result = await subject.filter_record({})
    assert_that(result, equal_to(False))


@pytest.mark.asyncio
async def test_base_filter_filters_correctly():
    class TestFilter(Filter):
        def __init__(self, results) -> None:
            self.results = results

        async def filter_record(self, record):
            return self.results.pop(0)

    subject = TestFilter([False, True])
    results = [record async for record in subject.process_record(1, None)]
    assert_that(results, equal_to([1]))
    results = [record async for record in subject.process_record(2, None)]
    assert_that(results, equal_to([]))


REGEX = ".*[\[\]\{\}\(\)\\\/~,]+"
REGEX_TEST_CASES = [
    {"value": "[test]", "include": True, "expect": False},
    {"value": "test", "include": True, "expect": True},
    {"value": "[test]", "include": False, "expect": True},
    {"value": "test", "include": False, "expect": False},
]


@pytest.mark.asyncio
async def test_match_regex():
    for test_case in REGEX_TEST_CASES:
        subject = ValueMatchesRegexFilter.from_file_data(
            value=test_case["value"], regex=REGEX, include=test_case["include"]
        )
        result = await subject.filter_record({})
        assert_that(result, equal_to(test_case["expect"]))


@pytest.fixture
def schema_dict():
    return {"type": "object", "properties": {"name": {"type": "string"}}}


@pytest.mark.asyncio
async def test_schema_enforcer_with_fetch_schema(mocker, schema_dict):
    object_store = mocker.Mock()
    object_store.get_pickled.return_value = schema_dict

    context = mocker.Mock()
    context.object_store = object_store

    subject = SchemaEnforcer.from_file_data(
        enforcement_policy="enforce", key="test_key"
    )
    await subject.start(context)

    record = {"name": "test"}
    result = await subject.filter_record(record)
    assert_that(result, equal_to(False))

    record = {"name": 123}
    result = await subject.filter_record(record)
    assert_that(result, equal_to(True))


@pytest.mark.asyncio
async def test_schema_enforcer_with_infer_schema(mocker):
    object_store = mocker.Mock()
    object_store.get_pickled.return_value = None

    context = mocker.Mock()
    context.object_store = object_store

    subject = SchemaEnforcer.from_file_data(inference_sample_size=2)
    await subject.start(context)

    record = {"name": "test"}
    result = await subject.filter_record(record)
    assert_that(result, equal_to(False))

    result = await subject.filter_record(record)
    assert_that(result, equal_to(False))

    record = {"name": 123}
    result = await subject.filter_record(record)
    assert_that(result, equal_to(True))


@pytest.mark.asyncio
async def test_schema_enforcement_modes(schema_dict):
    schema = Schema(schema_dict)

    enforce_mode = EnforceSchema(schema)
    warn_mode = WarnSchema(schema)

    record = {"name": "test"}
    assert_that(enforce_mode.should_filter(record), equal_to(False))
    assert_that(warn_mode.should_filter(record), equal_to(False))

    record = {"name": 123}
    assert_that(enforce_mode.should_filter(record), equal_to(True))
    assert_that(warn_mode.should_filter(record), equal_to(False))


@pytest.mark.asyncio
async def test_infer_mode_schema_already_exists(mocker, schema_dict):
    subject = SchemaEnforcer.from_file_data(inference_sample_size=2)

    context = mocker.Mock()
    context.object_store.get_pickled.return_value = schema_dict

    await subject.start(context)

    record = {"name": "test"}
    assert_that(await subject.filter_record(record), equal_to(False))


@pytest.mark.asyncio
async def test_enforce_mode_schema_not_present(mocker):
    subject = SchemaEnforcer.from_file_data(key="test_key")

    context = mocker.Mock()
    context.object_store.get_pickled.return_value = None

    with pytest.raises(ValueError):
        await subject.start(context)


@pytest.mark.asyncio
async def test_invalid_enforcement_policy(mocker, schema_dict):
    context = mocker.Mock()
    context.object_store.get_pickled.return_value = schema_dict
    subject = SchemaEnforcer.from_file_data(
        enforcement_policy="invalid", inference_sample_size=2
    )

    with pytest.raises(ValueError):
        await subject.start(context)


@pytest.mark.asyncio
async def test_warn_policy(mocker):
    context = mocker.Mock()
    context.object_store.get_pickled.return_value = None
    subject = SchemaEnforcer.from_file_data(
        inference_sample_size=0, enforcement_policy="warn"
    )
    await subject.start(context)
    assert_that(await subject.filter_record({}), equal_to(False))
    assert_that(subject.mode, instance_of(WarnSchema))


def test_filter_import_error(mocker):
    # These are the hoops we must run through to test an ImportError
    # for the case where the genson or jsonschema libraries are not installed
    # and the SchemaEnforcer is instantiated. So patch the modules to None
    # and delete the filters module from sys.modules to reload it.
    mocker.patch.dict("sys.modules", {"genson": None, "jsonschema": None})
    del sys.modules["nodestream.pipeline.filters"]
    from nodestream.pipeline.filters import SchemaEnforcer

    with pytest.raises(ImportError) as excinfo:
        SchemaEnforcer()

    assert (
        "SchemaEnforcer requires genson and jsonschema to be installed. Install the `validation` extra."
        in str(excinfo.value)
    )
