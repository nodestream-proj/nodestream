from decimal import Decimal

import pytest
from hamcrest import assert_that, equal_to
from yaml import safe_dump

from nodestream.pipeline.value_providers import StaticValueProvider
from nodestream.pipeline.value_providers.cast_value_provider import CastValueProvider
from nodestream.pipeline.value_providers.value_provider import ValueProviderException

from ...stubs import StubbedValueProvider


def test_cast_value_provider_single_value_int(blank_context):
    provider = CastValueProvider(data=StaticValueProvider("123"), to="int")
    assert_that(provider.single_value(blank_context), equal_to(123))


def test_cast_value_provider_single_value_float(blank_context):
    provider = CastValueProvider(data=StaticValueProvider("1.09"), to="float")
    assert_that(provider.single_value(blank_context), equal_to(1.09))


def test_cast_value_provider_single_value_bool_true(blank_context):
    provider = CastValueProvider(data=StaticValueProvider(1), to="bool")
    assert_that(provider.single_value(blank_context), equal_to(True))


def test_cast_value_provider_single_value_bool_false(blank_context):
    provider = CastValueProvider(data=StaticValueProvider(""), to="bool")
    assert_that(provider.single_value(blank_context), equal_to(False))


def test_cast_value_provider_single_value_string(blank_context):
    provider = CastValueProvider(data=StaticValueProvider(False), to="str")
    assert_that(provider.single_value(blank_context), equal_to("False"))


def test_cast_value_provider_single_value_decimal(blank_context):
    provider = CastValueProvider(data=StaticValueProvider(12.34), to="decimal")
    assert_that(provider.single_value(blank_context), equal_to(Decimal(12.34)))


def test_cast_value_provider_many_values(blank_context):
    provider = CastValueProvider(StubbedValueProvider(["1", "2", "3"]), "int")
    assert_that(list(provider.many_values(blank_context)), equal_to([1, 2, 3]))


def test_cast_value_provider_invalid_cast(blank_context):
    provider = CastValueProvider(StaticValueProvider("abc"), "int")

    with pytest.raises(ValueProviderException):
        provider.single_value(blank_context)


def test_cast_value_provider_yaml_dump():
    provider = CastValueProvider(StaticValueProvider("123"), "int")

    assert_that(
        safe_dump(provider),
        equal_to("!cast\ndata: !static '123'\nto: int\n"),
    )
