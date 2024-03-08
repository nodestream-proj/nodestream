from abc import ABC, abstractmethod
from typing import Dict

from ...model import PropertySet
from ...pipeline.value_providers import ProviderContext, ValueProvider


class PropertyMapping(ABC):
    @classmethod
    def from_file_data(cls, file_data):
        if isinstance(file_data, ValueProvider):
            return PropertyMappingFromValueProviider(file_data)

        providers = ValueProvider.guarantee_provider_dictionary(file_data)
        return PropertyMappingFromDict(providers)

    @abstractmethod
    def apply_to(
        self,
        context: ProviderContext,
        property_set: PropertySet,
        norm_args: Dict[str, bool],
    ):
        raise NotImplementedError


class PropertyMappingFromValueProviider(PropertyMapping):
    __slots__ = ("value_provider",)

    def __init__(self, value_provider: ValueProvider):
        self.value_provider = value_provider

    def apply_to(
        self,
        context: ProviderContext,
        property_set: PropertySet,
        norm_args: Dict[str, bool],
    ):
        should_be_a_dict = self.value_provider.single_value(context)
        if not isinstance(should_be_a_dict, dict):
            raise ValueError(
                f"When using a ValueProvider as a PropertyMapping, the ValueProvider must return a dict. Instead, it returned {should_be_a_dict}"
            )

        as_providers = ValueProvider.guarantee_provider_dictionary(should_be_a_dict)
        property_set.apply_providers(context, as_providers, norm_args)

    def __iter__(self):
        # This is used when adding properties to a schema.
        # We need to know what properties are being added, so we can add them to the schema.
        # Since we cannot know them until runtime, we cannot provide them here.
        return iter([])


class PropertyMappingFromDict(PropertyMapping):
    __slots__ = ("map_of_value_providers",)

    def __init__(self, map_of_value_providers: Dict[str, ValueProvider]):
        self.map_of_value_providers = map_of_value_providers

    def apply_to(
        self,
        context: ProviderContext,
        property_set: PropertySet,
        norm_args: Dict[str, bool],
    ):
        property_set.apply_providers(context, self.map_of_value_providers, norm_args)

    def __iter__(self):
        return iter(self.map_of_value_providers)
