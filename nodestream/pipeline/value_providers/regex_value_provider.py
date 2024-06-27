import re
from typing import Any, Iterable, Type

from yaml import SafeDumper, SafeLoader

from .context import ProviderContext
from .value_provider import ValueProvider, ValueProviderException


class RegexValueProvider(ValueProvider):
    def __init__(self, regex: str, data: ValueProvider, group: int | str = 0):
        self.raw_regex = regex
        self.regex = re.compile(regex)
        self.group = group
        self.data = data

    def apply_regex_to_value(self, value: Any) -> Any:
        match = self.regex.search(value)
        if match:
            return match.group(self.group)
        return None

    def single_value(self, context: ProviderContext) -> Any:
        try:
            return self.apply_regex_to_value(self.data.single_value(context))
        except Exception as e:
            raise ValueProviderException(str(context.document), self) from e

    def many_values(self, context: ProviderContext) -> Iterable[Any]:
        try:
            return map(self.apply_regex_to_value, self.data.many_values(context))
        except Exception as e:
            raise ValueProviderException(str(context.document), self) from e

    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        loader.add_constructor(
            "!regex", lambda loader, node: cls(**loader.construct_mapping(node))
        )

    def __str__(self):
        return f"RegexValueProvider: { {'regex': self.raw_regex, 'data': str(self.data), 'group': str(self.group)} }"


SafeDumper.add_representer(
    RegexValueProvider,
    lambda dumper, regex: dumper.represent_mapping(
        "!regex",
        {"regex": regex.regex.pattern, "group": regex.group, "data": regex.data},
    ),
)
