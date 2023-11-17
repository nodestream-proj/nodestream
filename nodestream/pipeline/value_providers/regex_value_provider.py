import re
from typing import Any, Iterable, Type

from yaml import SafeDumper, SafeLoader

from .context import ProviderContext
from .value_provider import ValueProvider


class RegexValueProvider(ValueProvider):
    def __init__(self, regex: str, data: ValueProvider, group: int | str = 0):
        self.regex = re.compile(regex)
        self.group = group
        self.data = data

    def apply_regex_to_value(self, value: Any) -> Any:
        match = self.regex.match(value)
        if match:
            return match.group(self.group)
        return None

    def single_value(self, context: ProviderContext) -> Any:
        return self.apply_regex_to_value(self.data.single_value(context))

    def many_values(self, context: ProviderContext) -> Iterable[Any]:
        return map(self.apply_regex_to_value, self.data.many_values(context))

    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        loader.add_constructor(
            "!regex", lambda loader, node: cls(**loader.construct_mapping(node))
        )


SafeDumper.add_representer(
    RegexValueProvider,
    lambda dumper, regex: dumper.represent_mapping(
        "!regex",
        {"regex": regex.regex.pattern, "group": regex.group, "data": regex.data},
    ),
)
