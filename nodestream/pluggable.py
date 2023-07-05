from importlib.metadata import entry_points
from functools import cache
from inspect import getmembers

NODESTREAM_PLUGINS_ENTRYPOINT_GROUP = "nodestream.plugins"


class Pluggable:
    entrypoint_name: str = "plugin"

    @classmethod
    @cache
    def all_entrypoints(cls):
        return entry_points(group=NODESTREAM_PLUGINS_ENTRYPOINT_GROUP)

    @classmethod
    def all(cls):
        for entrypoint in cls.all_entrypoints():
            try:
                plugin_module = entrypoint[cls.entrypoint_name].load()
                yield from getmembers(plugin_module, lambda member: issubclass(member, cls))
            except KeyError:
                pass
