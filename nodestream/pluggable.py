from functools import cache
from importlib.metadata import entry_points
from inspect import getmembers, isclass

NODESTREAM_PLUGINS_ENTRYPOINT_GROUP = "nodestream.plugins"


class Pluggable:
    entrypoint_name: str = "plugin"

    @classmethod
    def entrypoints(cls):
        return entry_points(
            group=NODESTREAM_PLUGINS_ENTRYPOINT_GROUP, name=cls.entrypoint_name
        )

    @classmethod
    @cache
    def all(cls):
        def is_plugin(member):
            return isclass(member) and issubclass(member, cls)

        for entrypoint in cls.entrypoints():
            plugin_module = entrypoint.load()
            for _, member in getmembers(plugin_module, is_plugin):
                yield member

    @classmethod
    def import_all(cls):
        for _ in cls.all():
            pass
