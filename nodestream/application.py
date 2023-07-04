import os
import sys
from abc import ABC, abstractmethod
from functools import cache
from importlib.metadata import entry_points, version
from typing import List, Type, Iterable, Optional
from pathlib import Path

from cleo.application import Application

from .cli.commands import (
    NodestreamCommand,
    New,
    Show,
    Scaffold,
    Remove,
    Run,
    PrintSchema,
    AuditCommand,
)
from .project import Project
from .project.audits import (
    Audit,
    AuditTimeToLiveConfigurations,
    AuditReferentialIntegrity,
)

NODESTREAM_PLUGINS_ENTRYPOINT_GROUP = "nodestream.plugins"
PROJECT_PLUGINS_ENTRYPOINT_NAME = "project"
APPLICATION_PLUGINS_ENTRYPOINT_NAME = "application"
BUILTIN_AUDITS = (AuditTimeToLiveConfigurations, AuditReferentialIntegrity)
BUILTIN_COMMANDS = (
    New,
    PrintSchema,
    Remove,
    Run,
    Scaffold,
    Show,
)


class Plugin(ABC):
    @classmethod
    @cache
    def all_entrypoints(cls):
        return entry_points(group=NODESTREAM_PLUGINS_ENTRYPOINT_GROUP)

    @classmethod
    def load_from_entrypoint_name(cls, name: str):
        for entrypoint in cls.all_entrypoints():
            try:
                plugin_cls = entrypoint[name].load()
                if issubclass(plugin_cls, cls):
                    yield plugin_cls()
            except KeyError:
                pass


class ProjectPlugin(Plugin):
    @classmethod
    def all(cls) -> Iterable["ProjectPlugin"]:
        return cls.load_from_entrypoint_name(PROJECT_PLUGINS_ENTRYPOINT_NAME)

    @abstractmethod
    def activate(self, project: Project):
        raise NotImplementedError


class ApplicationPlugin(Plugin):
    @classmethod
    def all(cls) -> Iterable["ApplicationPlugin"]:
        return cls.load_from_entrypoint_name(APPLICATION_PLUGINS_ENTRYPOINT_NAME)

    @abstractmethod
    def activate(self, application: "Nodestream"):
        raise NotImplementedError


class Nodestream:
    _instance: Optional["Nodestream"] = None

    @classmethod
    def default(cls) -> "Nodestream":
        instance = Nodestream()
        for command in BUILTIN_COMMANDS:
            instance.add_command(command)

        for audit in BUILTIN_AUDITS:
            instance.add_audit(audit)

        for plugin in ApplicationPlugin.all():
            plugin.activate(instance)

        return instance

    @classmethod
    def instance(cls) -> "Nodestream":
        if cls._instance is None:
            cls._instance = cls.default()

        return cls._instance

    def __init__(self) -> None:
        self.commands: List[NodestreamCommand] = []

    def add_audit(self, audit_cls: Type[Audit]):
        self.add_command(AuditCommand.for_audit(audit_cls))

    def add_command(self, command_cls: Type[NodestreamCommand]):
        self.commands.append(command_cls())

    def make_cli(self) -> Application:
        app = Application(name="nodestream", version=version("nodestream"))
        for command in self.commands:
            app.add(command)
        return app

    def run_cli(self):
        self.make_cli().run()

    def get_project(self, path: Path) -> Project:
        project = Project.read_from_file(path)
        for plugin in ProjectPlugin.all():
            plugin.activate(project)
        return project


def run():
    sys.path.append(os.getcwd())
    Nodestream.instance().run_cli()
