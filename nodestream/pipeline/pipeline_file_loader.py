from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from yaml import SafeLoader, load

from nodestream.pipeline.scope_config import ScopeConfig

from .argument_resolvers import ArgumentResolver
from .class_loader import ClassLoader
from .normalizers import Normalizer
from .pipeline import Pipeline
from .value_providers import ValueProvider


class InvalidPipelineDefinitionError(ValueError):
    """Raised when a pipeline definition is invalid."""

    pass


class PipelineFileSafeLoader(SafeLoader):
    """A YAML loader that can load pipeline files.""" ""

    was_configured = False

    @classmethod
    def configure(cls, config: ScopeConfig = None):
        if config:
            cls.add_constructor(
                "!config",
                lambda loader, node: config.get_config_value(
                    loader.construct_scalar(node)
                ),
            )

        if cls.was_configured:
            return

        for normalizer in Normalizer.all():
            normalizer.setup()
        for value_provider in ValueProvider.all():
            value_provider.install_yaml_tag(cls)
        for argument_resolver in ArgumentResolver.all():
            argument_resolver.install_yaml_tag(cls)

        cls.was_configured = True

    @classmethod
    def load_file_by_path(cls, file_path: str, config: ScopeConfig = None):
        PipelineFileSafeLoader.configure(config)
        with open(file_path) as fp:
            return load(fp, cls)


class NodestreamProjectFileSafeLoader(SafeLoader):
    """A YAML loader that can load nodestream project files.""" ""

    was_configured = False

    @classmethod
    def configure(cls):
        if cls.was_configured:
            return
        for argument_resolver in ArgumentResolver.all():
            argument_resolver.install_yaml_tag(cls)

        cls.was_configured = True


@dataclass(slots=True)
class PipelineInitializationArguments:
    """Arguments used to initialize a pipeline from a file."""

    annotations: Optional[List[str]] = None

    @classmethod
    def for_introspection(cls):
        return cls(annotations=["introspection"])

    @classmethod
    def for_testing(cls):
        return cls(annotations=["test"])

    def initialize_from_file_data(self, file_data: List[dict]):
        return Pipeline(self.load_steps(ClassLoader(), file_data))

    def load_steps(self, class_loader, file_data):
        return [
            class_loader.load_class(**step_data)
            for step_data in file_data
            if self.should_load_step(step_data)
        ]

    def should_load_step(self, step):
        return self.step_is_tagged_properly(step)

    def step_is_tagged_properly(self, step):
        if "annotations" in step and self.annotations is not None:
            if not set(step.pop("annotations")).intersection(self.annotations):
                return False

        return True


class PipelineFileLoader:
    """Loads a pipeline from a YAML file."""

    def __init__(self, file_path: Path):
        self.file_path = file_path

    def load_pipeline(
        self,
        init_args: Optional[PipelineInitializationArguments] = None,
        config: ScopeConfig = None,
    ) -> Pipeline:
        init_args = init_args or PipelineInitializationArguments()
        return self.load_pipeline_from_file_data(
            self.load_pipeline_file_data(config), init_args
        )

    def load_pipeline_from_file_data(
        self, file_data, init_args: PipelineInitializationArguments
    ):
        if not isinstance(file_data, list):
            raise InvalidPipelineDefinitionError(
                "File should be a list of step class to load"
            )

        return init_args.initialize_from_file_data(file_data)

    def load_pipeline_file_data(self, config: ScopeConfig = None):
        return PipelineFileSafeLoader.load_file_by_path(self.file_path, config)
