from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional

from yaml import SafeLoader, load

from .argument_resolvers import ArgumentResolver
from .class_loader import ClassLoader
from .normalizers import Normalizer
from .pipeline import Pipeline
from .scope_config import ScopeConfig
from .step import Step
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


@dataclass(slots=True)
class PipelineInitializationArguments:
    """Arguments used to initialize a pipeline from a file."""

    step_outbox_size: int = 1000
    annotations: Optional[List[str]] = None
    on_effective_configuration_resolved: Optional[Callable[[List[Dict]], None]] = None
    extra_steps: Optional[List[Step]] = None

    @classmethod
    def for_introspection(cls):
        return cls(annotations=["introspection"])

    @classmethod
    def for_testing(cls):
        return cls(annotations=["test"])

    def initialize_from_file_data(self, file_data: List[dict]):
        return Pipeline(
            steps=self.load_steps(ClassLoader(Step), file_data),
            step_outbox_size=self.step_outbox_size,
        )

    def load_steps(self, class_loader, file_data):
        effective = self.get_effective_configuration(file_data)
        if self.on_effective_configuration_resolved:
            self.on_effective_configuration_resolved(file_data)
        in_file = [class_loader.load_class(**step_data) for step_data in effective]
        return in_file + (self.extra_steps or [])

    def get_effective_configuration(self, file_data):
        return [
            step_data for step_data in file_data if self.should_load_step(step_data)
        ]

    def should_load_step(self, step):
        return self.step_is_tagged_properly(step)

    def step_is_tagged_properly(self, step):
        # By default, we need to load all steps. Only filter a step out if:
        #   1. We have set annotations during initialization (e.g from the cli)
        #   2. The step is annotated
        #   3. There is not an intersection between the annotations from (1) and (2).
        #
        # This function also has a side effect of removing the annotations from the step data.
        annotations_set_on_step = set(step.pop("annotations", []))
        if annotations_set_on_step and self.annotations:
            return annotations_set_on_step.intersection(self.annotations)

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
