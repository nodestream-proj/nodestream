from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from ..file_io import LazyLoadedArgument, LazyLoadedTagSafeLoader, LoadsFromYamlFile
from .argument_resolvers import set_config
from .class_loader import ClassLoader
from .normalizers import Normalizer
from .pipeline import Pipeline
from .scope_config import ScopeConfig
from .step import Step
from .value_providers import ValueProvider


class InvalidPipelineDefinitionError(ValueError):
    """Raised when a pipeline definition is invalid."""

    pass


class PipelineFileSafeLoader(LazyLoadedTagSafeLoader):
    """A YAML loader that can load pipeline files.""" ""

    was_configured = False

    @classmethod
    def configure(cls):
        if cls.was_configured:
            return

        for normalizer in Normalizer.all():
            normalizer.setup()
        for value_provider in ValueProvider.all():
            value_provider.install_yaml_tag(cls)

        cls.was_configured = True


@dataclass(slots=True)
class PipelineInitializationArguments:
    """Arguments used to initialize a pipeline from a file."""

    step_outbox_size: int = 1000
    annotations: Optional[List[str]] = None
    on_effective_configuration_resolved: Optional[Callable[[List[Dict]], None]] = None
    extra_steps: Optional[List[Step]] = None
    effecitve_config_values: Optional[ScopeConfig] = None

    @classmethod
    def for_introspection(cls):
        return cls(annotations=["introspection"])

    @classmethod
    def for_testing(cls):
        return cls(annotations=["test"])

    def initialize_from_file_data(self, file_data: Any):
        with set_config(self.effecitve_config_values):
            return Pipeline(
                steps=self.load_steps(ClassLoader(Step), file_data),
                step_outbox_size=self.step_outbox_size,
            )

    def load_steps(self, class_loader, file_data):
        effective = self.get_effective_configuration(file_data)
        if self.on_effective_configuration_resolved:
            self.on_effective_configuration_resolved(file_data)
        in_file = [self.load_step(class_loader, **step_data) for step_data in effective]
        return in_file + (self.extra_steps or [])

    def load_step(self, class_loader, implementation, arguments=None, factory=None):
        arguments = LazyLoadedArgument.resolve_if_needed(arguments or {})
        return class_loader.load_class(
            implementation=implementation, arguments=arguments, factory=factory
        )

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


class PipelineFileContents(LoadsFromYamlFile):
    @classmethod
    def describe_yaml_schema(cls):
        from schema import Optional, Schema

        return Schema(
            [
                {
                    "implementation": str,
                    Optional("factory"): str,
                    Optional("annotations"): [str],
                    Optional("arguments"): {str: object},
                }
            ]
        )

    @classmethod
    def get_loader(cls):
        PipelineFileSafeLoader.configure()
        return PipelineFileSafeLoader

    @classmethod
    def from_file_data(cls, data):
        return cls(data)

    def __init__(self, data: List[Dict]) -> None:
        self.data = data

    def initalize_with_arguments(self, init_args: PipelineInitializationArguments):
        return init_args.initialize_from_file_data(self.data)


class PipelineFile:
    def __init__(self, file_path: Path):
        self.file_path = file_path

    def load_pipeline(
        self, init_args: Optional[PipelineInitializationArguments] = None
    ) -> Pipeline:
        init_args = init_args or PipelineInitializationArguments()
        contents = PipelineFileContents.read_from_file(self.file_path)
        return contents.initalize_with_arguments(init_args)
