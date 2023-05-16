from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List

from yaml import SafeLoader, load

from ..argument_resolvers import ARGUMENT_RESOLVER_REGISTRY
from ..exceptions import InvalidPipelineDefinitionError
from ..value_providers import VALUE_PROVIDER_REGISTRY
from .class_loader import ClassLoader
from .pipeline import Pipeline


class PipelineFileSafeLoader(SafeLoader):
    was_configured = False

    @classmethod
    def configure(cls):
        if cls.was_configured:
            return

        for value_provider in VALUE_PROVIDER_REGISTRY.all_subclasses:
            value_provider.install_yaml_tag(cls)
        for argument_resolver in ARGUMENT_RESOLVER_REGISTRY.all_subclasses:
            argument_resolver.install_yaml_tag(cls)

        cls.was_configured = True

    @classmethod
    def load_file_by_path(cls, file_path: str):
        PipelineFileSafeLoader.configure()
        with open(file_path) as fp:
            return load(fp, cls)


@dataclass(slots=True)
class PipelineInitializationArguments:
    annnotations: Optional[List[str]] = None

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
        if "annnotations" in step and self.annnotations is not None:
            if not set(step.pop("annnotations")).intersection(self.annnotations):
                return False

        return True


class PipelineFileLoader:
    def __init__(self, file_path: Path):
        self.file_path = file_path

    def load_pipeline(
        self, init_args: Optional[PipelineInitializationArguments] = None
    ) -> Pipeline:
        init_args = init_args or PipelineInitializationArguments()
        return self.load_pipeline_from_file_data(
            self.load_pipeline_file_data(), init_args
        )

    def load_pipeline_from_file_data(
        self, file_data, init_args: PipelineInitializationArguments
    ):
        if not isinstance(file_data, list):
            raise InvalidPipelineDefinitionError(
                "File should be a list of step classs to load"
            )

        return init_args.initialize_from_file_data(file_data)

    def load_pipeline_file_data(self):
        return PipelineFileSafeLoader.load_file_by_path(self.file_path)
