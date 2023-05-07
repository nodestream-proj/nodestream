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


class PipelineFileLoader:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def load_pipeline(self) -> Pipeline:
        return self.load_pipeline_from_file_data(self.load_pipeline_file_data())

    def load_pipeline_from_file_data(self, file_data):
        if not isinstance(file_data, list):
            raise InvalidPipelineDefinitionError(
                "File should be a list of step classs to load"
            )

        class_loader = ClassLoader()
        return Pipeline([class_loader.load_class(**args) for args in file_data])

    def load_pipeline_file_data(self):
        return PipelineFileSafeLoader.load_file_by_path(self.file_path)
