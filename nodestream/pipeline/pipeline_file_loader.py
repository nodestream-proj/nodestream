import hashlib
from dataclasses import dataclass, field
from logging import getLogger
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from ..file_io import (
    LazyLoadedArgument,
    LazyLoadedTagSafeLoader,
    LoadsFromYaml,
    LoadsFromYamlFile,
)
from ..schema import ExpandsSchema
from .argument_resolvers import set_config
from .class_loader import ClassLoader, find_class
from .normalizers import Normalizer
from .object_storage import NamespacedObjectStore, NullObjectStore, ObjectStore
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
    effective_config_values: Optional[ScopeConfig] = None
    object_store: ObjectStore = field(default_factory=ObjectStore.null)
    schema_only: bool = False

    @classmethod
    def for_introspection(cls) -> "PipelineInitializationArguments":
        return cls(annotations=["introspection"], schema_only=True)

    @classmethod
    def for_schema_collection(cls) -> "PipelineInitializationArguments":
        """Return arguments suitable for schema-only collection.

        Only steps that implement ExpandsSchema will be instantiated, avoiding
        steps that require external credentials or a full runtime environment.
        """
        return cls(annotations=["introspection"], schema_only=True)

    @classmethod
    def for_testing(cls) -> "PipelineInitializationArguments":
        return cls(annotations=["test"])


@dataclass(frozen=True, slots=True)
class StepDefinition(LoadsFromYaml):
    implementation_path: str
    factory_method_name: Optional[str] = None
    annotations: Optional[Set[str]] = None
    arguments: Optional[Dict[str, Any]] = None

    @classmethod
    def describe_yaml_schema(cls):
        from schema import Optional, Schema

        return Schema(
            {
                "implementation": str,
                Optional("factory"): str,
                Optional("annotations"): [str],
                Optional("arguments"): {str: object},
            }
        )

    @classmethod
    def from_file_data(cls, data):
        return cls(
            implementation_path=data["implementation"],
            factory_method_name=data.get("factory"),
            annotations=set(data.get("annotations", [])),
            arguments=data.get("arguments"),
        )

    def should_be_loaded(self, user_annotations: Optional[Set[str]]) -> bool:
        """Determine if this step should be loaded.

        Here are the rules:
            - If the step has no annotations, it will always be loaded.
            - If the user supplies no annotations, it will always be loaded.
            - If the step has annotations, and the user supplies annotations, it will only be loaded if the step's
                annotations are a subset of the user's annotations.

        Args:
            user_annotations: The annotations supplied by the user.

        Returns:
            True if the step should be loaded, False otherwise.
        """
        if not bool(self.annotations) or not bool(user_annotations):
            return True

        return bool(self.annotations.intersection(user_annotations))

    def expands_schema(self) -> bool:
        """Return True if the step class implements ExpandsSchema.

        Resolves the class by import path without instantiating it, so this
        is safe to call in a credential-free schema-collection context.
        """
        try:
            resolved_class = find_class(self.implementation_path)
            return issubclass(resolved_class, ExpandsSchema)
        except Exception:
            return False

    def load_step(self) -> Step:
        arguments = LazyLoadedArgument.resolve_if_needed(self.arguments)
        return ClassLoader.instance(Step).load_class(
            implementation=self.implementation_path,
            arguments=arguments,
            factory=self.factory_method_name,
        )


class PipelineFileContents(LoadsFromYamlFile):
    @classmethod
    def describe_yaml_schema(cls):
        from schema import Schema

        return Schema([StepDefinition.describe_yaml_schema()])

    @classmethod
    def get_loader(cls):
        PipelineFileSafeLoader.configure()
        return PipelineFileSafeLoader

    @classmethod
    def from_file_data(cls, data: List[Dict]):
        step_definitions = [
            StepDefinition.from_file_data(definition_data) for definition_data in data
        ]
        return cls(step_definitions)

    def __init__(self, step_definitions: List[StepDefinition]) -> None:
        self.step_definitions = step_definitions

    def initialize_with_arguments(self, init_args: PipelineInitializationArguments):
        with set_config(init_args.effective_config_values):
            steps_defined_in_file = []
            for step_definition in self.step_definitions:
                if not step_definition.should_be_loaded(init_args.annotations):
                    continue
                if init_args.schema_only and not step_definition.expands_schema():
                    continue
                try:
                    steps_defined_in_file.append(step_definition.load_step())
                except Exception:
                    if not init_args.schema_only:
                        raise
            steps = steps_defined_in_file + (init_args.extra_steps or [])
        return Pipeline(
            steps,
            step_outbox_size=init_args.step_outbox_size,
            object_store=init_args.object_store,
        )


class PipelineFile:
    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.logger = getLogger(self.__class__.__name__)

    def file_sha_256(self) -> str:
        sha = hashlib.sha256()
        b = bytearray(128 * 1024)
        mv = memoryview(b)
        with self.file_path.open("rb", buffering=0) as file:
            while n := file.readinto(mv):
                sha.update(mv[:n])
        return sha.hexdigest()

    def load_pipeline(
        self, init_args: Optional[PipelineInitializationArguments] = None
    ) -> Pipeline:
        self.logger.info("Loading Pipeline")
        init_args = init_args or PipelineInitializationArguments()
        init_args.object_store = init_args.object_store.namespaced(
            self.file_path.stem + "-" + self.file_sha_256()
        )
        inner_store = init_args.object_store
        if isinstance(inner_store, NamespacedObjectStore):
            inner_store = inner_store.store
        if isinstance(inner_store, NullObjectStore):
            self.logger.info("Using null ObjectStore. No persistence is configured.")
        contents = self.get_contents()
        return contents.initialize_with_arguments(init_args)

    def load_pipeline_for_introspection(self) -> Pipeline:
        initialization_arguments = PipelineInitializationArguments.for_introspection()
        contents = self.get_contents()
        return contents.initialize_with_arguments(initialization_arguments)

    def load_pipeline_for_schema_collection(self) -> Pipeline:
        """Load only schema-expanding steps, skipping those that require credentials."""
        initialization_arguments = (
            PipelineInitializationArguments.for_schema_collection()
        )
        contents = self.get_contents()
        return contents.initialize_with_arguments(initialization_arguments)

    def get_contents(self) -> PipelineFileContents:
        return PipelineFileContents.read_from_file(self.file_path)
