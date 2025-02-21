import re
from abc import abstractmethod
from logging import getLogger
from typing import Any, AsyncGenerator, Dict, Iterable, List, Optional

from .flush import Flush
from .object_storage import ObjectStore
from .step import PassStep, Step, StepContext
from .value_providers import ProviderContext, StaticValueOrValueProvider, ValueProvider


class Filter(Step):
    """A `Filter` takes a given record and evaluates whether or not it should continue downstream.

    `Filter` steps generally make up the middle of an ETL pipeline and are
    responsible for ensuring only relevant records make it through.
    """

    async def process_record(self, record, _) -> AsyncGenerator[object, None]:
        if record is Flush or not await self.filter_record(record):
            yield record

    @abstractmethod
    async def filter_record(self, record: Any) -> bool:
        raise NotImplementedError


class ValueMatcher:
    @classmethod
    def from_file_data(
        cls,
        value: StaticValueOrValueProvider,
        possibilities: Iterable[StaticValueOrValueProvider],
        normalization: Optional[Dict[str, Any]] = None,
    ):
        return cls(
            value_provider=ValueProvider.guarantee_value_provider(value),
            possibilities=ValueProvider.guarantee_provider_list(possibilities),
            normalization=(normalization or {}),
        )

    def __init__(
        self,
        value_provider: ValueProvider,
        possibilities: Iterable[ValueProvider],
        normalization: Dict[str, Any],
    ) -> None:
        self.value_provider = value_provider
        self.possibilities = possibilities
        self.normalization = normalization

    def does_match(self, context: ProviderContext):
        actual_value = self.value_provider.normalize_single_value(
            context, self.normalization
        )

        return any(
            possibility_provider.normalize_single_value(context, self.normalization)
            == actual_value
            for possibility_provider in self.possibilities
        )


class ValuesMatchPossibilitiesFilter(Filter):
    """A filter that checks if a given value matches any of a set of possibilities."""

    @classmethod
    def from_file_data(cls, *, fields: Iterable[Dict[str, Any]]):
        value_matchers = [ValueMatcher.from_file_data(**field) for field in fields]
        return cls(value_matchers=value_matchers)

    def __init__(self, value_matchers: Iterable[ValueMatcher]):
        self.value_matchers = value_matchers

    async def filter_record(self, item):
        context_from_record = ProviderContext(item, None)
        return not all(
            matcher.does_match(context_from_record) for matcher in self.value_matchers
        )


class ExcludeWhenValuesMatchPossibilities(Filter):
    @classmethod
    def from_file_data(cls, **kwargs):
        inner = ValuesMatchPossibilitiesFilter.from_file_data(**kwargs)
        return cls(inner)

    def __init__(self, inner: ValuesMatchPossibilitiesFilter) -> None:
        self.inner = inner

    async def filter_record(self, record: Any) -> bool:
        return not await self.inner.filter_record(record)


class RegexMatcher:
    @classmethod
    def from_file_data(
        cls,
        value: StaticValueOrValueProvider,
        regex: str,
        include: bool = True,
        normalization: Optional[Dict[str, Any]] = None,
    ):
        return cls(
            value_provider=ValueProvider.guarantee_value_provider(value),
            regex=regex,
            include=include,
            normalization=(normalization or {}),
        )

    def __init__(
        self,
        value_provider: StaticValueOrValueProvider,
        regex: StaticValueOrValueProvider,
        include: bool,
        normalization: Dict[str, Any],
    ) -> None:
        self.value_provider = value_provider
        self.regex = re.compile(regex)
        self.include = include
        self.normalization = normalization

    def should_include(self, context: ProviderContext) -> bool:
        actual_value = self.value_provider.normalize_single_value(
            context, self.normalization
        )
        match = self.regex.match(actual_value) is not None
        return not match if self.include else match


class ValueMatchesRegexFilter(Filter):
    """A filter that includes/excludes a given value based on a matched regex."""

    @classmethod
    def from_file_data(
        cls,
        value: StaticValueOrValueProvider,
        regex: StaticValueOrValueProvider,
        include: bool,
        normalize: Optional[Dict[str, Any]] = None,
    ):
        regex_matcher = RegexMatcher.from_file_data(value, regex, include, normalize)
        return cls(regex_matcher=regex_matcher)

    def __init__(self, regex_matcher: RegexMatcher):
        self.regex_matcher = regex_matcher

    async def filter_record(self, item: Any):
        context_from_record = ProviderContext(item, None)
        return self.regex_matcher.should_include(context_from_record)


try:
    import genson
    import jsonschema

    INFERRED_OBJECT_SCHEMA_KEY = "inferred_object_schema"

    class SchemaEnforcementMode:
        """A mode of schema enforcement that can transition between different states."""

        def should_filter(self, record) -> bool:
            return True  # pragma: no cover

        def start(self, object_store: ObjectStore) -> "SchemaEnforcementMode":
            return self  # pragma: no cover

        def inform_mode_change(self) -> "SchemaEnforcementMode":
            return self  # pragma: no cover

        @staticmethod
        def from_enforcement_policy(
            enforcement_policy: str, schema: "Schema"
        ) -> "SchemaEnforcementMode":
            if enforcement_policy == "enforce":
                return EnforceSchema(schema)
            elif enforcement_policy == "warn":
                return WarnSchema(schema)
            raise ValueError(f"Invalid enforcement policy: {enforcement_policy}")

    class Schema:
        """A schema that can be enforced on records."""

        def __init__(self, schema_dict):
            self.schema_dict = schema_dict
            self.validator = jsonschema.Draft7Validator(schema_dict)

        def validate(self, record) -> Optional[List[str]]:
            errors = sorted(self.validator.iter_errors(record), key=lambda e: e.path)
            return list(map(lambda e: e.message, errors))

        def persist(self, object_store: ObjectStore, key: str):
            object_store.put_picklable(key, self.schema_dict)

        @staticmethod
        def get_from_object_store(
            object_store: ObjectStore, key: str
        ) -> Optional["Schema"]:
            if schema_dict := object_store.get_pickled(key):
                return Schema(schema_dict)
            return None

    class SchemaBuilder:
        """A helper class to build a schema from records."""

        def __init__(self):
            self._inner = genson.SchemaBuilder()
            self.collected_samples = 0

        def add_record(self, record):
            self._inner.add_object(record)
            self.collected_samples += 1

        def build_schema(self) -> Schema:
            return Schema(self._inner.to_schema())

    class FetchSchema(SchemaEnforcementMode):
        """Fetches a schema from the object store and enforces it."""

        def __init__(self, key: str, enforcement_policy: str):
            self.key = key
            self.enforcement_policy = enforcement_policy

        def start(self, object_store: ObjectStore):
            if schema := Schema.get_from_object_store(object_store, self.key):
                return SchemaEnforcementMode.from_enforcement_policy(
                    self.enforcement_policy, schema
                )

            raise ValueError(f"Schema with key {self.key} not found in object store.")

    class InferSchema(SchemaEnforcementMode):
        """Infers a schema from the first `sample_size` records.

        If the schema is already present in the object store, it will switch to
        out of inferring immediately. Otherwise, it will infer the schema from the
        first `sample_size` records and then switch.

        Whether or not the switch is to ENFORCE or WARN mode is determined by
        the `warn` parameter.
        """

        def __init__(self, sample_size: int, enforcement_policy: str):
            self.sample_size = sample_size
            self.builder = SchemaBuilder()
            self.enforcement_policy = enforcement_policy
            self.logger = getLogger(self.__class__.__name__)
            self.object_store = None

        def start(self, object_store):
            self.object_store = object_store
            if schema := Schema.get_from_object_store(
                object_store, INFERRED_OBJECT_SCHEMA_KEY
            ):
                return self.switch_out(schema)
            return self

        def should_filter(self, record):
            self.builder.add_record(record)
            self.logger.debug(
                "In infer mode. Not filtering.",
                extra={"samples": self.builder.collected_samples},
            )
            return False

        def switch_out(self, schema: Schema) -> SchemaEnforcementMode:
            self.logger.info("Switching out of infer mode.")
            schema.persist(self.object_store, INFERRED_OBJECT_SCHEMA_KEY)
            return SchemaEnforcementMode.from_enforcement_policy(
                self.enforcement_policy, schema
            )

        def inform_mode_change(self):
            if self.builder.collected_samples >= self.sample_size:
                return self.switch_out(self.builder.build_schema())

            return self

    class EnforceSchema(SchemaEnforcementMode):
        """Filters records that do not match the schema.

        Once in this mode, no transition is possible. The schema is enforced
        until the end of the pipeline.
        """

        def __init__(self, schema: Schema):
            self.schema = schema
            self.logger = getLogger(self.__class__.__name__)

        def should_filter(self, record):
            if errors := self.schema.validate(record):
                self.logger.error(
                    "Schema validation failed. Filtering due to ENFORCE mode.",
                    extra={"errors": errors},
                )
            return bool(errors)

    class WarnSchema(SchemaEnforcementMode):
        """Permits (while warning about) records that do not match the schema.

        Once in this mode, no transition is possible. The schema is warned about
        until the end of the pipeline.
        """

        def __init__(self, schema: Schema):
            self.schema = schema
            self.logger = getLogger(self.__class__.__name__)

        def should_filter(self, record):
            if errors := self.schema.validate(record):
                self.logger.warning(
                    "Schema validation failed. NOT filtering due to WARN mode.",
                    extra={"errors": errors},
                )
            return False

    class SchemaEnforcer(Filter):
        """A filter that enforces a schema on records.

        This effectively works as a state machine that transitions between
        different modes of schema enforcement.
        """

        @classmethod
        def from_file_data(
            cls,
            enforcement_policy: str = "enforce",
            key: str = None,
            inference_sample_size: int = 1000,
        ):
            if key:
                return cls(FetchSchema(key, enforcement_policy))
            return cls(InferSchema(inference_sample_size, enforcement_policy))

        def __init__(self, mode: SchemaEnforcementMode):
            self.mode = mode

        async def start(self, context: StepContext):
            self.mode = self.mode.start(context.object_store)

        async def filter_record(self, record):
            self.mode = self.mode.inform_mode_change()
            return self.mode.should_filter(record)

except ImportError:

    class SchemaEnforcer(PassStep):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            raise ImportError(
                "SchemaEnforcer requires genson and jsonschema to be installed. Install the `validation` extra."
            )
