from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Iterable

from nodestream.schema.state import SchemaExpansionCoordinator

from ..pipeline import Transformer
from ..pipeline.value_providers import ProviderContext
from ..schema import ExpandsSchema, ExpandsSchemaFromChildren
from .interpretations import Interpretation
from .record_decomposers import RecordDecomposer


class InterpretationPass(ExpandsSchema, ABC):
    @classmethod
    def from_file_data(self, args):
        if args is None:
            return NullInterpretationPass()

        if len(args) > 0 and isinstance(args[0], list):
            return MultiSequenceInterpretationPass.from_file_data(args)

        return SingleSequenceInterpretationPass.from_file_data(args)

    @abstractmethod
    def apply_interpretations(self, context: ProviderContext):
        pass


class NullInterpretationPass(InterpretationPass):
    def apply_interpretations(self, context: ProviderContext):
        yield context


class MultiSequenceInterpretationPass(ExpandsSchemaFromChildren, InterpretationPass):
    __slots__ = ("passes",)

    @classmethod
    def from_file_data(cls, args):
        return cls(*(InterpretationPass.from_file_data(arg) for arg in args))

    def __init__(self, *passes: InterpretationPass) -> None:
        self.passes = passes

    def apply_interpretations(self, context: ProviderContext):
        for interpretation_pass in self.passes:
            provided_subcontext = deepcopy(context)
            for res in interpretation_pass.apply_interpretations(provided_subcontext):
                yield res

    def get_child_expanders(self) -> Iterable[ExpandsSchema]:
        yield from self.passes


class SingleSequenceInterpretationPass(ExpandsSchemaFromChildren, InterpretationPass):
    __slots__ = ("interpretations",)

    @classmethod
    def from_file_data(cls, interpretation_arg_list):
        interpretations = (
            Interpretation.from_file_data(**args) for args in interpretation_arg_list
        )
        return cls(*interpretations)

    def __init__(self, *interpretations: Interpretation):
        self.interpretations = interpretations

    def apply_interpretations(self, context: ProviderContext):
        for interpretation in self.interpretations:
            interpretation.interpret(context)
        yield context

    def get_child_expanders(self) -> Iterable[ExpandsSchema]:
        yield from self.interpretations


class Interpreter(Transformer, ExpandsSchema):
    __slots__ = (
        "before_iteration",
        "interpretations",
        "decomposer",
    )

    @classmethod
    def from_file_data(cls, interpretations, before_iteration=None, iterate_on=None):
        # Import all interpretation plugins before we try to load any interpretations.
        Interpretation.import_all()

        return cls(
            before_iteration=InterpretationPass.from_file_data(before_iteration),
            interpretations=InterpretationPass.from_file_data(interpretations),
            decomposer=RecordDecomposer.from_iteration_arguments(iterate_on),
        )

    def __init__(
        self,
        before_iteration: InterpretationPass,
        interpretations: InterpretationPass,
        decomposer: RecordDecomposer,
    ) -> None:
        self.before_iteration = before_iteration
        self.interpretations = interpretations
        self.decomposer = decomposer

    async def transform_record(self, record):
        for output_context in self.interpret_record(record):
            yield output_context.desired_ingest

    def interpret_record(self, record):
        context = ProviderContext.fresh(record)
        for base_context in self.before_iteration.apply_interpretations(context):
            for sub_context in self.decomposer.decompose_record(base_context):
                yield from self.interpretations.apply_interpretations(sub_context)

    def expand_schema(self, coordinator: SchemaExpansionCoordinator):
        self.before_iteration.expand_schema(coordinator)
        self.interpretations.expand_schema(coordinator)
        coordinator.clear_aliases()
