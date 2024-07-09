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

    @property
    def assigns_source_nodes(self) -> bool:
        pass


class NullInterpretationPass(InterpretationPass):
    def apply_interpretations(self, context: ProviderContext):
        yield context

    @property
    def assigns_source_nodes(self) -> bool:
        return False


"""
A switch interpretation can assign a source node
A Single Sequence interpretation pass can assign source nodes.
    InterpretationPass(
        switch_interpretation(
            InterpretationPass(
                
            )
        )
    )
The switch interpretation does not assign the source node but it can within the pass.
We cannot be context agnostic, we need to know if we are the ones creating the source node.
If we know we are creating the source node, we can ignore whether the nested interpretations are:

if self.creates_source_nodes:
    create the thing -> propagates to the level above. 

creates_source_nodes: for propagation upwards
exclusively_creates_source_nodes: for propagation downwards. I am creating the source node. 


"""


class SingleSequenceInterpretationPass(InterpretationPass):
    __slots__ = ("interpretations",)

    @classmethod
    def from_file_data(cls, interpretation_arg_list):
        interpretations = (
            Interpretation.from_file_data(**args) for args in interpretation_arg_list
        )
        return cls(*interpretations)

    def __init__(self, *interpretations: Interpretation):
        self.interpretations: list[Interpretation] = interpretations

    @property
    def assigns_source_nodes(self) -> bool:
        return any(
            interpretation.assigns_source_nodes
            for interpretation in self.interpretations
        )

    # If we assign a source node at this level, add to the schema at this level.
    @property
    def exclusively_assigns_source_nodes(self) -> bool:
        return any(
            (
                interpretation.assigns_source_nodes
                and not isinstance(interpretation, ExpandsSchemaFromChildren)
            )
            for interpretation in self.interpretations
        )

    def apply_interpretations(self, context: ProviderContext):
        for interpretation in self.interpretations:
            interpretation.interpret(context)
        yield context

    def interpret(self, context: ProviderContext):
        for interpretation in self.interpretations:
            interpretation.interpret(context)

    @property
    def schema_ordered_interpretations(self):
        source_node_interpretation = None
        interpretations_copy = [
            interpretation for interpretation in self.interpretations
        ]
        for index, interpretation in enumerate(self.interpretations):
            if interpretation.assigns_source_nodes:
                source_node_interpretation = interpretations_copy.pop(index)
        return interpretations_copy + (
            [source_node_interpretation]
            if source_node_interpretation is not None
            else []
        )

    def expand_schema(self, coordinator: SchemaExpansionCoordinator):
        interpretations = self.interpretations
        if self.assigns_source_nodes:
            interpretations = self.schema_ordered_interpretations

        for interpretation in interpretations:
            interpretation.expand_schema(coordinator=coordinator)

        if self.exclusively_assigns_source_nodes:
            coordinator.clear_aliases()


# For each interpretationpass they maintain their own context hence, they expand schema from children.
class MultiSequenceInterpretationPass(ExpandsSchemaFromChildren, InterpretationPass):
    __slots__ = ("passes",)

    @classmethod
    def from_file_data(cls, args):
        return cls(*(InterpretationPass.from_file_data(arg) for arg in args))

    def __init__(self, *passes: InterpretationPass) -> None:
        self.passes: list[InterpretationPass] = passes

    @property
    def assigns_source_nodes(self) -> bool:
        return any(
            interpretation_pass.assigns_source_nodes
            for interpretation_pass in self.passes
        )

    @property
    def should_be_distinct(self) -> bool:
        return self.assigns_source_nodes

    def apply_interpretations(self, context: ProviderContext):
        for interpretation_pass in self.passes:
            provided_subcontext = deepcopy(context)
            for res in interpretation_pass.apply_interpretations(provided_subcontext):
                yield res

    def get_child_expanders(self) -> Iterable[ExpandsSchema]:
        yield from self.passes


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
        # Choose between the before_iteration and interpretation passes, which one creates the source nodes.
        # Process the schema for the one that processes source nodes last so that the context of the prior is involved for each pass.
        context_pass = (
            self.before_iteration
            if self.interpretations.assigns_source_nodes
            else self.interpretations
        )
        head_pass = (
            self.interpretations
            if self.interpretations.assigns_source_nodes
            else self.before_iteration
        )

        context_pass.expand_schema(coordinator)
        head_pass.expand_schema(coordinator)
