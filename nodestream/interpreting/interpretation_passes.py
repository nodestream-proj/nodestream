from abc import ABC, abstractmethod
from copy import deepcopy
from os import environ
from typing import Iterable

from nodestream.schema.state import SchemaExpansionCoordinator

from ..pipeline.value_providers import ProviderContext
from ..schema import ExpandsSchema, ExpandsSchemaFromChildren
from .interpretations import Interpretation

COMPLETENESS_ERROR_MESSAGE = "Each Interpreter Pass in the Multi Sequence interpreter pass must define a source node."
UNIQUENESS_ERROR_MESSAGE = (
    "Only one interpretation can generate a source node within an interpretation pass."
)
VALIDATION_FLAG = "VALIDATE_PIPELINE_SCHEMA"


class InterpretationPassError(Exception):
    pass


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


class SingleSequenceInterpretationPass(InterpretationPass):
    __slots__ = ("interpretations",)

    @classmethod
    def from_file_data(cls, interpretation_arg_list):
        interpretations = (
            Interpretation.from_file_data(**args) for args in interpretation_arg_list
        )
        return_class = cls(*interpretations)
        return_class.verify_uniqueness()
        return return_class

    def __init__(self, *interpretations: Interpretation):
        self.interpretations = interpretations

    # Verifies that there is only one source node creator in all the interpretations
    def verify_uniqueness(self):
        source_node_generator_count = 0
        for interpretation in self.interpretations:
            if interpretation.assigns_source_nodes:
                source_node_generator_count += 1
        if source_node_generator_count > 1 and environ.get(VALIDATION_FLAG, False):
            raise InterpretationPassError(COMPLETENESS_ERROR_MESSAGE)

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

    # Ordering the interpretations to leave the source node creator last.
    @property
    def schema_ordered_interpretations(self):
        source_node_interpretation = None
        interpretations_copy = [
            interpretation for interpretation in self.interpretations
        ]

        for index, interpretation in enumerate(interpretations_copy):
            if interpretation.assigns_source_nodes:
                source_node_interpretation = interpretations_copy.pop(index)

        return interpretations_copy + (
            [source_node_interpretation]
            if source_node_interpretation is not None
            else []
        )

    def expand_schema(self, coordinator: SchemaExpansionCoordinator):
        for interpretation in self.schema_ordered_interpretations:
            interpretation.expand_schema(coordinator=coordinator)

        if self.exclusively_assigns_source_nodes:
            coordinator.clear_aliases()


# For each interpretationpass they maintain their own context hence, they expand schema from children.
class MultiSequenceInterpretationPass(ExpandsSchemaFromChildren, InterpretationPass):
    __slots__ = ("passes",)

    @classmethod
    def from_file_data(cls, args):
        interpretation_passes = (InterpretationPass.from_file_data(arg) for arg in args)
        return_class = cls(*interpretation_passes)
        return_class.verify_completeness()
        return return_class

    def __init__(self, *passes: InterpretationPass) -> None:
        self.passes: list[InterpretationPass] = passes

    # If any sequence assigns source nodes, all of the passes must assign source nodes.
    def verify_completeness(self):
        if (
            self.assigns_source_nodes
            and not all(
                interpretation_pass.assigns_source_nodes
                for interpretation_pass in self.passes
            )
            and environ.get(VALIDATION_FLAG, False)
        ):
            raise InterpretationPassError(COMPLETENESS_ERROR_MESSAGE)

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
