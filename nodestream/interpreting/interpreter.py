from os import environ

from nodestream.schema.state import SchemaExpansionCoordinator

from ..pipeline import Transformer
from ..pipeline.value_providers import ProviderContext
from ..schema import ExpandsSchema
from .interpretation_passes import InterpretationPass
from .interpretations import Interpretation
from .record_decomposers import RecordDecomposer

INTERPRETER_UNIQUENESS_ERROR_MESSAGE = "The interpreter must only generate source nodes in either the before_iteration phase, or the interpretations phase, not both."
VALIDATION_FLAG = "VALIDATE_PIPELINE_SCHEMA"


class InterpreterError(Exception):
    pass


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
        interpreter_class = cls(
            before_iteration=InterpretationPass.from_file_data(before_iteration),
            interpretations=InterpretationPass.from_file_data(interpretations),
            decomposer=RecordDecomposer.from_iteration_arguments(iterate_on),
        )
        interpreter_class.verify_completeness()
        return interpreter_class

    def __init__(
        self,
        before_iteration: InterpretationPass,
        interpretations: InterpretationPass,
        decomposer: RecordDecomposer,
    ) -> None:
        self.before_iteration = before_iteration
        self.interpretations = interpretations
        self.decomposer = decomposer

    # Raise an error if both the interpretations and before_iteration phases create source nodes.
    def verify_completeness(self):
        if all(
            intepretater_phase.assigns_source_nodes
            for intepretater_phase in [self.before_iteration, self.interpretations]
        ) and environ.get(VALIDATION_FLAG, False):
            raise InterpreterError(INTERPRETER_UNIQUENESS_ERROR_MESSAGE)

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
