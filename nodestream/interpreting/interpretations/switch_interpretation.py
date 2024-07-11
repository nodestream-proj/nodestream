from os import environ
from typing import Any, Dict, Iterable, List

from ...pipeline.value_providers import (
    ProviderContext,
    StaticValueOrValueProvider,
    ValueProvider,
)
from ...schema import ExpandsSchema, ExpandsSchemaFromChildren
from ..interpretation_passes import InterpretationPass, MultiSequenceInterpretationPass
from .interpretation import Interpretation

SWITCH_COMPLETENESS_ERROR_MESSAGE = (
    "Every branch within the switch interpreter must contain a source node generator."
)
INVALID_SWITCH_ERROR_MESSAGE = (
    "Switch interpretations cannot handle multiple interpretation passes."
)
VALIDATION_FLAG = "VALIDATE_PIPELINE_SCHEMA"


class UnhandledBranchError(ValueError):
    """Raised when a branch is not handled in a switch case."""

    def __init__(self, missing_branch_value, *args: object) -> None:
        super().__init__(
            f"'{missing_branch_value}' was not matched in switch case", *args
        )


class SwitchError(Exception):
    pass


class SwitchInterpretation(Interpretation, ExpandsSchemaFromChildren, alias="switch"):
    __slots__ = (
        "switch_on",
        "interpretations",
        "default",
        "normalization",
        "fail_on_unhandled",
    )

    @staticmethod
    def guarantee_single_interpretation_pass_from_file_data(
        file_data,
    ) -> InterpretationPass:
        # Check for multiinterpretationpass.
        if isinstance(file_data, list):
            interpretation_pass = InterpretationPass.from_file_data(file_data)
        else:
            interpretation_pass = InterpretationPass.from_file_data([file_data])

        # We do not support multiple interpretation passes within the switch interpretation.
        if isinstance(interpretation_pass, MultiSequenceInterpretationPass):
            raise SwitchError(INVALID_SWITCH_ERROR_MESSAGE)

        return interpretation_pass

    def __init__(
        self,
        switch_on: StaticValueOrValueProvider,
        cases: Dict[str, Interpretation | List[Interpretation]],
        default: Interpretation | List[Interpretation] = None,
        normalization: Dict[str, Any] = None,
        fail_on_unhandled: bool = True,
    ):
        self.switch_on = ValueProvider.guarantee_value_provider(switch_on)
        self.branches = {
            field_value: self.guarantee_single_interpretation_pass_from_file_data(
                interpretation
            )
            for field_value, interpretation in cases.items()
        }
        self.default = (
            self.guarantee_single_interpretation_pass_from_file_data(default)
            if default
            else None
        )
        self.normalization = normalization or {}
        self.fail_on_unhandled = fail_on_unhandled
        self.verify_completeness()

    @property
    def all_interpretations(self) -> Iterable[Interpretation]:
        yield from self.branches.values()
        if self.default:
            yield self.default

    @property
    def assigns_source_nodes(self) -> bool:
        # If all branches have at least one interpretation that assigns source nodes,
        # then the branches are distinct and we should not merge their schemas.
        return any(branch.assigns_source_nodes for branch in self.all_interpretations)

    # There is only a distinct context between children if this interpretation assign source nodes.
    @property
    def should_be_distinct(self) -> bool:
        return self.assigns_source_nodes

    # If this interpretation assigns source nodes, and not all of the branches including the default branch assign source nodes it is incomplete.
    def verify_completeness(self):
        if (
            self.assigns_source_nodes
            and not all(
                interpretation_pass.assigns_source_nodes
                for interpretation_pass in self.all_interpretations
            )
            and environ.get(VALIDATION_FLAG, False)
        ):
            raise SwitchError(SWITCH_COMPLETENESS_ERROR_MESSAGE)

    def get_child_expanders(self) -> Iterable[ExpandsSchema]:
        yield from self.all_interpretations

    def interpret(self, context: ProviderContext):
        key = self.switch_on.normalize_single_value(context, self.normalization)
        interpretation_pass = self.branches.get(key, self.default)

        if interpretation_pass is not None:
            next(interpretation_pass.apply_interpretations(context))
        else:
            if self.fail_on_unhandled:
                raise UnhandledBranchError(key)
