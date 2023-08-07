from typing import Any, Dict, List

from ...pipeline.value_providers import (
    ProviderContext,
    StaticValueOrValueProvider,
    ValueProvider,
)
from ...schema.schema import AggregatedIntrospectiveIngestionComponent
from .interpretation import Interpretation


class UnhandledBranchError(ValueError):
    """Raised when a branch is not handled in a switch case."""

    def __init__(self, missing_branch_value, *args: object) -> None:
        super().__init__(
            f"'{missing_branch_value}' was not matched in switch case", *args
        )


class SwitchInterpretation(
    AggregatedIntrospectiveIngestionComponent, Interpretation, alias="switch"
):
    __slots__ = (
        "switch_on",
        "interpretations",
        "default",
        "normalization",
        "fail_on_unhandled",
    )

    @staticmethod
    def guarantee_interpretation_list_from_file_data(file_data) -> List[Interpretation]:
        if isinstance(file_data, list):
            return [
                Interpretation.from_file_data(**interpretation)
                for interpretation in file_data
            ]

        return [Interpretation.from_file_data(**file_data)]

    def __init__(
        self,
        switch_on: StaticValueOrValueProvider,
        cases: Dict[str, dict],
        default: Dict[str, Any] = None,
        normalization: Dict[str, Any] = None,
        fail_on_unhandled: bool = True,
    ):
        self.switch_on = ValueProvider.guarantee_value_provider(switch_on)
        self.interpretations = {
            field_value: self.guarantee_interpretation_list_from_file_data(
                interpretation
            )
            for field_value, interpretation in cases.items()
        }
        self.default = (
            self.guarantee_interpretation_list_from_file_data(default)
            if default
            else None
        )
        self.normalization = normalization or {}
        self.fail_on_unhandled = fail_on_unhandled

    def all_subordinate_components(self):
        for child in self.interpretations.values():
            yield from child
        if self.default:
            yield from self.default

    def interpret(self, context: ProviderContext):
        key = self.switch_on.normalize_single_value(context, **self.normalization)
        interpretations = self.interpretations.get(key, self.default)

        if interpretations is None:
            if self.fail_on_unhandled:
                raise UnhandledBranchError(key)
            else:
                interpretations = []

        for interpretation in interpretations:
            interpretation.interpret(context)
