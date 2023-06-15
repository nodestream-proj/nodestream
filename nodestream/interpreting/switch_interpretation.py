from typing import Any, Dict

from ..exceptions import UnhandledBranchError
from ..model import AggregatedIntrospectiveIngestionComponent, InterpreterContext
from ..value_providers import StaticValueOrValueProvider, ValueProvider
from .interpretation import Interpretation


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
            field_value: Interpretation.from_file_data(**interpretation)
            for field_value, interpretation in cases.items()
        }
        self.default = Interpretation.from_file_data(**default) if default else None
        self.normalization = normalization or {}
        self.fail_on_unhandled = fail_on_unhandled

    def all_subordinate_components(self):
        yield from self.interpretations.values()
        if self.default:
            yield self.default

    def interpret(self, context: InterpreterContext):
        value_to_look_for = self.switch_on.normalize_single_value(
            context, **self.normalization
        )
        if value_to_look_for not in self.interpretations:
            if self.default:
                self.default.interpret(context)
                return
            if self.fail_on_unhandled:
                raise UnhandledBranchError(value_to_look_for)
            else:
                return
        return self.interpretations[value_to_look_for].interpret(context)
