from typing import Any, Dict, Optional

from ...pipeline.value_providers import ProviderContext, StaticValueOrValueProvider
from .interpretation import Interpretation
from .property_mapping import PropertyMapping


class ExtractVariablesInterpretation(Interpretation, alias="variables"):
    """Stores variables that can be used later in the processing of a record.

    You may store an arbitrary set of properties as variables that come from any value provider or statically provided.

    ```yaml
    interpretations:
      - type: variables
        variables:
          first_name: !jmespath first_name
          last_name: Smith
    ```

    You may also apply normalization in the same way as any other interpretation.

    ```yaml
    interpretations:
      - type: variables
        variables:
          first_name: !jmespath first_name
        normalization:
            do_lowercase_strings: true
    ```
    """

    __slots__ = ("variables", "norm_args")

    def __init__(
        self,
        variables: Dict[str, StaticValueOrValueProvider],
        normalization: Optional[Dict[str, Any]] = None,
    ):
        self.variables = PropertyMapping.from_file_data(variables)
        self.norm_args = normalization or {}

    def interpret(self, context: ProviderContext):
        self.variables.apply_to(context, context.variables, self.norm_args)
