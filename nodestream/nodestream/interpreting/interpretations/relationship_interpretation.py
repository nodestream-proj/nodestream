from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, Optional, Tuple

from ...compat import deprecated_arugment
from ...model import Node, NodeCreationRule, PropertySet, Relationship
from ...pipeline.normalizers import LowercaseStrings
from ...pipeline.value_providers import (
    ProviderContext,
    StaticValueOrValueProvider,
    ValueProvider,
)
from ...schema import Cardinality, GraphObjectSchema, SchemaExpansionCoordinator
from ..record_decomposers import RecordDecomposer
from .interpretation import Interpretation
from .property_mapping import PropertyMapping, PropertyMappingFromDict
from .source_node_interpretation import SourceNodeInterpretation

DEFAULT_KEY_NORMALIZATION_ARGUMENTS = {LowercaseStrings.argument_flag(): True}


class InvalidKeyLengthError(ValueError):
    """Raised when a related nodes have differing lengths of key parts returned from a value provider.."""

    def __init__(self, district_lengths, *args: object) -> None:
        lengths = f"({','.join((str(length) for length in district_lengths))})"
        error = f"Node Relationships do not have a consistent key length. Lengths are: ({lengths}) "
        super().__init__(error, *args)


class RelatedNodeKeySearchAlgorithm(ABC):
    def __init__(
        self, key_searches: Dict[str, ValueProvider], key_normalization: Dict[str, Any]
    ) -> None:
        self.node_key = key_searches
        self.key_normalization = key_normalization

    @abstractmethod
    def get_related_node_key_sets(
        self, context: ProviderContext
    ) -> Iterable[Dict[str, Any]]:
        raise NotImplementedError


class SingleNodeKeySearchAlgorithm(RelatedNodeKeySearchAlgorithm):
    def get_related_node_key_sets(
        self, context: ProviderContext
    ) -> Iterable[Dict[str, Any]]:
        return [
            {
                k: v.normalize_single_value(context, self.key_normalization)
                for k, v in self.node_key.items()
            }
        ]


class MultiNodeKeySearchAlgorithm(RelatedNodeKeySearchAlgorithm):
    def get_related_node_key_sets(self, context: ProviderContext):
        # If we do not have the same length, then there is an error because
        # we do not have pairs to create keys for each node based off of.
        all_values_by_key_property = {
            k: tuple(v.normalize_many_values(context, self.key_normalization))
            for k, v in self.node_key.items()
        }
        distinct_lengths = {len(vals) for vals in all_values_by_key_property.values()}
        all_of_same_length = len(distinct_lengths) == 1
        if not all_of_same_length:
            raise InvalidKeyLengthError(distinct_lengths)

        # Now that we know everything has the same length, we can simply return the pairs
        # for each index in the arrays.
        common_length = next(iter(distinct_lengths))
        return [
            {k: v[i] for k, v in all_values_by_key_property.items()}
            for i in range(common_length)
        ]


class RelationshipInterpretation(Interpretation, alias="relationship"):
    """Provides a generic method by which to interpret a relationship between the source node and zero-to-many related nodes."""

    __slots__ = (
        "can_find_many",
        "cardinality",
        "outbound",
        "node_creation_rule",
        "decomposer",
        "node_type",
        "relationship_type",
        "node_key",
        "node_properties",
        "relationship_key",
        "relationship_properties",
        "key_normalization",
        "properties_normalization",
        "key_search_algorithm",
        "node_additional_types",
    )

    @deprecated_arugment("match_strategy", "node_creation_rule")
    def __init__(
        self,
        node_type: StaticValueOrValueProvider,
        relationship_type: StaticValueOrValueProvider,
        node_key: Dict[str, StaticValueOrValueProvider],
        node_properties: Optional[Dict[str, StaticValueOrValueProvider]] = None,
        relationship_key: Optional[Dict[str, StaticValueOrValueProvider]] = None,
        relationship_properties: Optional[Dict[str, StaticValueOrValueProvider]] = None,
        outbound: bool = True,
        find_many: bool = False,
        iterate_on: Optional[ValueProvider] = None,
        cardinality: str = "SINGLE",
        node_creation_rule: Optional[str] = None,
        key_normalization: Optional[Dict[str, Any]] = None,
        properties_normalization: Optional[Dict[str, Any]] = None,
        node_additional_types: Optional[Iterable[str]] = None,
    ):
        self.can_find_many = find_many or iterate_on is not None
        self.cardinality = Cardinality(cardinality)
        self.outbound = outbound
        self.node_creation_rule = NodeCreationRule(
            node_creation_rule or NodeCreationRule.EAGER.value
        )
        self.decomposer = RecordDecomposer.from_iteration_arguments(iterate_on)
        self.node_type = ValueProvider.guarantee_value_provider(node_type)
        self.relationship_type = ValueProvider.guarantee_value_provider(
            relationship_type
        )
        self.node_key = ValueProvider.guarantee_provider_dictionary(node_key)
        self.node_properties = PropertyMapping.from_file_data(node_properties or {})
        self.relationship_key = ValueProvider.guarantee_provider_dictionary(
            relationship_key or {}
        )
        self.relationship_properties = PropertyMapping.from_file_data(
            relationship_properties or {}
        )
        self.key_normalization = {
            **DEFAULT_KEY_NORMALIZATION_ARGUMENTS,
            **(key_normalization or {}),
        }
        self.properties_normalization = properties_normalization or {}

        key_search_algorithm = (
            MultiNodeKeySearchAlgorithm if find_many else SingleNodeKeySearchAlgorithm
        )
        self.key_search_algorithm = key_search_algorithm(
            self.node_key, self.key_normalization
        )
        self.node_additional_types = tuple(node_additional_types or tuple())

    def interpret(self, context: ProviderContext):
        for sub_context in self.decomposer.decompose_record(context):
            for relationship, related_node in self.find_matches(sub_context):
                context.desired_ingest.add_relationship(
                    related_node, relationship, self.outbound, self.node_creation_rule
                )

    def find_relationship(self, context: ProviderContext) -> Relationship:
        rel = Relationship(type=self.relationship_type.single_value(context))
        relationship_key_property_mapping = PropertyMappingFromDict(
            self.relationship_key
        )
        relationship_key_property_mapping.apply_to(
            context, rel.key_values, self.properties_normalization
        )
        self.relationship_properties.apply_to(
            context, rel.properties, self.properties_normalization
        )
        return rel

    def find_related_nodes(self, context: ProviderContext) -> Iterable[Node]:
        for key_set in self.key_search_algorithm.get_related_node_key_sets(context):
            node = Node(
                type=self.node_type.single_value(context),
                key_values=PropertySet(key_set),
                additional_types=self.node_additional_types,
            )
            if node.has_valid_id:
                self.node_properties.apply_to(
                    context, node.properties, self.properties_normalization
                )
                yield node

    def find_matches(
        self, context: ProviderContext
    ) -> Iterable[Tuple[Relationship, Node]]:
        relationship = self.find_relationship(context)
        for related_node in self.find_related_nodes(context):
            yield relationship, related_node

    def expand_relationship_schema(self, relationship_schema: GraphObjectSchema):
        relationship_schema.add_keys(self.relationship_key)
        relationship_schema.add_properties(self.relationship_properties)
        relationship_schema.add_indexed_timestamp()

    def expand_related_node_schema(self, node_schema: GraphObjectSchema):
        if self.node_creation_rule == NodeCreationRule.EAGER:
            node_schema.add_keys(self.node_key)
        else:
            node_schema.add_properties(self.node_key)
        node_schema.add_properties(self.node_properties)
        node_schema.add_indexed_timestamp()

    def expand_schema(self, coordinator: SchemaExpansionCoordinator):
        if self.relationship_type.is_static:
            coordinator.on_relationship_schema(
                self.expand_relationship_schema,
                relationship_type=self.relationship_type.value,
            )

        if self.node_type.is_static:
            coordinator.on_node_schema(
                self.expand_related_node_schema,
                node_type=self.node_type.value,
            )

        if self.relationship_type.is_static and self.node_type.is_static:
            source_node = SourceNodeInterpretation.SOURCE_NODE_TYPE_ALIAS
            related_node = self.node_type.value
            from_type = source_node if self.outbound else related_node
            to_type = related_node if self.outbound else source_node
            foreign_cardinality = Cardinality.MANY
            source_cardinality = (
                Cardinality.MANY if self.can_find_many else self.cardinality
            )
            if self.outbound:
                to_side_cardinality, from_side_cardinality = (
                    foreign_cardinality,
                    source_cardinality,
                )
            else:
                from_side_cardinality, to_side_cardinality = (
                    foreign_cardinality,
                    source_cardinality,
                )
            coordinator.connect(
                from_type_or_alias=from_type,
                to_type_or_alias=to_type,
                relationship_type=self.relationship_type.value,
                to_cardinality=to_side_cardinality,
                from_cardinality=from_side_cardinality,
            )
