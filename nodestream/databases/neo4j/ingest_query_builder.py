import re
from datetime import datetime, timedelta
from functools import cache, wraps
from typing import Iterable

from cymple.builder import NodeAfterMergeAvailable, NodeAvailable, QueryBuilder

from ...model import (
    MatchStrategy,
    Node,
    Relationship,
    RelationshipIdentityShape,
    RelationshipWithNodes,
    TimeToLiveConfiguration,
)
from ...schema.schema import GraphObjectType
from ..query_executor import OperationOnNodeIdentity, OperationOnRelationshipIdentity
from .query import Query, QueryBatch

PROPERTIES_PARAM_NAME = "properties"
ADDITIONAL_LABELS_PARAM_NAME = "additional_labels"
GENERIC_NODE_REF_NAME = "node"
FROM_NODE_REF_NAME = "from_node"
TO_NODE_REF_NAME = "to_node"
RELATIONSHIP_REF_NAME = "rel"
PARAMETER_CORRECTION_REGEX = re.compile(r"\"(params.__\w+)\"")


def correct_parameters(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        query = f(*args, **kwargs)
        return PARAMETER_CORRECTION_REGEX.sub(r"\1", query)

    return wrapper


def generate_prefixed_param_name(property_name: str, prefix: str) -> str:
    return f"__{prefix}_{property_name}"


def generate_properties_set_with_prefix(properties: Iterable[str], prefix: str):
    return {
        prop: f"params.{generate_prefixed_param_name(prop, prefix)}"
        for prop in properties
    }


def generate_where_set_with_prefix(properties: frozenset, prefix: str):
    return {
        f"{prefix}.{prop}": f"params.{generate_prefixed_param_name(prop, prefix)}"
        for prop in properties
    }


@cache
def _match_node(
    node_operation: OperationOnNodeIdentity, name=GENERIC_NODE_REF_NAME
) -> NodeAvailable:
    op = "=~" if node_operation.match_strategy == MatchStrategy.FUZZY else "="
    identity = node_operation.node_identity
    props = generate_where_set_with_prefix(identity.keys, name)
    return (
        QueryBuilder()
        .match()
        .node(labels=identity.type, ref_name=name)
        .where_multiple(props, comparison_operator=op)
    )


@cache
def _merge_node(
    node_operation: OperationOnNodeIdentity, name=GENERIC_NODE_REF_NAME
) -> NodeAfterMergeAvailable:
    properties = generate_properties_set_with_prefix(
        node_operation.node_identity.keys, name
    )
    return (
        QueryBuilder()
        .merge()
        .node(
            labels=node_operation.node_identity.type,
            ref_name=name,
            properties=properties,
        )
    )


@cache
def _merge_relationship(rel_identity: RelationshipIdentityShape):
    keys = generate_properties_set_with_prefix(rel_identity.keys, RELATIONSHIP_REF_NAME)
    match_rel_query = (
        QueryBuilder()
        .match_optional()
        .node(ref_name=FROM_NODE_REF_NAME)
        .related_to(
            ref_name=RELATIONSHIP_REF_NAME,
            properties=keys,
            label=rel_identity.type,
        )
        .node(ref_name=TO_NODE_REF_NAME)
    )
    create_rel_query = str(match_rel_query).replace("OPTIONAL MATCH ", "CREATE")
    set_properties_query = f"SET {RELATIONSHIP_REF_NAME} += params.{generate_prefixed_param_name(PROPERTIES_PARAM_NAME, RELATIONSHIP_REF_NAME)}"

    return f"""
    {match_rel_query}
    FOREACH (x IN CASE WHEN {RELATIONSHIP_REF_NAME} IS NULL THEN [1] ELSE [] END |
        {create_rel_query} {set_properties_query})
    FOREACH (i in CASE WHEN {RELATIONSHIP_REF_NAME} IS NOT NULL THEN [1] ELSE [] END |
        {set_properties_query})
    """


class Neo4jIngestQueryBuilder:
    @cache
    @correct_parameters
    def generate_update_node_operation_query_statement(
        self,
        operation: OperationOnNodeIdentity,
    ) -> str:
        """Generate a query to update a node in the database given a node type and a match strategy."""

        if operation.match_strategy == MatchStrategy.EAGER:
            query = str(_merge_node(operation))
        else:
            query = str(_match_node(operation))

        result = f"{query} SET {GENERIC_NODE_REF_NAME} += params.{generate_prefixed_param_name(PROPERTIES_PARAM_NAME, GENERIC_NODE_REF_NAME)}"
        if operation.node_identity.additional_types:
            result += f" WITH {GENERIC_NODE_REF_NAME}, params CALL apoc.create.addLabels({GENERIC_NODE_REF_NAME}, params.{generate_prefixed_param_name(ADDITIONAL_LABELS_PARAM_NAME, GENERIC_NODE_REF_NAME)}) yield node RETURN true"
        return result

    def generate_update_node_operation_params(self, node: Node) -> dict:
        """Generate the parameters for a query to update a node in the database."""

        params = self.generate_node_key_params(node)
        params[
            generate_prefixed_param_name(PROPERTIES_PARAM_NAME, GENERIC_NODE_REF_NAME)
        ] = node.properties
        params[
            generate_prefixed_param_name(
                ADDITIONAL_LABELS_PARAM_NAME, GENERIC_NODE_REF_NAME
            )
        ] = node.additional_types

        return params

    def generate_node_key_params(self, node: Node, name=GENERIC_NODE_REF_NAME) -> dict:
        """Generate the parameters for a query to update a node in the database."""

        return {
            generate_prefixed_param_name(k, name): v for k, v in node.key_values.items()
        }

    @cache
    @correct_parameters
    def generate_update_relationship_operation_query_statement(
        self,
        operation: OperationOnRelationshipIdentity,
    ) -> str:
        """Generate a query to update a relationship in the database given a relationship operation."""

        match_from_node_segment = _match_node(operation.from_node, FROM_NODE_REF_NAME)
        match_to_node_segment = _match_node(operation.to_node, TO_NODE_REF_NAME)
        merge_rel_segment = _merge_relationship(operation.relationship_identity)
        return f"{match_from_node_segment} {match_to_node_segment} {merge_rel_segment}"

    def generate_update_rel_params(self, rel: Relationship) -> dict:
        """Generate the parameters for a query to update a relationship in the database."""

        params = {
            generate_prefixed_param_name(k, RELATIONSHIP_REF_NAME): v
            for k, v in rel.key_values.items()
        }
        params[
            generate_prefixed_param_name(PROPERTIES_PARAM_NAME, RELATIONSHIP_REF_NAME)
        ] = rel.properties

        return params

    def generate_update_rel_between_nodes_params(
        self, rel: RelationshipWithNodes
    ) -> dict:
        """Generate the parameters for a query to update a relationship in the database."""

        params = self.generate_update_rel_params(rel.relationship)
        params.update(self.generate_node_key_params(rel.from_node, FROM_NODE_REF_NAME))
        params.update(self.generate_node_key_params(rel.to_node, TO_NODE_REF_NAME))
        return params

    def generate_batch_update_node_operation_batch(
        self,
        operation: OperationOnNodeIdentity,
        nodes: Iterable[Node],
    ) -> QueryBatch:
        """Generate a batch of queries to update nodes in the database in the same way of the same type."""

        query_statement = self.generate_update_node_operation_query_statement(operation)
        params = [self.generate_update_node_operation_params(node) for node in nodes]
        return QueryBatch(query_statement, params)

    def generate_batch_update_relationship_query_batch(
        self,
        operation: OperationOnRelationshipIdentity,
        relationships: Iterable[RelationshipWithNodes],
    ) -> QueryBatch:
        """Generate a batch of queries to update relationships in the database in the same way of the same type."""

        query = self.generate_update_relationship_operation_query_statement(operation)
        params = [
            self.generate_update_rel_between_nodes_params(rel) for rel in relationships
        ]
        return QueryBatch(query, params)

    def generate_ttl_query_from_configuration(
        self, config: TimeToLiveConfiguration
    ) -> Query:
        earliest_allowed_time = datetime.utcnow() - timedelta(
            hours=config.expiry_in_hours
        )
        params = {"earliest_allowed_time": earliest_allowed_time}
        if config.custom_query is not None:
            return Query(config.custom_query, params)

        query_builder = QueryBuilder()
        ref_name = "x"

        if config.graph_object_type == GraphObjectType.NODE:
            query_builder = query_builder.match().node(
                labels=config.object_type, ref_name=ref_name
            )
        else:
            query_builder = (
                query_builder.match()
                .node()
                .related_to(label=config.object_type, ref_name=ref_name)
                .node()
            )

        query_builder = query_builder.where_literal(
            f"{ref_name}.last_ingested_at <= $earliest_allowed_time"
        ).return_literal(f"id({ref_name}) as id")

        return Query(str(query_builder), params)
