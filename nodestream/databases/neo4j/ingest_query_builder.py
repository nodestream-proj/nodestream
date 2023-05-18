import re
from datetime import datetime, timedelta
from typing import Iterable
from functools import cache, wraps

from cymple.builder import QueryBuilder, NodeAvailable, NodeAfterMergeAvailable

from ...model import (
    Node,
    NodeIdentityShape,
    RelationshipWithNodes,
    RelationshipWithNodesIdentityShape,
    TimeToLiveConfiguration,
    MatchStrategy,
    GraphObjectType,
)
from .query import QueryBatch, Query
from ..query_executor import OperationOnNodeIdentity

PROPERTIES_PARAM_NAME = "properties"
ADDITIONAL_LABELS_PARAM_NAME = "additional_labels"
GENERIC_NODE_REF_NAME = "node"
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
        f"{GENERIC_NODE_REF_NAME}.{prop}": f"params.{generate_prefixed_param_name(prop, prefix)}"
        for prop in properties
    }


def _match_node(node_operation: OperationOnNodeIdentity) -> NodeAvailable:
    op = "=~" if node_operation.match_strategy == MatchStrategy.FUZZY else "="
    identity = node_operation.node_identity
    props = generate_where_set_with_prefix(identity.keys, GENERIC_NODE_REF_NAME)
    return (
        QueryBuilder()
        .match()
        .node(labels=identity.type, ref_name=GENERIC_NODE_REF_NAME)
        .where_multiple(props, comparison_operator=op)
    )


def _merge_node(
    node_operation: OperationOnNodeIdentity,
) -> NodeAfterMergeAvailable:
    properties = generate_properties_set_with_prefix(
        node_operation.node_identity.keys, GENERIC_NODE_REF_NAME
    )
    return (
        QueryBuilder()
        .merge()
        .node(
            labels=node_operation.node_identity.type,
            ref_name=GENERIC_NODE_REF_NAME,
            properties=properties,
        )
    )


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

        params = {
            generate_prefixed_param_name(k, GENERIC_NODE_REF_NAME): v
            for k, v in node.key_values.items()
        }
        params[
            generate_prefixed_param_name(PROPERTIES_PARAM_NAME, GENERIC_NODE_REF_NAME)
        ] = node.properties
        params[
            generate_prefixed_param_name(
                ADDITIONAL_LABELS_PARAM_NAME, GENERIC_NODE_REF_NAME
            )
        ] = node.additional_types

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
        shape: RelationshipWithNodesIdentityShape,
        rels: Iterable[RelationshipWithNodes],
    ) -> QueryBatch:
        """Generate a batch of queries to update relationships in the database in the same way of the same type."""

        pass

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

        if config.graph_type == GraphObjectType.NODE:
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
