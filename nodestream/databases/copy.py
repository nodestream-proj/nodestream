import logging
from abc import ABC, abstractmethod
from typing import AsyncGenerator, List

from ..model import Node, RelationshipWithNodes
from ..pipeline import Extractor
from ..schema import Adjacency, Schema


class TypeRetriever(ABC):
    @abstractmethod
    def get_nodes_of_type(self, node_type: str) -> AsyncGenerator[Node, None]:
        raise NotImplementedError

    @abstractmethod
    def get_relationships_of_type_between(
        self, from_node_type: str, to_node_type: str, relationship_type: str
    ) -> AsyncGenerator[RelationshipWithNodes, None]:
        raise NotImplementedError


class Copier(Extractor):
    def __init__(
        self,
        type_retriver: TypeRetriever,
        schema: Schema,
        node_types_to_copy: List[str],
        relationship_types_to_copy: List[str],
    ) -> None:
        self.type_retriever = type_retriver
        self.relationship_types = relationship_types_to_copy
        self.node_types = node_types_to_copy
        self.schema = schema
        self.logger = logging.getLogger(__name__)
        self.logger.info(
            f"Copying {len(self.node_types)} node types and {len(self.relationship_types)} relationship types"
        )

    async def extract_records(self):
        for node_type in self.node_types:
            self.logger.info(f"Copying nodes of type {node_type}")
            async for node in self.type_retriever.get_nodes_of_type(node_type):
                yield self.convert_node_to_ingest(node)

        for rel_type in self.relationship_types:
            # Prefer schema-driven adjacency expansion when available so we can
            # fully specify from/to node types for the retriever. Note that it
            # is impossible to have a relationship without an adjacency, we only
            # support copyting from a nodestream schema.
            self.logger.info(f"Copying relationships of type {rel_type}")
            adjacencies: List[Adjacency] = list(
                self.schema.get_adjacencies_by_relationship_type(rel_type)
            )

            for adjacency in adjacencies:
                async for (
                    relationship
                ) in self.type_retriever.get_relationships_of_type_between(
                    adjacency.from_node_type,
                    adjacency.to_node_type,
                    adjacency.relationship_type,
                ):
                    yield self.convert_relationship_to_ingest(relationship)

    def reorganize_node_key_properties(self, node: Node):
        # This is a bit of a hack, but it's the only way to make sure that the
        # keys are in the right place. We need to remove the keys from the
        # properties and put them in the key_values because otherwise
        # we have to push a lot of work down to the database connector to ensure
        # that the keys are in the right place. Its easier to just do it here.
        type_def = self.schema.get_node_type_by_name(node.type)
        if type_def is None:
            return
        for key_name in type_def.keys:
            node.key_values[key_name] = node.properties[key_name]
            del node.properties[key_name]

    def convert_node_to_ingest(self, node: Node):
        self.reorganize_node_key_properties(node)
        return node.into_ingest()

    def convert_relationship_to_ingest(self, relationship: RelationshipWithNodes):
        self.reorganize_node_key_properties(relationship.from_node)
        self.reorganize_node_key_properties(relationship.to_node)
        return relationship.into_ingest()
