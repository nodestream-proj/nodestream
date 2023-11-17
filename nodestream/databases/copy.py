from abc import ABC, abstractmethod
from typing import AsyncGenerator, List

from ..model import Node, RelationshipWithNodes
from ..pipeline import Extractor
from ..project import Project
from ..schema.indexes import KeyIndex


class TypeRetriever(ABC):
    @abstractmethod
    def get_nodes_of_type(self, shape: str) -> AsyncGenerator[Node, None]:
        raise NotImplementedError

    @abstractmethod
    def get_relationships_of_type(
        self, type: str
    ) -> AsyncGenerator[RelationshipWithNodes, None]:
        raise NotImplementedError


class Copier(Extractor):
    def __init__(
        self,
        type_retriver: TypeRetriever,
        project: Project,
        node_types: List[str],
        relationship_types: List[str],
    ) -> None:
        self.type_retriever = type_retriver
        self.relationship_types = relationship_types
        self.node_types = node_types
        self.keys_by_node_type = {
            key.type: key
            for key in project.gather_used_indexes()
            if isinstance(key, KeyIndex)
        }

    async def extract_records(self):
        for node_type in self.node_types:
            nodes = self.type_retriever.get_nodes_of_type(node_type)
            async for node in nodes:
                yield self.convert_node_to_ingest(node)

        for rel_type in self.relationship_types:
            rels = self.type_retriever.get_relationships_of_type(rel_type)
            async for relationship in rels:
                yield self.convert_relationship_to_ingest(relationship)

    def reorganize_node_key_properties(self, node: Node):
        # This is a bit of a hack, but it's the only way to make sure that the
        # keys are in the right place. We need to remove the keys from the
        # properties and put them in the key_values because otherwise
        # we have to push a lot of work down to the database connector to ensure
        # that the keys are in the right place. Its easier to just do it here.
        key = self.keys_by_node_type[node.type]
        for key_name in key.identity_keys:
            node.key_values[key_name] = node.properties[key_name]
            del node.properties[key_name]

    def convert_node_to_ingest(self, node: Node):
        self.reorganize_node_key_properties(node)
        return node.into_ingest()

    def convert_relationship_to_ingest(self, relationship: RelationshipWithNodes):
        self.reorganize_node_key_properties(relationship.from_node)
        self.reorganize_node_key_properties(relationship.to_node)
        return relationship.into_ingest()
