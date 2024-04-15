from dataclasses import dataclass
from logging import getLogger

from nodestream.model import Node, NodeCreationRule

LOGGER = getLogger(__name__)


@dataclass
class IngestNode:
    node: Node
    creation_rule: NodeCreationRule
