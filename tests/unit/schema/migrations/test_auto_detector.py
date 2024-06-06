import random
import string
from abc import ABC, abstractmethod
from copy import deepcopy
from itertools import combinations
from typing import Iterable, Type

import pytest
from hamcrest import assert_that, equal_to, has_items

from nodestream.schema.migrations.auto_change_detector import (
    AutoChangeDetector,
    MigratorInput,
)
from nodestream.schema.migrations.operations import (
    AddAdditionalNodePropertyIndex,
    AddAdditionalRelationshipPropertyIndex,
    AddNodeProperty,
    AddRelationshipProperty,
    CreateNodeType,
    CreateRelationshipType,
    DropAdditionalNodePropertyIndex,
    DropAdditionalRelationshipPropertyIndex,
    DropNodeProperty,
    DropNodeType,
    DropRelationshipProperty,
    DropRelationshipType,
    NodeKeyExtended,
    NodeKeyPartRenamed,
    Operation,
    RelationshipKeyExtended,
    RelationshipKeyPartRenamed,
    RenameNodeProperty,
    RenameNodeType,
    RenameRelationshipProperty,
    RenameRelationshipType,
)
from nodestream.schema.migrations.state_providers import (
    InMemoryMigrator,
    StaticStateProvider,
)

ALPHABET = string.ascii_lowercase + string.ascii_uppercase + string.digits


def test_migrator_input_ask_yes_no(mocker):
    input = MigratorInput()
    assert_that(input.ask_yes_no("Hello, world!"), equal_to(False))


def test_migrator_input_ask_property_renamed_asks_correct_question(mocker):
    input = MigratorInput()
    input.ask_yes_no = mocker.Mock(return_value=True)
    assert_that(
        input.ask_property_renamed("object_type", "old_property", "new_property"),
        equal_to(True),
    )
    input.ask_yes_no.assert_called_once_with(
        "Did you rename old_property to new_property on object_type?"
    )


def test_migrator_input_ask_type_renamed_asks_correct_question(mocker):
    input = MigratorInput()
    input.ask_yes_no = mocker.Mock(return_value=True)
    assert_that(
        input.ask_type_renamed("old_type", "new_type"),
        equal_to(True),
    )
    input.ask_yes_no.assert_called_once_with("Did you rename old_type to new_type?")


class ScenarioMigratorInput(MigratorInput):
    """A migrator input that integrates with the test framework.

    This migrator input allows the test framework to explicitly specify the
    answers to the questions that the schema change detector asks. This allows
    the test framework to test the schema change detector in a deterministic
    way.
    """

    def __init__(self) -> None:
        self._property_renames = set()
        self._type_renames = set()

    def renamed_type(self, old_name: str, new_name: str) -> None:
        self._type_renames.add((old_name, new_name))

    def renamed_property(self, type, old_name: str, new_name: str) -> None:
        self._property_renames.add((type, old_name, new_name))

    def ask_type_renamed(self, old_type: str, new_type: str) -> bool:
        return (old_type, new_type) in self._type_renames

    def ask_property_renamed(
        self, object_type: str, old_property_name: str, new_property_name: str
    ) -> bool:
        return (
            object_type,
            old_property_name,
            new_property_name,
        ) in self._property_renames


class Scenario(ABC):
    """A scenario for testing the auto change detector.

    Each scenario represents a type of change that can be made to the schema.
    (e.g. adding a node type, dropping a relationship type, etc.) Each scenario
    roughly corresponds to a single operation in the schema change detector.

    There are three methods that can be implemented to define a scenario:

    - get_intial_state_operations: Returns the operations that should be
        executed to prepare the initial state of the schema. This method should
        be implemented by scenarios that require an initial state to be set up.
        By default, this method returns an empty list.

    - get_change_operations: Returns the operations that should be executed to
        make the change to the schema that we want to detect. This method
        should be implemented by all scenarios.

    - get_expected_detections: Returns the operations that should be detected
        by the auto change detector. This method does not need to be
        implemented by all scenarios. By default, this method returns the
        same operations as get_change_operations.

    If the scenario requires answers to questions from the schema change
    detector, then the scenario should call the renamed_type or
    renamed_property methods on the migrator input. This will allow the
    scenario to specify the answers to the questions that the schema change
    detector asks.

    If the scenario can be permuted with other scenarios, then it should be
    added to the ALL_PERMUTABLE_SCENARIOS list. The test_auto_change_detections
    test will then permute all of the scenarios in this list and test the
    permutations.
    """

    def _make_name(self) -> str:
        return "".join(random.choices(ALPHABET, k=10))

    def get_name(self, alias: str) -> str:
        if alias in self._name_cache:
            return self._name_cache[alias]
        name = self._make_name()
        self._name_cache[alias] = name
        return name

    def __init__(
        self, migrator: InMemoryMigrator, input: ScenarioMigratorInput
    ) -> None:
        self.migrator = migrator
        self.input = input
        self._name_cache = {}

    async def prepare_initial_state(self):
        for operation in self.get_intial_state_operations():
            await self.migrator.execute_operation(operation)

    async def make_schema_changes(self):
        for operation in self.get_change_operations():
            await self.migrator.execute_operation(operation)

    def compare_to_expected_detections(self, detections: Iterable[Operation]):
        expected_detections = list(self.get_expected_detections())
        assert_that(detections, has_items(*expected_detections))

    def get_intial_state_operations(self) -> Iterable[Operation]:
        return []

    @abstractmethod
    def get_change_operations(self) -> Iterable[Operation]:
        pass

    def get_expected_detections(self) -> Iterable[Operation]:
        return self.get_change_operations()


class AddedNodeType(Scenario):
    """Tests that the detector detects a new node type."""

    def get_change_operations(self) -> Iterable[Operation]:
        yield CreateNodeType(
            name=self.get_name("node_type"),
            properties={self.get_name("property")},
            keys={self.get_name("key")},
        )


class DroppedNodeType(Scenario):
    """Tests that the detector detects a dropped node type."""

    def get_intial_state_operations(self) -> Iterable[Operation]:
        yield CreateNodeType(
            name=self.get_name("node_type"),
            properties={self.get_name("property")},
            keys={self.get_name("key")},
        )

    def get_change_operations(self) -> Iterable[Operation]:
        yield DropNodeType(name=self.get_name("node_type"))


class AddedRelationshipType(Scenario):
    """Tests that the detector detects a new relationship type."""

    def get_change_operations(self) -> Iterable[Operation]:
        yield CreateRelationshipType(
            name=self.get_name("relationship_type"),
            properties={self.get_name("property")},
            keys={self.get_name("key")},
        )


class DroppedRelationshipType(Scenario):
    """Tests that the detector detects a dropped relationship type."""

    def get_intial_state_operations(self) -> Iterable[Operation]:
        yield CreateRelationshipType(
            name=self.get_name("relationship_type"),
            properties={self.get_name("property")},
            keys={self.get_name("key")},
        )

    def get_change_operations(self) -> Iterable[Operation]:
        yield DropRelationshipType(name=self.get_name("relationship_type"))


class AddedNodeProperty(Scenario):
    """Tests that the detector detects a new node property."""

    def get_intial_state_operations(self) -> Iterable[Operation]:
        yield CreateNodeType(
            name=self.get_name("node_type"),
            properties={self.get_name("property")},
            keys={self.get_name("key")},
        )

    def get_change_operations(self) -> Iterable[Operation]:
        yield AddNodeProperty(
            node_type=self.get_name("node_type"),
            property_name=self.get_name("new_property"),
        )


class DroppedNodeProperty(Scenario):
    """Tests that the detector detects a dropped node property."""

    def get_intial_state_operations(self) -> Iterable[Operation]:
        yield CreateNodeType(
            name=self.get_name("node_type"),
            properties={self.get_name("property")},
            keys={self.get_name("key")},
        )

    def get_change_operations(self) -> Iterable[Operation]:
        yield DropNodeProperty(
            node_type=self.get_name("node_type"),
            property_name=self.get_name("property"),
        )


class AddedRelationshipProperty(Scenario):
    """Tests that the detector detects a new relationship property."""

    def get_intial_state_operations(self) -> Iterable[Operation]:
        yield CreateRelationshipType(
            name=self.get_name("relationship_type"),
            properties={self.get_name("property")},
            keys={self.get_name("key")},
        )

    def get_change_operations(self) -> Iterable[Operation]:
        yield AddRelationshipProperty(
            relationship_type=self.get_name("relationship_type"),
            property_name=self.get_name("new_property"),
        )


class DroppedRelationshipProperty(Scenario):
    """Tests that the detector detects a dropped relationship property."""

    def get_intial_state_operations(self) -> Iterable[Operation]:
        yield CreateRelationshipType(
            name=self.get_name("relationship_type"),
            properties={self.get_name("property")},
            keys={self.get_name("key")},
        )

    def get_change_operations(self) -> Iterable[Operation]:
        yield DropRelationshipProperty(
            relationship_type=self.get_name("relationship_type"),
            property_name=self.get_name("property"),
        )


class RenamedNodeProperty(Scenario):
    """Tests that the detector detects a renamed node property."""

    def get_intial_state_operations(self) -> Iterable[Operation]:
        yield CreateNodeType(
            name=self.get_name("node_type"),
            properties={self.get_name("old_property")},
            keys={self.get_name("key")},
        )

    def get_change_operations(self) -> Iterable[Operation]:
        type = self.get_name("node_type")
        old_property = self.get_name("old_property")
        new_property = self.get_name("new_property")
        yield RenameNodeProperty(
            node_type=type,
            old_property_name=old_property,
            new_property_name=new_property,
        )
        self.input.renamed_property(type, old_property, new_property)


class RenamedRelationshipProperty(Scenario):
    """Tests that the detector detects a renamed relationship property."""

    def get_intial_state_operations(self) -> Iterable[Operation]:
        yield CreateRelationshipType(
            name=self.get_name("relationship_type"),
            properties={self.get_name("old_property")},
            keys={self.get_name("key")},
        )

    def get_change_operations(self) -> Iterable[Operation]:
        type = self.get_name("relationship_type")
        old_property = self.get_name("old_property")
        new_property = self.get_name("new_property")
        yield RenameRelationshipProperty(
            relationship_type=type,
            old_property_name=old_property,
            new_property_name=new_property,
        )
        self.input.renamed_property(type, old_property, new_property)


class RenamedNodeType(Scenario):
    """Tests that the detector detects a renamed node type."""

    def get_intial_state_operations(self) -> Iterable[Operation]:
        yield CreateNodeType(
            name=self.get_name("old_type"),
            properties={self.get_name("property")},
            keys={self.get_name("key")},
        )

    def get_change_operations(self) -> Iterable[Operation]:
        old_type = self.get_name("old_type")
        new_type = self.get_name("new_type")
        yield RenameNodeType(old_type=old_type, new_type=new_type)
        self.input.renamed_type(old_type, new_type)


class RenamedRelationshipType(Scenario):
    """Tests that the detector detects a renamed relationship type."""

    def get_intial_state_operations(self) -> Iterable[Operation]:
        yield CreateRelationshipType(
            name=self.get_name("old_type"),
            properties={self.get_name("property")},
            keys={self.get_name("key")},
        )

    def get_change_operations(self) -> Iterable[Operation]:
        old_type = self.get_name("old_type")
        new_type = self.get_name("new_type")
        yield RenameRelationshipType(
            old_type=old_type,
            new_type=new_type,
        )
        self.input.renamed_type(old_type, new_type)


class ExtendedNodeKey(Scenario):
    """Tests that the detector detects an extended node key."""

    def get_intial_state_operations(self) -> Iterable[Operation]:
        yield CreateNodeType(
            name=self.get_name("node_type"),
            properties={self.get_name("property")},
            keys={self.get_name("key")},
        )

    def get_change_operations(self) -> Iterable[Operation]:
        yield NodeKeyExtended(
            node_type=self.get_name("node_type"),
            added_key_property=self.get_name("new_property"),
            default=None,
        )


class ExtendedRelationshipKey(Scenario):
    """Tests that the detector detects an extended relationship key."""

    def get_intial_state_operations(self) -> Iterable[Operation]:
        yield CreateRelationshipType(
            name=self.get_name("relationship_type"),
            properties={self.get_name("property")},
            keys={self.get_name("key")},
        )

    def get_change_operations(self) -> Iterable[Operation]:
        yield RelationshipKeyExtended(
            relationship_type=self.get_name("relationship_type"),
            added_key_property=self.get_name("new_property"),
            default=None,
        )


class RenamedNodeKeyPart(Scenario):
    """Tests that the detector detects a renamed node key part."""

    def get_intial_state_operations(self) -> Iterable[Operation]:
        yield CreateNodeType(
            name=self.get_name("node_type"),
            properties={self.get_name("property")},
            keys={self.get_name("old_key")},
        )

    def get_change_operations(self) -> Iterable[Operation]:
        type = self.get_name("node_type")
        old_key = self.get_name("old_key")
        new_key = self.get_name("new_key")
        yield NodeKeyPartRenamed(
            node_type=type,
            old_key_part_name=old_key,
            new_key_part_name=new_key,
        )
        self.input.renamed_property(type, old_key, new_key)


class RenamedRelationshipKeyPart(Scenario):
    """Tests that the etector detects a renamed relationship key."""

    def get_intial_state_operations(self) -> Iterable[Operation]:
        yield CreateRelationshipType(
            name=self.get_name("relationship_type"),
            properties={self.get_name("property")},
            keys={self.get_name("old_key")},
        )

    def get_change_operations(self) -> Iterable[Operation]:
        type = self.get_name("relationship_type")
        old_key = self.get_name("old_key")
        new_key = self.get_name("new_key")
        yield RelationshipKeyPartRenamed(
            relationship_type=type,
            old_key_part_name=old_key,
            new_key_part_name=new_key,
        )
        self.input.renamed_property(type, old_key, new_key)


class AddedNodePropertyIndex(Scenario):
    """Tests that the detector detects an added node property index."""

    def get_intial_state_operations(self) -> Iterable[Operation]:
        yield CreateNodeType(
            name=self.get_name("node_type"),
            properties={self.get_name("property")},
            keys={self.get_name("key")},
        )

    def get_change_operations(self) -> Iterable[Operation]:
        yield AddAdditionalNodePropertyIndex(
            node_type=self.get_name("node_type"),
            field_name=self.get_name("property"),
        )


class AddedRelationshipPropertyIndex(Scenario):
    """Tests that the detector detects an added relationship property index."""

    def get_intial_state_operations(self) -> Iterable[Operation]:
        yield CreateRelationshipType(
            name=self.get_name("relationship_type"),
            properties={self.get_name("property")},
            keys={self.get_name("key")},
        )

    def get_change_operations(self) -> Iterable[Operation]:
        yield AddAdditionalRelationshipPropertyIndex(
            relationship_type=self.get_name("relationship_type"),
            field_name=self.get_name("property"),
        )


class DroppedNodePropertyIndex(Scenario):
    """Tests that the detector detects a dropped node index."""

    def get_intial_state_operations(self) -> Iterable[Operation]:
        yield CreateNodeType(
            name=self.get_name("node_type"),
            properties={self.get_name("property")},
            keys={self.get_name("key")},
        )
        yield AddAdditionalNodePropertyIndex(
            node_type=self.get_name("node_type"),
            field_name=self.get_name("property"),
        )

    def get_change_operations(self) -> Iterable[Operation]:
        yield DropAdditionalNodePropertyIndex(
            node_type=self.get_name("node_type"),
            field_name=self.get_name("property"),
        )


class DroppedRelationshipPropertyIndex(Scenario):
    """Tests that the detector detects a dropped relationship index."""

    def get_intial_state_operations(self) -> Iterable[Operation]:
        yield CreateRelationshipType(
            name=self.get_name("relationship_type"),
            properties={self.get_name("property")},
            keys={self.get_name("key")},
        )
        yield AddAdditionalRelationshipPropertyIndex(
            relationship_type=self.get_name("relationship_type"),
            field_name=self.get_name("property"),
        )

    def get_change_operations(self) -> Iterable[Operation]:
        yield DropAdditionalRelationshipPropertyIndex(
            relationship_type=self.get_name("relationship_type"),
            field_name=self.get_name("property"),
        )


class MoveNodePropertyToKey(Scenario):
    # If a property is moved to a key, then the detector should detect that the
    # key was extended only. It should not detect that the property was dropped
    # as it was only moved to the key.

    def get_intial_state_operations(self) -> Iterable[Operation]:
        yield CreateNodeType(
            name=self.get_name("node_type"),
            properties={self.get_name("property")},
            keys={self.get_name("key")},
        )

    def get_change_operations(self) -> Iterable[Operation]:
        yield DropNodeProperty(
            node_type=self.get_name("node_type"),
            property_name=self.get_name("property"),
        )
        yield NodeKeyExtended(
            node_type=self.get_name("node_type"),
            added_key_property=self.get_name("property"),
            default=None,
        )

    def get_expected_detections(self) -> Iterable[Operation]:
        yield NodeKeyExtended(
            node_type=self.get_name("node_type"),
            added_key_property=self.get_name("property"),
            default=None,
        )


class MoveRelationshipPropertyToKey(Scenario):
    # If a property is moved to a key, then the detector should detect that the
    # key was extended only. It should not detect that the property was dropped
    # as it was only moved to the key.

    def get_intial_state_operations(self) -> Iterable[Operation]:
        yield CreateRelationshipType(
            name=self.get_name("relationship_type"),
            properties={self.get_name("property")},
            keys={self.get_name("key")},
        )

    def get_change_operations(self) -> Iterable[Operation]:
        yield DropRelationshipProperty(
            relationship_type=self.get_name("relationship_type"),
            property_name=self.get_name("property"),
        )
        yield RelationshipKeyExtended(
            relationship_type=self.get_name("relationship_type"),
            added_key_property=self.get_name("property"),
            default=None,
        )

    def get_expected_detections(self) -> Iterable[Operation]:
        yield RelationshipKeyExtended(
            relationship_type=self.get_name("relationship_type"),
            added_key_property=self.get_name("property"),
            default=None,
        )


ALL_PERMUTABLE_SCENARIOS = [
    AddedNodeType,
    DroppedNodeType,
    AddedRelationshipType,
    DroppedRelationshipType,
    AddedNodeProperty,
    DroppedNodeProperty,
    AddedRelationshipProperty,
    DroppedRelationshipProperty,
    RenamedNodeProperty,
    RenamedRelationshipProperty,
    RenamedNodeType,
    RenamedRelationshipType,
    AddedNodePropertyIndex,
    DroppedNodePropertyIndex,
    AddedRelationshipPropertyIndex,
    DroppedRelationshipPropertyIndex,
    ExtendedNodeKey,
    ExtendedRelationshipKey,
    RenamedNodeKeyPart,
    RenamedRelationshipKeyPart,
    MoveNodePropertyToKey,
    MoveRelationshipPropertyToKey,
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario_permutation", tuple(combinations(ALL_PERMUTABLE_SCENARIOS, 3))
)
async def test_auto_change_detections_with_permutations(
    scenario_permutation: Iterable[Type[Scenario]],
):
    # Prepare the intial state of the schema.
    memory_migrator = InMemoryMigrator()
    input = ScenarioMigratorInput()
    scenario_instances = [
        scenario(memory_migrator, input) for scenario in scenario_permutation
    ]
    for scenario in scenario_instances:
        await scenario.prepare_initial_state()
    from_state = StaticStateProvider(deepcopy(memory_migrator.schema))

    # Make the changes to the schema that we want to detect.
    for scenario in scenario_instances:
        await scenario.make_schema_changes()

    # Detect the changes.
    to_state = StaticStateProvider(memory_migrator.schema)
    detector = AutoChangeDetector(input, from_state, to_state)
    detections = await detector.detect_changes()

    # Compare the detected changes to the expected changes.
    for scenario in scenario_instances:
        scenario.compare_to_expected_detections(detections)
