from typing import Iterable

import pytest
from hamcrest import assert_that, equal_to, is_, none, same_instance

from nodestream.schema.schema import (
    AggregatedIntrospectiveIngestionComponent,
    Cardinality,
    GraphObjectShape,
    GraphObjectType,
    GraphSchema,
    GraphSchemaOverrides,
    IntrospectiveIngestionComponent,
    KnownTypeMarker,
    PresentRelationship,
    PropertyMetadata,
    PropertyMetadataSet,
    PropertyOverride,
    PropertyOverrides,
    PropertyType,
    UnknownTypeMarker,
)


@pytest.fixture
def simple_source_node_shape():
    return GraphObjectShape(
        GraphObjectType.NODE,
        KnownTypeMarker.fulfilling_source_node("Test"),
        PropertyMetadataSet({}),
    )


@pytest.fixture
def node_with_no_alias():
    return GraphObjectShape(
        GraphObjectType.NODE,
        KnownTypeMarker("Test"),
        PropertyMetadataSet({}),
    )


@pytest.fixture
def uknown_source_node_shape():
    return GraphObjectShape(
        GraphObjectType.NODE,
        UnknownTypeMarker.source_node(),
        PropertyMetadataSet({}),
    )


def test_uknown_type_marker_resolve_type_finds_matching_alias_when_present(
    simple_source_node_shape,
):
    subject = UnknownTypeMarker.source_node()
    result = subject.resolve_type([simple_source_node_shape])
    assert_that(result, equal_to(simple_source_node_shape.object_type))


def test_uknown_type_marker_resolve_type_returns_none_when_no_matching_alias(
    node_with_no_alias,
):
    subject = UnknownTypeMarker.source_node()
    assert_that(subject.resolve_type([node_with_no_alias]), is_(none()))


def test_uknkown_type_marker_fulls_aliases_returns_false():
    subject = UnknownTypeMarker.source_node()
    assert_that(subject.fulfills_alias(subject.alias), is_(False))


def test_known_type_marker_resolve_type_returns_self():
    subject = KnownTypeMarker("Test")
    assert_that(subject.resolve_type([]), same_instance(subject))


def test_property_metadata_set_update():
    subject = PropertyMetadataSet({"foo": "bar", "baz": "qux"})
    subject.update(PropertyMetadataSet({"foo": "baz"}))
    assert_that(subject, equal_to(PropertyMetadataSet({"foo": "baz", "baz": "qux"})))


@pytest.mark.parametrize(
    "left_graph_object_shape,right_graph_object_shape,left_object_type,right_object_type,expected_result",
    [
        (GraphObjectType.NODE, GraphObjectType.NODE, "Test", "Test", True),
        (GraphObjectType.NODE, GraphObjectType.NODE, "Test", "Other", False),
        (GraphObjectType.NODE, GraphObjectType.RELATIONSHIP, "Test", "Test", False),
        (GraphObjectType.RELATIONSHIP, GraphObjectType.NODE, "Test", "Test", False),
        (
            GraphObjectType.RELATIONSHIP,
            GraphObjectType.RELATIONSHIP,
            "Test",
            "Test",
            True,
        ),
        (
            GraphObjectType.RELATIONSHIP,
            GraphObjectType.RELATIONSHIP,
            "Test",
            "Other",
            False,
        ),
    ],
)
def test_graph_object_shape_overlaps_with_returns_true_when_types_match(
    left_graph_object_shape,
    right_graph_object_shape,
    left_object_type,
    right_object_type,
    expected_result,
):
    left = GraphObjectShape(left_graph_object_shape, left_object_type, {})
    right = GraphObjectShape(right_graph_object_shape, right_object_type, {})
    assert_that(left.overlaps_with(right), equal_to(expected_result))


def test_graph_object_shape_include(simple_source_node_shape):
    other = GraphObjectShape(
        GraphObjectType.NODE,
        KnownTypeMarker.fulfilling_source_node("Test"),
        PropertyMetadataSet({"foo": PropertyMetadata("test_property")}),
    )
    simple_source_node_shape.include(other)
    assert_that(simple_source_node_shape.properties, equal_to(other.properties))


def test_graph_object_shape_include_errors_when_shapes_do_not_overlap():
    left = GraphObjectShape(GraphObjectType.NODE, "Test", {})
    right = GraphObjectShape(GraphObjectType.RELATIONSHIP, "Test", {})
    with pytest.raises(ValueError):
        left.include(right)


def test_graph_object_shape_resolve_types_sets_type_when_unknown_type_marker(
    uknown_source_node_shape,
    simple_source_node_shape,
):
    uknown_source_node_shape.resolve_types([simple_source_node_shape])
    assert_that(
        uknown_source_node_shape.object_type,
        equal_to(simple_source_node_shape.object_type),
    )


def test_graph_object_shape_resolve_types_unchaged_when_no_matching_type(
    uknown_source_node_shape,
):
    uknown_source_node_shape.resolve_types([])
    assert_that(
        uknown_source_node_shape.object_type, equal_to(UnknownTypeMarker.source_node())
    )


@pytest.mark.parametrize(
    "left_from_object_type,left_to_object_type,left_relationship_object_type,righ_from_object_type,right_to_object_type,right_relationship_object_type,expected_result",
    [
        ("To", "Rel", "From", "To", "Rel", "From", True),
        ("To", "Rel", "From", "To", "Rel", "Other", False),
        ("To", "Rel", "From", "To", "Other", "From", False),
        ("To", "Rel", "From", "Other", "Rel", "From", False),
    ],
)
def test_present_relationship_overlaps_with_returns_true_when_types_match(
    left_from_object_type,
    left_to_object_type,
    left_relationship_object_type,
    righ_from_object_type,
    right_to_object_type,
    right_relationship_object_type,
    expected_result,
):
    left = PresentRelationship(
        KnownTypeMarker(left_from_object_type),
        KnownTypeMarker(left_to_object_type),
        KnownTypeMarker(left_relationship_object_type),
        Cardinality.SINGLE,
        Cardinality.SINGLE,
    )
    right = PresentRelationship(
        KnownTypeMarker(righ_from_object_type),
        KnownTypeMarker(right_to_object_type),
        KnownTypeMarker(right_relationship_object_type),
        Cardinality.SINGLE,
        Cardinality.SINGLE,
    )
    assert_that(left.overlaps_with(right), equal_to(expected_result))


def test_present_relationship_include_errors_when_types_mismatch():
    with pytest.raises(ValueError):
        simple_present_relationship = PresentRelationship(
            KnownTypeMarker("From"),
            KnownTypeMarker("To"),
            KnownTypeMarker("Rel"),
            Cardinality.SINGLE,
            Cardinality.SINGLE,
        )
        other = PresentRelationship(
            KnownTypeMarker("Other"),
            KnownTypeMarker("Other"),
            KnownTypeMarker("Other"),
            Cardinality.SINGLE,
            Cardinality.SINGLE,
        )
        simple_present_relationship.include(other)
        assert_that(simple_present_relationship, equal_to(other))


def test_present_relationship_include_updates_cardinalities():
    simple_present_relationship = PresentRelationship(
        KnownTypeMarker("From"),
        KnownTypeMarker("To"),
        KnownTypeMarker("Rel"),
        Cardinality.SINGLE,
        Cardinality.SINGLE,
    )
    other = PresentRelationship(
        KnownTypeMarker("From"),
        KnownTypeMarker("To"),
        KnownTypeMarker("Rel"),
        Cardinality.MANY,
        Cardinality.MANY,
    )
    simple_present_relationship.include(other)
    assert_that(simple_present_relationship, equal_to(other))


def test_present_relationships_resolve_types_from_source(simple_source_node_shape):
    simple_present_relationship = PresentRelationship(
        UnknownTypeMarker.source_node(),
        KnownTypeMarker("To"),
        KnownTypeMarker("Rel"),
        Cardinality.SINGLE,
        Cardinality.SINGLE,
    )

    simple_present_relationship.resolve_types([simple_source_node_shape])
    assert_that(
        simple_present_relationship.from_object_type,
        simple_source_node_shape.object_type,
    )


def test_present_relationships_resolve_types_to_source(simple_source_node_shape):
    simple_present_relationship = PresentRelationship(
        KnownTypeMarker("From"),
        UnknownTypeMarker.source_node(),
        KnownTypeMarker("Rel"),
        Cardinality.SINGLE,
        Cardinality.SINGLE,
    )

    simple_present_relationship.resolve_types([simple_source_node_shape])
    assert_that(
        simple_present_relationship.to_object_type,
        simple_source_node_shape.object_type,
    )


def test_graph_schema_merge_identity(simple_source_node_shape):
    schema = GraphSchema([simple_source_node_shape], [])
    assert_that(schema.merge(schema), equal_to(schema))


def test_graph_schema_merge_identity_with_empty(simple_source_node_shape):
    schema = GraphSchema([simple_source_node_shape], [])
    assert_that(schema.merge(GraphSchema.empty()), equal_to(schema))


def test_graph_schema_merge_identity_with_empty_other(simple_source_node_shape):
    schema = GraphSchema([simple_source_node_shape], [])
    assert_that(GraphSchema.empty().merge(schema), equal_to(schema))


def test_graph_schema_merge_two_non_empty_schemas():
    left = GraphSchema(
        [
            GraphObjectShape(
                GraphObjectType.NODE,
                KnownTypeMarker("Test"),
                PropertyMetadataSet({"foo": PropertyMetadata("foo")}),
            )
        ],
        [],
    )
    right = GraphSchema(
        [
            GraphObjectShape(
                GraphObjectType.NODE,
                KnownTypeMarker("Test"),
                PropertyMetadataSet({"bar": PropertyMetadata("bar")}),
            )
        ],
        [],
    )
    expected = GraphSchema(
        [
            GraphObjectShape(
                GraphObjectType.NODE,
                KnownTypeMarker("Test"),
                PropertyMetadataSet(
                    {
                        "foo": PropertyMetadata("foo"),
                        "bar": PropertyMetadata("bar"),
                    }
                ),
            )
        ],
        [],
    )
    assert_that(left.merge(right), equal_to(expected))


class DummyAggregatedIntrospectiveIngestionComponent(
    AggregatedIntrospectiveIngestionComponent
):
    def __init__(self, components) -> None:
        self.components = components

    def all_subordinate_components(self) -> Iterable[IntrospectiveIngestionComponent]:
        return self.components


def test_aggregated_introspection_mixin_gather_object_shapes(
    mocker, simple_source_node_shape, uknown_source_node_shape
):
    one = mocker.Mock(IntrospectiveIngestionComponent)
    one.gather_object_shapes.return_value = [simple_source_node_shape]
    two = mocker.Mock(IntrospectiveIngestionComponent)
    two.gather_object_shapes.return_value = [uknown_source_node_shape]

    subject = DummyAggregatedIntrospectiveIngestionComponent([one, two])
    assert_that(subject.gather_object_shapes(), equal_to([simple_source_node_shape]))


def test_aggregated_introspection_mixin_gather_present_relationships(mocker):
    one = mocker.Mock(IntrospectiveIngestionComponent)
    one.gather_object_shapes.return_value = []
    one.gather_present_relationships.return_value = [
        PresentRelationship(
            KnownTypeMarker("From"),
            KnownTypeMarker("To"),
            KnownTypeMarker("Rel"),
            Cardinality.SINGLE,
            Cardinality.SINGLE,
        )
    ]
    two = mocker.Mock(IntrospectiveIngestionComponent)
    two.gather_object_shapes.return_value = []
    two.gather_present_relationships.return_value = [
        PresentRelationship(
            KnownTypeMarker("From"),
            KnownTypeMarker("To"),
            KnownTypeMarker("Rel"),
            Cardinality.SINGLE,
            Cardinality.SINGLE,
        )
    ]
    subject = DummyAggregatedIntrospectiveIngestionComponent([one, two])
    assert_that(
        subject.gather_present_relationships(),
        equal_to(
            [
                PresentRelationship(
                    KnownTypeMarker("From"),
                    KnownTypeMarker("To"),
                    KnownTypeMarker("Rel"),
                    Cardinality.MANY,
                    Cardinality.MANY,
                )
            ]
        ),
    )


def test_property_override():
    override_file_data = {"type": "DATETIME"}
    override = PropertyOverride.from_file_data(override_file_data)
    assert_that(override, equal_to(PropertyOverride(PropertyType.DATETIME)))
    existing_property = PropertyMetadata("foo", PropertyType.STRING)
    override.apply_to(existing_property)
    assert_that(existing_property.type, equal_to(PropertyType.DATETIME))


def test_property_overrides():
    overiddes_file_data = {
        "foo": {"type": "DATETIME"},
    }
    overrides = PropertyOverrides.from_file_data(overiddes_file_data)
    assert_that(
        overrides,
        equal_to(
            PropertyOverrides(
                {
                    "foo": PropertyOverride(PropertyType.DATETIME),
                }
            )
        ),
    )
    existing_properties = PropertyMetadataSet(
        {
            "foo": PropertyMetadata("foo", PropertyType.STRING),
            "bar": PropertyMetadata("bar", PropertyType.INTEGER),
        }
    )
    overrides.apply_to(GraphObjectShape("", "", existing_properties))
    assert_that(
        existing_properties,
        equal_to(
            PropertyMetadataSet(
                {
                    "foo": PropertyMetadata("foo", PropertyType.DATETIME),
                    "bar": PropertyMetadata("bar", PropertyType.INTEGER),
                }
            )
        ),
    )


def test_graph_schema_apply_type_overrides_from_file(mocker):
    schema = GraphSchema.empty()
    overrides = mocker.Mock(PropertyOverrides)
    GraphSchemaOverrides.from_file = mocker.Mock(return_value=overrides)
    schema.apply_type_overrides_from_file("foo")
    GraphSchemaOverrides.from_file.assert_called_once_with("foo")
    overrides.apply_to.assert_called_once_with(schema)
