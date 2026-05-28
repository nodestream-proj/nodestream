from pathlib import Path

import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline import PipelineInitializationArguments
from nodestream.project import PipelineConfiguration, PipelineDefinition


def test_pipeline_definition_initialize(mocker):
    mocked_load_ppl = mocker.patch(
        "nodestream.pipeline.pipeline_file_loader.PipelineFile.load_pipeline"
    )
    args = PipelineInitializationArguments()
    subject = PipelineDefinition(
        "test", "tests/unit/project/fixtures/simple_pipeline.yaml"
    )
    subject.initialize(args)
    mocked_load_ppl.assert_called_once_with(args)


def test_from_file_data_string_input():
    result = PipelineDefinition.from_file_data(
        "path/to/pipeline", PipelineConfiguration()
    )
    assert_that(result.name, equal_to("pipeline"))
    assert_that(result.file_path, equal_to(Path("path/to/pipeline")))


def test_from_file_data_complex_input():
    result = PipelineDefinition.from_file_data(
        {"path": "path/to/pipeline", "name": "test", "annotations": {"foo": "bar"}},
        PipelineConfiguration(annotations={"baz": "qux"}),
    )
    assert_that(result.name, equal_to("test"))
    assert_that(result.file_path, equal_to(Path("path/to/pipeline")))
    assert_that(
        result.configuration.effective_annotations,
        equal_to({"foo": "bar", "baz": "qux"}),
    )


def test_from_plugin_data_complex_input():
    result = PipelineDefinition.from_plugin_data(
        {
            "name": "test",
            "annotations": {"foo": "bar"},
            "targets": ["target2"],
            "exclude_inherited_targets": True,
        },
        PipelineConfiguration(
            ["target1"],
            False,
            {"baz": "qux"},
        ),
    )
    assert_that(result.name, equal_to("test"))
    assert_that(result.configuration.effective_targets, equal_to({"target2"}))
    assert_that(
        result.configuration.effective_annotations,
        equal_to({"foo": "bar", "baz": "qux"}),
    )


def test_use_configuration():
    result = PipelineDefinition.from_plugin_data(
        {
            "name": "test",
            "annotations": {"foo": "bar"},
            "targets": ["target2"],
            "exclude_inherited_targets": True,
        },
        PipelineConfiguration(
            ["target1"],
            False,
            {"baz": "qux"},
        ),
    )
    new_config = PipelineConfiguration(set(["other_target"]), False, {"foo": "bar"})
    result.use_configuration(new_config)

    assert_that(result.configuration, equal_to(new_config))


@pytest.mark.parametrize(
    "definition,expected_data",
    [
        (
            (
                PipelineDefinition(
                    "test1", Path("test1.yaml"), PipelineConfiguration([], False, {})
                ),
                "test1.yaml",
            )
        ),
        (
            PipelineDefinition(
                "test2",
                Path("test2.yaml"),
                PipelineConfiguration([], False, {"foo": True}),
            ),
            {"path": "test2.yaml", "annotations": {"foo": True}},
        ),
        (
            PipelineDefinition(
                "baz3",
                Path("test3.yaml"),
                PipelineConfiguration([], False, {"foo": True}),
            ),
            {"path": "test3.yaml", "annotations": {"foo": True}, "name": "baz3"},
        ),
        (
            PipelineDefinition(
                "baz4", Path("test4.yaml"), PipelineConfiguration([], False, {})
            ),
            {"path": "test4.yaml", "name": "baz4"},
        ),
    ],
)
def test_to_file_data(definition, expected_data):
    assert_that(definition.to_file_data(), equal_to(expected_data))
    assert_that(
        PipelineDefinition.from_file_data(expected_data, None),
        equal_to(definition),
    )


@pytest.mark.parametrize(
    "definition,expected_data",
    [
        (
            PipelineDefinition(
                "test", Path("test.yaml"), PipelineConfiguration([], False, {})
            ),
            {
                "path": "test.yaml",
                "name": "test",
                "targets": [],
                "annotations": {},
                "exclude_inherited_targets": False,
            },
        ),
        (
            PipelineDefinition(
                "test",
                Path("test.yaml"),
                PipelineConfiguration([], False, {"foo": True}),
            ),
            {
                "path": "test.yaml",
                "name": "test",
                "targets": [],
                "annotations": {"foo": True},
                "exclude_inherited_targets": False,
            },
        ),
        (
            PipelineDefinition(
                "baz",
                Path("test.yaml"),
                PipelineConfiguration([], False, {"foo": True}),
            ),
            {
                "path": "test.yaml",
                "name": "baz",
                "targets": [],
                "annotations": {"foo": True},
                "exclude_inherited_targets": False,
            },
        ),
        (
            PipelineDefinition("baz", Path("test.yaml"), PipelineConfiguration(["t1"])),
            {
                "path": "test.yaml",
                "name": "baz",
                "targets": ["t1"],
                "annotations": {},
                "exclude_inherited_targets": False,
            },
        ),
    ],
)
def test_to_file_data_verbose(definition, expected_data):
    assert_that(definition.to_file_data(verbose=True), equal_to(expected_data))
    assert_that(
        PipelineDefinition.from_file_data(expected_data, None),
        equal_to(definition),
    )


def test_from_path():
    path = Path("test.yaml")
    result = PipelineDefinition.from_path(path)
    assert_that(result.name, equal_to("test"))
    assert_that(result.configuration.annotations, equal_to({}))
    assert_that(result.file_path, equal_to(path))


def test_pipeline_definition_initialize_for_schema_collection(mocker):
    mocked_load = mocker.patch(
        "nodestream.pipeline.pipeline_file_loader.PipelineFile.load_pipeline_for_schema_collection"
    )
    subject = PipelineDefinition(
        "test", "tests/unit/project/fixtures/simple_pipeline.yaml"
    )
    subject.initialize_for_schema_collection()
    mocked_load.assert_called_once()


def test_pipeline_definition_expand_schema_uses_schema_collection(mocker):
    mocked_pipeline = mocker.MagicMock()
    mocker.patch(
        "nodestream.project.pipeline_definition.PipelineDefinition.initialize_for_schema_collection",
        return_value=mocked_pipeline,
    )
    coordinator = mocker.MagicMock()
    coordinator.pipeline_context.return_value = mocker.MagicMock(
        __enter__=mocker.MagicMock(return_value=None),
        __exit__=mocker.MagicMock(return_value=False),
    )
    subject = PipelineDefinition(
        "test", "tests/unit/project/fixtures/simple_pipeline.yaml"
    )
    subject.expand_schema(coordinator)
    mocked_pipeline.expand_schema.assert_called_once_with(coordinator)


def test_pipeline_definition_expand_schema_with_introspection(mocker):
    mocked_pipeline = mocker.MagicMock()
    mocker.patch(
        "nodestream.project.pipeline_definition.PipelineDefinition.initialize_for_introspection",
        return_value=mocked_pipeline,
    )
    coordinator = mocker.MagicMock()
    coordinator.pipeline_context.return_value = mocker.MagicMock(
        __enter__=mocker.MagicMock(return_value=None),
        __exit__=mocker.MagicMock(return_value=False),
    )
    subject = PipelineDefinition(
        "test", "tests/unit/project/fixtures/simple_pipeline.yaml"
    )
    subject.expand_schema_with_introspection(coordinator)
    mocked_pipeline.expand_schema.assert_called_once_with(coordinator)


def test_effective_annotation_prioritizes_self():
    pipeline_configuration = PipelineConfiguration(
        targets=[],
        exclude_inherited_targets=False,
        annotations={"key": "value"},
        parent=PipelineConfiguration(
            targets=[],
            exclude_inherited_targets=False,
            annotations={"key": "other_value", "other_key": "arbitrary_value"},
        ),
    )
    assert pipeline_configuration.effective_annotations == {
        "key": "value",
        "other_key": "arbitrary_value",
    }


def test_expand_schema_for_copy_logs_warning_on_failure(mocker):
    """expand_schema_for_copy swallows exceptions and logs a warning."""
    coordinator = mocker.Mock()
    subject = PipelineDefinition(
        "test", "tests/unit/project/fixtures/simple_pipeline.yaml"
    )
    subject.initialize_for_schema_collection = mocker.Mock(
        side_effect=RuntimeError("collection failed")
    )
    mock_logger = mocker.Mock()
    mocker.patch("nodestream.project.pipeline_definition.logger", mock_logger)
    subject.expand_schema_for_copy(coordinator)
    mock_logger.warning.assert_called_once()
    assert "test" in mock_logger.warning.call_args[0][1]


def test_expand_schema_for_copy_happy_path(mocker):
    """expand_schema_for_copy calls initialize_for_schema_collection and expand_schema."""
    coordinator = mocker.MagicMock()
    subject = PipelineDefinition(
        "test", "tests/unit/project/fixtures/simple_pipeline.yaml"
    )
    mock_pipeline = mocker.Mock()
    subject.initialize_for_schema_collection = mocker.Mock(return_value=mock_pipeline)
    subject.expand_schema_for_copy(coordinator)
    mock_pipeline.expand_schema.assert_called_once_with(coordinator)


def test_expand_schema_calls_initialize_for_schema_collection(mocker):
    """expand_schema calls initialize_for_schema_collection and expand_schema."""
    coordinator = mocker.MagicMock()
    subject = PipelineDefinition(
        "test", "tests/unit/project/fixtures/simple_pipeline.yaml"
    )
    mock_pipeline = mocker.Mock()
    subject.initialize_for_schema_collection = mocker.Mock(return_value=mock_pipeline)
    subject.expand_schema(coordinator)
    mock_pipeline.expand_schema.assert_called_once_with(coordinator)
