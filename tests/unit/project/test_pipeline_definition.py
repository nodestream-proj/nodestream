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
    result = PipelineDefinition.from_file_data("path/to/pipeline", set(), {})
    assert_that(result.name, equal_to("pipeline"))
    assert_that(result.file_path, equal_to(Path("path/to/pipeline")))


def test_from_file_data_complex_input():
    result = PipelineDefinition.from_file_data(
        {"path": "path/to/pipeline", "name": "test", "annotations": {"foo": "bar"}},
        set(),
        {"baz": "qux"},
    )
    assert_that(result.name, equal_to("test"))
    assert_that(result.file_path, equal_to(Path("path/to/pipeline")))
    assert_that(
        result.configuration.annotations, equal_to({"foo": "bar", "baz": "qux"})
    )


def test_from_plugin_data_complex_input():
    result = PipelineDefinition.from_plugin_data(
        {
            "name": "test",
            "annotations": {"foo": "bar"},
            "targets": ["target2"],
            "exclude_inherited_targets": True,
        },
        set("target1"),
        {"baz": "qux"},
    )
    assert_that(result.name, equal_to("test"))
    assert_that(result.configuration.targets, equal_to(["target2"]))
    assert_that(
        result.configuration.annotations, equal_to({"foo": "bar", "baz": "qux"})
    )


def test_use_configuration():
    result = PipelineDefinition.from_plugin_data(
        {
            "name": "test",
            "annotations": {"foo": "bar"},
            "targets": ["target2"],
            "exclude_inherited_targets": True,
        },
        set("target1"),
        {"baz": "qux"},
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
                    "test1", Path("test1.yaml"), PipelineConfiguration(set(), False, {})
                ),
                "test1.yaml",
            )
        ),
        (
            PipelineDefinition(
                "test2",
                Path("test2.yaml"),
                PipelineConfiguration(set(), False, {"foo": True}),
            ),
            {"path": "test2.yaml", "annotations": {"foo": True}},
        ),
        (
            PipelineDefinition(
                "baz3",
                Path("test3.yaml"),
                PipelineConfiguration(set(), False, {"foo": True}),
            ),
            {"path": "test3.yaml", "annotations": {"foo": True}, "name": "baz3"},
        ),
        (
            PipelineDefinition(
                "baz4", Path("test4.yaml"), PipelineConfiguration(set(), False, {})
            ),
            {"path": "test4.yaml", "name": "baz4"},
        ),
    ],
)
def test_to_file_data(definition, expected_data):
    assert_that(definition.to_file_data(), equal_to(expected_data))
    assert_that(
        PipelineDefinition.from_file_data(expected_data, set(), {}),
        equal_to(definition),
    )


@pytest.mark.parametrize(
    "definition,expected_data",
    [
        (
            PipelineDefinition(
                "test", Path("test.yaml"), PipelineConfiguration(set(), False, {})
            ),
            {
                "path": "test.yaml",
                "name": "test",
                "targets": [],
                "annotations": {},
            },
        ),
        (
            PipelineDefinition(
                "test",
                Path("test.yaml"),
                PipelineConfiguration(set(), False, {"foo": True}),
            ),
            {
                "path": "test.yaml",
                "name": "test",
                "targets": [],
                "annotations": {"foo": True},
            },
        ),
        (
            PipelineDefinition(
                "baz",
                Path("test.yaml"),
                PipelineConfiguration(set(), False, {"foo": True}),
            ),
            {
                "path": "test.yaml",
                "name": "baz",
                "targets": [],
                "annotations": {"foo": True},
            },
        ),
        (
            PipelineDefinition(
                "baz", Path("test.yaml"), PipelineConfiguration(set(["t1"]))
            ),
            {"path": "test.yaml", "name": "baz", "targets": ["t1"], "annotations": {}},
        ),
    ],
)
def test_to_file_data_verbose(definition, expected_data):
    assert_that(definition.to_file_data(verbose=True), equal_to(expected_data))
    assert_that(
        PipelineDefinition.from_file_data(expected_data, set(), {}),
        equal_to(definition),
    )


def test_from_path():
    path = Path("test.yaml")
    result = PipelineDefinition.from_path(path)
    assert_that(result.name, equal_to("test"))
    assert_that(result.configuration.annotations, equal_to({}))
    assert_that(result.file_path, equal_to(path))
