from pathlib import Path

import pytest

from nodestream.pipeline import PipelineInitializationArguments
from nodestream.project import PipelineDefinition


def test_pipeline_definition_initialize(mocker):
    mocked_load_ppl = mocker.patch(
        "nodestream.pipeline.pipeline_file_loader.PipelineFileLoader.load_pipeline"
    )
    args = PipelineInitializationArguments()
    subject = PipelineDefinition(
        "test", "tests/unit/project/fixtures/simple_pipeline.yaml"
    )
    subject.initialize(args)
    mocked_load_ppl.assert_called_once_with(args)


def test_from_file_data_string_input():
    result = PipelineDefinition.from_file_data("path/to/pipeline", {})
    assert result.name == "pipeline"
    assert result.file_path == Path("path/to/pipeline")


def test_from_file_data_complex_input():
    result = PipelineDefinition.from_file_data(
        {"path": "path/to/pipeline", "name": "test", "annotations": {"foo": "bar"}},
        {"baz": "qux"},
    )
    assert result.name == "test"
    assert result.file_path == Path("path/to/pipeline")
    assert result.annotations == {"foo": "bar", "baz": "qux"}


@pytest.mark.parametrize(
    "definition,expected_data",
    [
        (PipelineDefinition("test", Path("test.yaml")), "test.yaml"),
        (
            PipelineDefinition("test", Path("test.yaml"), {"foo": True}),
            {"path": "test.yaml", "annotations": {"foo": True}},
        ),
        (
            PipelineDefinition("baz", Path("test.yaml"), {"foo": True}),
            {"path": "test.yaml", "annotations": {"foo": True}, "name": "baz"},
        ),
        (
            PipelineDefinition("baz", Path("test.yaml")),
            {"path": "test.yaml", "name": "baz"},
        ),
    ],
)
def test_as_file_definition(definition, expected_data):
    assert definition.as_file_definition() == expected_data
    assert PipelineDefinition.from_file_data(expected_data, {}) == definition


def test_from_path():
    path = Path("test.yaml")
    result = PipelineDefinition.from_path(path)
    assert result.name == "test"
    assert result.annotations == {}
    assert result.file_path == path
