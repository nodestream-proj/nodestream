from pathlib import Path

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
