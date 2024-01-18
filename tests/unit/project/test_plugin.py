from hamcrest import assert_that, equal_to
from nodestream.pipeline.scope_config import ScopeConfig
from nodestream.project.plugin import PluginConfiguration


def test_from_plugin_data_complex_input():
    result = PluginConfiguration.from_file_data(
        {
            "name": "test",
            "annotations": {"foo": "bar"},
            "targets": ["target2"],
            "config": {"username": "bob"},
        },
    )

    assert_that(result.name, equal_to("test"))
    assert_that(result.targets, equal_to(["target2"]))
    assert_that(result.annotations, equal_to({"foo": "bar"}))
    assert_that(result.config, equal_to(ScopeConfig({"username": "bob"})))


def test_update_pipeline_configurations():
    result = PluginConfiguration.from_file_data(
        {
            "name": "test",
            "annotations": {"1": "2"},
            "targets": ["target2"],
            "config": {"username": "bob"},
            "pipelines": [
                {
                    "name": "pipeline1",
                    "targets": ["target1"],
                    "annotations": {"baz": "qux"},
                }
            ],
        },
    )

    other = PluginConfiguration.from_file_data(
        {
            "name": "test",
            "annotations": {"foo": "bar"},
            "targets": ["target2"],
            "config": {"username": "bob"},
            "pipelines": [
                {
                    "name": "pipeline1",
                    "targets": ["target2"],
                    "annotations": {"foo": "bar"},
                }
            ],
        },
    )

    result.update_pipeline_configurations(other)
    assert_that(result.annotations, equal_to({"1": "2"}))
    assert_that(result.pipelines_by_name, equal_to(other.pipelines_by_name))
