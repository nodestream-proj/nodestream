from hamcrest import assert_that, equal_to

from nodestream.pipeline.scope_config import ScopeConfig


def test_from_file_data_string_input():
    result = ScopeConfig.from_file_data({"TestKey": "TestValue"})
    assert_that(result.config, equal_to({"TestKey": "TestValue"}))
