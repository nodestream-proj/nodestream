from hamcrest import assert_that, equal_to

from nodestream.pipeline.argument_resolvers import IncludeFileResolver


def test_include_file_resolution(load_file_with_resolver, mocker):
    file = "tests/unit/pipeline/argument_resolvers/fixtures/with_file.yaml"
    result = load_file_with_resolver(file, IncludeFileResolver)
    assert_that(
        result, equal_to({"test": {"included_test_properties": {"included": "TEST"}}})
    )
