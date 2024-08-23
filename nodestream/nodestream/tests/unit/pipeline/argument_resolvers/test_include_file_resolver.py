from hamcrest import assert_that, equal_to

from nodestream.pipeline.argument_resolvers import IncludeFileResolver


def test_include_file_resolution(load_file_with_resolver, mocker):
    file = "nodestream/nodestream/tests/unit/pipeline/argument_resolvers/fixtures/with_file.yaml"
    mocker.patch.dict("os.environ", {"TEST_ENV": "SET_FROM_ENV"})
    result = load_file_with_resolver(file, IncludeFileResolver)
    assert_that(
        result,
        equal_to(
            {
                "test": {
                    "included_test_properties": {"included": "TEST"},
                    "included_test_properties_with_env": {
                        "test": {"env": "SET_FROM_ENV", "not_env": 123}
                    },
                }
            }
        ),
    )
