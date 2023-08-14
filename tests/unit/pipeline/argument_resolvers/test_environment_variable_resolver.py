from hamcrest import assert_that, equal_to

from nodestream.pipeline.argument_resolvers import EnvironmentResolver


def test_resolve_argument_value_resolution(load_file_with_resolver, mocker):
    mocker.patch.dict("os.environ", {"TEST_ENV": "SET_FROM_ENV"})
    file = "tests/unit/pipeline/argument_resolvers/fixtures/with_env.yaml"
    result = load_file_with_resolver(file, EnvironmentResolver)
    assert_that(result, equal_to({"test": {"env": "SET_FROM_ENV", "not_env": 123}}))
