from typing import Any

from hamcrest.core.base_matcher import BaseMatcher

from nodestream_plugin_neo4j.query import Query


class RanQueryOnShot(BaseMatcher):
    def __init__(self, query: Query):
        self.query = query

    def search_mock(self, mock) -> bool:
        return any((c.args and c.args[0] == self.query) for c in mock.await_args_list)

    def _matches(self, item: Any) -> bool:
        return self.search_mock(item.database_connection.execute)

    def describe_to(self, description):
        description.append_text("query: ").append_text(self.query)


def ran_query(query: Query):
    return RanQueryOnShot(query)
