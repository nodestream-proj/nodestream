from pathlib import Path

from hamcrest.core.base_matcher import BaseMatcher
from hamcrest.core.description import Description


class IsExistingPath(BaseMatcher):
    def _matches(self, item: Path) -> bool:
        return item.exists()

    def describe_to(self, description: Description) -> None:
        description.append_text("to exist on the file system")


def exists():
    return IsExistingPath()
