from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Dict, Generator, Generic, Iterable, List, TypeVar


class StringSuggester:
    """A utility for suggesting alternative strings based on similarity to a given string."""

    def __init__(self, all_strings: Iterable[str]):
        self.all_strings = list(all_strings)

    def suggest_closest(self, string: str) -> str:
        """Suggest the string that is most similar to the given string.

        Args:
            string: The string to compare to the list of strings.

        Returns:
            The string from the list of strings that is most similar to the given string.
        """
        return max(
            self.all_strings,
            key=lambda s: SequenceMatcher(None, string, s).ratio(),
        )


T = TypeVar("T")

"""
    LayeredList Datatype:
        This datatype mimicks the list datatype in python to the requestor, but involves different 
        layers of data that is aggregated together on reads, and only reveals the top layer on writes. 
"""


@dataclass(slots=True, frozen=True)
class LayeredList(Generic[T]):
    LATEST_CONTEXT_LEVEL = -1

    _data: List[List[T]] = field(default_factory=lambda: [[]])

    def __setitem__(self, index: int, item: T) -> None:
        self._data[self.LATEST_CONTEXT_LEVEL][index] = item

    def __getitem__(self, index: int) -> T:
        return self.effective_items[index]

    def __contains__(self, value: T) -> bool:
        return value in self.effective_items

    def pop(self, index: int = -1) -> T:
        return self._data[self.LATEST_CONTEXT_LEVEL].pop(index)

    def increment_context_level(self) -> None:
        self._data.append([])

    def decrement_context_level(self) -> None:
        self._data.pop()

    def append(self, data: T) -> None:
        self._data[self.LATEST_CONTEXT_LEVEL].append(data)

    def __iter__(self) -> Generator[T, None, None]:
        yield from self.effective_items

    def __del__(self) -> None:
        self.decrement_context_level()

    @property
    def effective_items(self) -> List[T]:
        return_value = []
        for list_obj in self._data:
            return_value += list_obj
        return return_value


K = TypeVar("K")
V = TypeVar("V")

"""
    LayeredDict Datatype:
        This datatype mimicks the dictionary datatype in python to the requestor, but involves different 
        layers of data that is aggregated together on reads, and only reveals the top layer on writes. 
"""


@dataclass(slots=True, frozen=True)
class LayeredDict(Generic[K, V]):
    LATEST_CONTEXT_LEVEL = -1

    _data: List[Dict[K, V]] = field(default_factory=lambda: [{}])

    def __setitem__(self, key: K, item: V):
        self._data[self.LATEST_CONTEXT_LEVEL][key] = item

    def __getitem__(self, key: K) -> V:
        return self.effective_items[key]

    def __contains__(self, key: K) -> bool:
        return key in self.effective_items

    def increment_context_level(self) -> None:
        self._data.append({})

    def decrement_context_level(self) -> None:
        self._data.pop()

    def pop(self, key: K, default: V) -> V:
        return self._data[self.LATEST_CONTEXT_LEVEL].pop(key, default)

    def get(self, key: K, default: V) -> V:
        return self.effective_items.get(key, default)

    def items(self) -> Generator[tuple[K, V], None, None]:
        yield from ([k, v] for k, v in self.effective_items.items())

    def values(self) -> Generator[V, None, None]:
        yield from (v for v in self.effective_items.values())

    def keys(self) -> Generator[V, None, None]:
        yield from (k for k in self.effective_items.keys())

    def __del__(self) -> None:
        self.decrement_context_level()

    @property
    def effective_items(self):
        return_value = {}
        for dict_obj in self._data:
            for key, value in dict_obj.items():
                return_value[key] = value
        return return_value
