import pytest

from nodestream.utils import StringSuggester, LayeredDict, LayeredList
from copy import deepcopy


FRUITS = ["apple", "banana", "cherry"]


@pytest.mark.parametrize(
    "strings,input,expected",
    [
        (FRUITS, "banan", "banana"),
        (FRUITS, "cheri", "cherry"),
        (FRUITS, "appl", "apple"),
        (FRUITS, "app", "apple"),
        (FRUITS, "ban", "banana"),
        (FRUITS, "cher", "cherry"),
    ],
)
def test_suggest_closest(strings, input, expected):
    suggester = StringSuggester(strings)
    assert suggester.suggest_closest(input) == expected


def test_factory_creation():
    layered_list = LayeredList()
    layered_dict = LayeredDict()
    assert layered_list._data == [[]]
    assert layered_dict._data == [{}]

def test_get_and_set_within_context():
    layered_list = LayeredList()
    layered_dict = LayeredDict()
    layered_list.append("item")
    layered_dict["key"] = "item"
    layered_list.increment_context_level()
    layered_dict.increment_context_level()
    layered_list.append("item2")
    layered_dict["key2"] = "item2"
    assert (layered_list[0], layered_list[1]) == ("item", "item2")
    assert (layered_dict["key"], layered_dict["key2"]) == ("item", "item2")
    layered_list[0] = "new_item"
    assert (layered_list[0], layered_list[1]) == ("item", "new_item")

TEST_LIST_LAYERS = [[1, 2, 3],[4, 5, 6]]
TEST_DICT_LAYERS = [{"1": 1, "2": 2, "3": 3},{"4": 4, "5": 5, "6": 6}]

@pytest.fixture
def subject():
    return deepcopy(TEST_LIST_LAYERS), deepcopy(TEST_DICT_LAYERS)


def test_contains(subject):
    layered_list = LayeredList(subject[0])
    layered_dict = LayeredDict(subject[1])
    assert 1 in layered_list
    assert "1" in layered_dict
    assert 7 not in layered_list
    assert "7" not in layered_dict

def test_pop(subject):
    layered_list = LayeredList(subject[0])
    layered_dict = LayeredDict(subject[1])
    assert layered_list.pop() == 6
    assert layered_dict.pop("4", None) == 4
    assert layered_list.pop(1) == 5
    assert layered_dict.pop("1", None) == None

def test_context_level_changes(subject):
    layered_list = LayeredList()
    layered_dict = LayeredDict()
    assert layered_list._data == [[]]
    assert layered_dict._data == [{}]
    layered_list.increment_context_level()
    layered_dict.increment_context_level()
    assert layered_list._data == [[], []]
    assert layered_dict._data == [{}, {}]
    layered_list.decrement_context_level()
    layered_dict.decrement_context_level()
    assert layered_list._data == [[]]
    assert layered_dict._data == [{}]


def test_iterators(subject):
    layered_list = LayeredList(subject[0])
    layered_dict = LayeredDict(subject[1])
    assert [item for item in layered_list] == [1,2,3,4,5,6]
    assert [value for value in layered_dict.values()] == [1,2,3,4,5,6]
    assert [key for key in layered_dict.keys()] == ["1","2","3","4","5","6"]
    assert [(key, value) for key, value in layered_dict.items()] == [("1",1),("2",2),("3",3),("4",4),("5",5),("6",6)]


def test_other_functionalities(subject):
    layered_list = LayeredList()
    layered_dict = LayeredDict(subject[1])
    layered_list.append(1)
    assert layered_list._data == [[1]]
    layered_list.increment_context_level()
    layered_list.append(2)
    assert layered_list._data == [[1], [2]]
    assert layered_dict.get("1", None) == 1
    assert layered_dict.get("4", None) == 4
    assert layered_dict.get("7", None) == None