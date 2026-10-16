"""Sort keys order as Python orders them, whatever their kinds, sizes, and nesting."""

import math
import sys
from collections.abc import Callable
from collections.abc import Sequence
from fractions import Fraction
from typing import NamedTuple
from typing import cast

import pytest
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from spillsort import Sorter
from spillsort import SortKey
from typing_extensions import override

from tests.helpers import python_sorted

MAX_FLOAT = sys.float_info.max
EDGE_INTS = [
    2**53 + 1,
    2**63,
    2**64 - 1,
    2**127,
    2**128,
    int(MAX_FLOAT),
    int(MAX_FLOAT) + 1,
    int(MAX_FLOAT) + 2**970,
    2**1024,
    2**1024 + 1,
    10**400,
]
EDGE_FLOATS = [0.0, 0.5, 2.0**53, 2.0**63, 2.0**127, MAX_FLOAT, math.inf]

numbers: st.SearchStrategy[int | float] = st.one_of(
    st.integers(),
    st.sampled_from(EDGE_INTS).flatmap(
        lambda edge: st.sampled_from([edge, -edge, edge - 1, 1 - edge])
    ),
    st.floats(allow_nan=False),
    st.sampled_from(EDGE_FLOATS).flatmap(lambda edge: st.sampled_from([edge, -edge])),
    st.booleans(),
)

keys: st.SearchStrategy[SortKey] = st.recursive(
    st.one_of(
        st.none(),
        numbers,
        st.text(max_size=4),
        st.text(alphabet=st.characters(categories=["Cs", "Ll"]), max_size=3),
        st.binary(max_size=4),
    ),
    lambda children: st.one_of(st.tuples(children, children), st.lists(children, max_size=3)),
    max_leaves=6,
)


def sorted_by_sorter(values: list[SortKey]) -> list[SortKey]:
    sorter: Sorter[SortKey] = Sorter(max_bytes=64)
    sorter.extend(values)
    return list(sorter)


def order_by_sorter(keys: Sequence[object]) -> list[int]:
    """The positions of `keys` in the order the sorter sorts them."""
    sorter = Sorter(cast("Callable[[int], SortKey]", keys.__getitem__), max_bytes=64)
    sorter.extend(range(len(keys)))
    return list(sorter)


def comparable(values: list[SortKey]) -> bool:
    try:
        python_sorted(values)
    except TypeError:
        return False
    return True


@settings(max_examples=500, deadline=None)
@given(st.lists(numbers, max_size=40))
def test_numbers_of_any_size_and_kind_sort_as_python_sorts_them(values: list[SortKey]) -> None:
    assert sorted_by_sorter(values) == python_sorted(values)


@settings(max_examples=300, deadline=None)
@given(st.lists(keys, max_size=30))
def test_keys_sort_as_python_sorts_them(values: list[SortKey]) -> None:
    if comparable(values):
        assert sorted_by_sorter(values) == python_sorted(values)


@settings(max_examples=200, deadline=None)
@given(st.lists(numbers, max_size=40))
def test_equal_numbers_keep_the_order_they_were_added(values: list[SortKey]) -> None:
    assert order_by_sorter(values) == python_sorted(range(len(values)), key=values.__getitem__)


def test_ints_and_floats_that_are_equal_keep_their_order() -> None:
    values: list[SortKey] = [1.0, True, 1, 0, -0.0, False, 0.0, 2**53, 2.0**53]
    assert [repr(value) for value in sorted_by_sorter(values)] == [
        "0",
        "-0.0",
        "False",
        "0.0",
        "1.0",
        "True",
        "1",
        "9007199254740992",
        "9007199254740992.0",
    ]


def test_ints_beyond_every_float_sort_inside_the_infinities() -> None:
    values: list[SortKey] = [math.inf, 10**400, MAX_FLOAT, -(10**400), -math.inf, -MAX_FLOAT]
    expected: list[SortKey] = [-math.inf, -(10**400), -MAX_FLOAT, MAX_FLOAT, 10**400, math.inf]
    assert sorted_by_sorter(values) == expected


def test_nan_sorts_after_every_number_in_the_order_added() -> None:
    nan = float("nan")
    assert order_by_sorter([nan, math.inf, 10**400, -nan, -1]) == [4, 2, 1, 0, 3]


def test_text_with_lone_surrogates_sorts_by_code_point() -> None:
    values: list[SortKey] = ["\U0001f600", "\udc00", "\ud800", "￿", "a", ""]
    assert sorted_by_sorter(values) == python_sorted(values)


def test_text_bytes_and_sequences_sort_after_their_prefixes() -> None:
    values: list[SortKey] = ["a\x00", "a", b"a\x00", b"a", (1, None), (1,), [1, 0], [1]]
    expected: list[SortKey] = ["a", "a\x00", b"a", b"a\x00", (1,), (1, None), [1], [1, 0]]
    assert sorted_by_sorter(values) == expected


def test_kinds_python_cannot_compare_sort_by_kind() -> None:
    values: list[SortKey] = [[1], (1,), b"1", "1", 1, None]
    assert sorted_by_sorter(values) == [None, 1, "1", b"1", (1,), [1]]


class Index:
    def __init__(self, value: int) -> None:
        self.value: int = value

    def __index__(self) -> int:
        return self.value


class Big(int):
    pass


class Text(str):
    @override
    def encode(self, encoding: str = "utf-8", errors: str = "strict") -> bytes:
        raise AssertionError("the subclass's encode was called")


class Point(NamedTuple):
    x: int
    y: int


def test_subclasses_and_integer_like_values_sort_as_their_base_values() -> None:
    keys = [Big(2**100), Index(-(2**70)), Text("\udc00"), Point(1, 2), 1.5]
    assert order_by_sorter(keys) == [1, 4, 0, 2, 3]


@pytest.mark.parametrize(
    ("key", "message"),
    [
        (
            object(),
            "a sort key must be an int, float, str, bytes, None, or a tuple or list of them",
        ),
        (Fraction(1, 3), "not Fraction"),
        ((1, {2}), "not set"),
    ],
)
def test_values_that_are_not_sort_keys_are_refused(key: object, message: str) -> None:
    def refused(_: int) -> SortKey:
        return cast("SortKey", key)

    sorter = Sorter(refused)
    with pytest.raises(TypeError, match=message):
        sorter.add(0)
    assert len(sorter) == 0
    sorter.add_encoded(1, b"")
    with pytest.raises(TypeError, match=message):
        sorter.add_encoded(cast("SortKey", key), b"")
    assert list(sorter.encoded()) == [b""]


def test_keys_nested_too_deeply_are_refused() -> None:
    key: SortKey = 1
    for _ in range(300):
        key = (key,)
    sorter: Sorter[SortKey] = Sorter()
    with pytest.raises(RecursionError, match="nested more than 256 deep"):
        sorter.add(key)
