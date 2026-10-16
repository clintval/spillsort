"""Type checkers infer what a sorter holds from its key function or codec, or else sort keys."""

from spillsort import Sorter
from spillsort import SortKey
from typing_extensions import assert_type


def position(read: tuple[str, int, str]) -> tuple[str, int]:
    return read[0], read[1]


class Text:
    def encode(self, item: str, /) -> bytes:
        return item.encode()

    def decode(self, data: bytes, /) -> str:
        return data.decode()


def test_a_sorter_without_a_key_function_holds_sort_keys() -> None:
    sorter = assert_type(Sorter(), Sorter[SortKey])
    assert list(sorter.sort([2, None, 1.5])) == [None, 1.5, 2]
    assert list(assert_type(Sorter(codec=Text()), Sorter[str]).sort(["b", "a"])) == ["a", "b"]


def test_a_key_function_or_codec_says_what_a_sorter_holds() -> None:
    reads = [("chr2", 5, "read1"), ("chr1", 9, "read2")]
    assert_type(Sorter(position), Sorter[tuple[str, int, str]])
    sorter = assert_type(Sorter(key=position), Sorter[tuple[str, int, str]])
    assert list(sorter.sort(reads)) == sorted(reads, key=position)
    lines = assert_type(Sorter(lambda line: line.split()[1], codec=Text()), Sorter[str])
    assert list(lines.sort(["chr1 b", "chr1 a"])) == ["chr1 a", "chr1 b"]
