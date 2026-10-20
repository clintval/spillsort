"""A sorter returns what was added in sorted, stable order, and leaves no files behind."""

import errno
import gc
import io
import os
import pickle
import shutil
import sys
import tempfile
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
from dataclasses import dataclass
from operator import itemgetter
from pathlib import Path
from typing import Any
from typing import TypeVar
from typing import cast

import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from spillsort import Codec
from spillsort import Sorter
from spillsort import SortKey

from tests.helpers import python_sorted
from tests.helpers import spilled


@dataclass(frozen=True)
class Feature:
    refname: str
    start: int
    end: int


def features(count: int) -> list[Feature]:
    return [Feature(f"chr{index % 3}", (index * 7919) % 1000, index) for index in range(count)]


def by_position(feature: Feature) -> SortKey:
    return (feature.refname, feature.start)


T = TypeVar("T")


def add_all(sorter: Sorter[T], items: Iterable[T]) -> None:
    for item in items:
        sorter.add(item)


@settings(
    max_examples=100, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
@given(
    st.lists(st.tuples(st.integers(0, 5), st.integers()), max_size=300),
    st.integers(2, 4_000),
    st.integers(2, 4),
)
def test_items_come_out_sorted_and_stable_whatever_the_memory(
    tmp_path: Path, items: list[tuple[int, int]], max_bytes: int, max_open: int
) -> None:
    sorter = Sorter(itemgetter(0), max_bytes=max_bytes, max_open=max_open, tmp_dir=tmp_path)
    add_all(sorter, items)
    assert len(sorter) == len(items)
    assert list(sorter.sort()) == python_sorted(items, key=itemgetter(0))
    assert list(tmp_path.iterdir()) == []


def test_many_more_runs_than_can_be_open_are_merged_in_passes(tmp_path: Path) -> None:
    items = features(5_000)
    with Sorter(by_position, max_bytes=512, max_open=2, tmp_dir=tmp_path) as sorter:
        add_all(sorter, items)
        assert len(spilled(tmp_path)) > 100
        assert list(sorter.sort()) == python_sorted(items, key=by_position)
        assert list(tmp_path.iterdir()) == []


def test_items_that_fit_in_memory_are_never_spilled(tmp_path: Path) -> None:
    sorter = Sorter(by_position, tmp_dir=tmp_path)
    add_all(sorter, features(1_000))
    assert list(tmp_path.iterdir()) == []
    assert list(sorter.sort()) == python_sorted(features(1_000), key=by_position)


def test_the_key_function_can_be_given_by_name() -> None:
    items = features(100)
    assert list(Sorter(key=by_position).sort(items)) == python_sorted(items, key=by_position)


def test_an_empty_sorter_reads_nothing() -> None:
    assert list(Sorter[int]().sort()) == []
    assert list(Sorter[int]().encoded()) == []
    out = io.BytesIO()
    Sorter[int]().write_to(out)
    assert out.getvalue() == b""


class CountingCodec:
    def __init__(self) -> None:
        self.encoded: int = 0
        self.decoded: int = 0

    def encode(self, item: Feature, /) -> bytes:
        self.encoded += 1
        return f"{item.refname}\t{item.start}\t{item.end}".encode()

    def decode(self, data: bytes, /) -> Feature:
        self.decoded += 1
        refname, start, end = data.decode().split("\t")
        return Feature(refname, int(start), int(end))


def test_a_codec_encodes_and_decodes_each_item_once(tmp_path: Path) -> None:
    codec = CountingCodec()
    items = features(500)
    sorter = Sorter(by_position, codec=codec, max_bytes=256, tmp_dir=tmp_path)
    add_all(sorter, items)
    assert (codec.encoded, codec.decoded) == (500, 0)
    assert list(sorter.sort()) == python_sorted(items, key=by_position)
    assert (codec.encoded, codec.decoded) == (500, 500)


def test_items_are_pickled_without_a_codec() -> None:
    sorter = Sorter(by_position, max_bytes=256)
    add_all(sorter, features(300))
    values = list(sorter.encoded())
    assert [pickle.loads(value) for value in values] == python_sorted(
        features(300), key=by_position
    )


def test_encoded_items_skip_the_key_function_and_the_codec(tmp_path: Path) -> None:
    def refuse(_: object) -> bytes:
        raise AssertionError("called")

    codec = cast(
        "Codec[bytes]", cast(object, type("Refusing", (), {"encode": refuse, "decode": refuse})())
    )
    lines = [(("chr2", 5), b"chr2\t5\t9\n"), (("chr1", 7), b"chr1\t7\t8\n"), (("chr1", 2), b"")]
    sorter = Sorter(
        cast("Callable[[bytes], SortKey]", refuse), codec=codec, max_bytes=32, tmp_dir=tmp_path
    )
    for line in lines:
        sorter.add_encoded(*line)
    out = io.BytesIO()
    sorter.write_to(out)
    assert out.getvalue() == b"chr1\t7\t8\nchr2\t5\t9\n"
    assert list(tmp_path.iterdir()) == []


def test_an_encoded_payload_must_be_bytes() -> None:
    sorter: Sorter[int] = Sorter()
    with pytest.raises(TypeError):
        sorter.add_encoded(1, cast("bytes", cast(object, "payload")))
    assert len(sorter) == 0


def test_a_failing_key_or_codec_keeps_the_items_before_it() -> None:
    def key(item: int) -> SortKey:
        if item == 3:
            raise KeyError(item)
        return -item

    sorter = Sorter(key)
    with pytest.raises(KeyError):
        sorter.sort(range(10))
    assert len(sorter) == 3

    class Text:
        def encode(self, item: int, /) -> str:
            return str(item)

        def decode(self, data: bytes, /) -> int:
            return int(data)

    wrong = Sorter(codec=cast("Codec[int]", cast(object, Text())))
    with pytest.raises(TypeError, match="a codec must encode an item into bytes, not str"):
        wrong.add(1)
    assert list(sorter.sort()) == [2, 1, 0]


class Partial:
    """A raw handle that writes at most a few bytes at a time."""

    def __init__(self, most: int) -> None:
        self.most: int = most
        self.written: bytearray = bytearray()

    def write(self, data: bytes, /) -> int:
        self.written += data[: self.most]
        return min(len(data), self.most)


def test_writing_writes_everything_even_to_a_handle_that_writes_part(tmp_path: Path) -> None:
    payloads = [bytes([index % 251]) * (index % 2_000) for index in range(3_000)]
    sorter: Sorter[bytes] = Sorter(max_bytes=1 << 16, tmp_dir=tmp_path)
    for index, payload in enumerate(payloads):
        sorter.add_encoded((len(payload), -index), payload)
    handle = Partial(most=100_000)
    sorter.write_to(handle)
    expected = sorted(enumerate(payloads), key=lambda pair: (len(pair[1]), -pair[0]))
    assert bytes(handle.written) == b"".join(payload for _, payload in expected)
    assert list(tmp_path.iterdir()) == []


def test_writing_to_a_handle_that_fails_raises_and_removes_spilled_files(tmp_path: Path) -> None:
    class Failing:
        def write(self, _data: bytes, /) -> int:
            raise OSError("disk full")

    class Stuck:
        def write(self, _data: bytes, /) -> int:
            return 0

    for handle, message in ((Failing(), "disk full"), (Stuck(), "wrote no bytes")):
        sorter: Sorter[int] = Sorter(max_bytes=64, tmp_dir=tmp_path)
        add_all(sorter, range(1_000))
        with pytest.raises(OSError, match=message):
            sorter.write_to(handle)
        assert list(tmp_path.iterdir()) == []


def test_writing_to_something_without_write_leaves_the_sorter_unread() -> None:
    sorter: Sorter[int] = Sorter()
    sorter.add(1)
    with pytest.raises(AttributeError):
        sorter.write_to(cast("io.BytesIO", object()))
    assert list(sorter.sort()) == [1]


def test_a_sorter_is_read_once() -> None:
    readers: list[Callable[[Sorter[int]], object]] = [
        lambda sorter: list(sorter.sort()),
        lambda sorter: list(sorter.encoded()),
        lambda sorter: sorter.write_to(io.BytesIO()),
    ]
    for first in readers:
        for second in readers:
            sorter: Sorter[int] = Sorter()
            sorter.add(1)
            first(sorter)
            with pytest.raises(RuntimeError, match="read only once"):
                second(sorter)
            with pytest.raises(RuntimeError, match="once it has been read"):
                sorter.add(2)
            with pytest.raises(RuntimeError, match="once it has been read"):
                sorter.add_encoded(2, b"")
            with pytest.raises(RuntimeError, match="once it has been read"):
                sorter.sort([2])
            assert len(sorter) == 1


def test_a_closed_sorter_refuses_everything(tmp_path: Path) -> None:
    sorter: Sorter[int] = Sorter(max_bytes=64, tmp_dir=tmp_path)
    add_all(sorter, range(100))
    sorter.close()
    sorter.close()
    assert list(tmp_path.iterdir()) == []
    assert len(sorter) == 100
    actions: list[Callable[[], object]] = [
        lambda: sorter.add(1),
        lambda: sorter.add_encoded(1, b""),
        sorter.sort,
        lambda: sorter.sort([1]),
        sorter.encoded,
        lambda: sorter.write_to(io.BytesIO()),
    ]
    for action in actions:
        with pytest.raises(ValueError, match="closed sorter"):
            action()


def test_spilled_files_are_removed_however_reading_ends(tmp_path: Path) -> None:
    def filled() -> Sorter[int]:
        sorter: Sorter[int] = Sorter(max_bytes=64, tmp_dir=tmp_path)
        add_all(sorter, range(1_000, 0, -1))
        assert spilled(tmp_path)
        return sorter

    sorter = filled()
    assert sum(1 for _ in sorter.sort()) == 1_000
    assert list(tmp_path.iterdir()) == []

    with filled() as sorter:
        iterator = sorter.sort()
        assert next(iterator) == 1
    assert list(tmp_path.iterdir()) == []
    with pytest.raises(ValueError, match="closed sorter"):
        next(iterator)

    with pytest.raises(KeyError), filled() as sorter:
        raise KeyError
    assert list(tmp_path.iterdir()) == []

    sorter = filled()
    del sorter
    gc.collect()
    assert list(tmp_path.iterdir()) == []


def test_a_decode_error_ends_reading_and_removes_spilled_files(tmp_path: Path) -> None:
    class Failing:
        def encode(self, item: int, /) -> bytes:
            return str(item).encode()

        def decode(self, data: bytes, /) -> int:
            if data == b"500":
                raise ValueError("cannot decode 500")
            return int(data)

    sorter = Sorter(codec=Failing(), max_bytes=256, tmp_dir=tmp_path)
    iterator = sorter.sort(range(1_000, 0, -1))
    assert [next(iterator) for _ in range(499)] == list(range(1, 500))
    with pytest.raises(ValueError, match="cannot decode 500"):
        next(iterator)
    assert next(iterator, None) is None
    assert list(tmp_path.iterdir()) == []


def test_an_iterator_outlives_its_sorter() -> None:
    def numbers() -> Iterator[int]:
        sorter: Sorter[int] = Sorter(max_bytes=64)
        return sorter.sort(range(500, 0, -1))

    iterator = numbers()
    gc.collect()
    assert list(iterator) == list(range(1, 501))
    assert next(iterator, None) is None


def test_a_sorter_in_a_reference_cycle_is_collected(tmp_path: Path) -> None:
    def fill() -> None:
        sorter: Sorter[int]

        def key(item: int) -> SortKey:
            return item + 0 * len(sorter)

        sorter = Sorter(key, max_bytes=64, tmp_dir=tmp_path)
        add_all(sorter, range(1_000))

    fill()
    assert spilled(tmp_path)
    gc.collect()
    assert list(tmp_path.iterdir()) == []


def wait_for_runs(sorter: Sorter[int], tmp_path: Path, count: int) -> None:
    """Add items until `count` runs exist, so that every run before the last is complete."""
    value = 0
    while len(spilled(tmp_path)) < count:
        sorter.add(value)
        value += 1


def test_a_failed_spill_refuses_to_go_on(tmp_path: Path) -> None:
    sorter: Sorter[int] = Sorter(max_bytes=64, tmp_dir=tmp_path)
    wait_for_runs(sorter, tmp_path, 2)
    (directory,) = tmp_path.iterdir()
    while directory.exists():
        shutil.rmtree(directory, ignore_errors=True)
    with pytest.raises(FileNotFoundError):
        for value in range(1_000):
            sorter.add(value)
    with pytest.raises(RuntimeError, match="an earlier error lost records"):
        sorter.add(0)
    with pytest.raises(RuntimeError, match="an earlier error lost records"):
        list(sorter.sort())


def test_spilled_runs_are_compressed(tmp_path: Path) -> None:
    max_bytes = 2**20
    sorter: Sorter[bytes] = Sorter(max_bytes=max_bytes, tmp_dir=tmp_path)
    index = 0
    while len(spilled(tmp_path)) < 3:
        sorter.add_encoded(-index, b"ACGT" * 50 + str(index).encode())
        index += 1
    sizes = [path.stat().st_size for path in spilled(tmp_path)[:2]]
    assert all(0 < size < max_bytes // 8 for size in sizes), sizes
    encoded = list(sorter.encoded())
    assert len(encoded) == index
    assert encoded[0] == b"ACGT" * 50 + str(index - 1).encode()


def test_a_run_that_is_cut_short_is_an_error_when_read(tmp_path: Path) -> None:
    sorter: Sorter[int] = Sorter(max_bytes=2**20, tmp_dir=tmp_path)
    wait_for_runs(sorter, tmp_path, 3)
    first = spilled(tmp_path)[0]
    first.write_bytes(first.read_bytes()[:-1])
    iterator = sorter.sort()
    with pytest.raises(OSError, match="ended before its last record"):
        list(iterator)
    assert list(tmp_path.iterdir()) == []
    assert next(iterator, None) is None


def test_a_missing_run_is_an_error_when_read(tmp_path: Path) -> None:
    sorter: Sorter[int] = Sorter(max_bytes=64, tmp_dir=tmp_path)
    wait_for_runs(sorter, tmp_path, 3)
    first = spilled(tmp_path)[0]
    first.unlink()
    with pytest.raises(FileNotFoundError) as raised:
        sorter.sort()
    assert raised.value.errno == errno.ENOENT
    assert raised.value.filename == str(first)
    assert list(tmp_path.iterdir()) == []
    with pytest.raises(RuntimeError, match="stopped"):
        sorter.sort()


def test_settings_are_checked(tmp_path: Path) -> None:
    for max_bytes in (1, 0, -5):
        with pytest.raises(ValueError, match=f"max_bytes must be at least 2, not {max_bytes}"):
            Sorter(max_bytes=max_bytes)
    with pytest.raises(ValueError, match="max_open must be at least 2, not 1"):
        Sorter(max_open=1)
    with pytest.raises(TypeError, match="key must be a function returning a sort key, not int"):
        Sorter(cast("Callable[[int], SortKey]", 3))
    with pytest.raises(TypeError, match="a codec must have encode and decode methods"):
        Sorter(codec=cast("Codec[int]", object()))
    with pytest.raises(FileNotFoundError, match="missing"):
        Sorter(tmp_dir=tmp_path / "missing")
    (tmp_path / "file").touch()
    with pytest.raises(NotADirectoryError, match="is not a directory"):
        Sorter(tmp_dir=str(tmp_path / "file"))


def test_an_os_error_keeps_its_errno_and_names_its_path(tmp_path: Path) -> None:
    missing = tmp_path / "missing"
    with pytest.raises(FileNotFoundError) as raised:
        Sorter(tmp_dir=missing)
    assert raised.value.errno == errno.ENOENT
    assert raised.value.filename == str(missing)
    assert raised.value.strerror
    assert "os error" not in raised.value.strerror


@pytest.mark.skipif(
    sys.platform == "win32" or os.geteuid() == 0, reason="permissions do not stop writing"
)
def test_a_tmp_dir_that_cannot_be_written_is_refused_when_the_sorter_is_made(
    tmp_path: Path,
) -> None:
    locked = tmp_path / "locked"
    locked.mkdir(mode=0o500)
    try:
        with pytest.raises(PermissionError, match="locked"):
            Sorter(tmp_dir=locked)
    finally:
        locked.chmod(0o700)
    assert list(locked.iterdir()) == []


def test_a_sorter_can_be_subscripted_but_not_pickled() -> None:
    sorter = Sorter[int]()
    assert type(sorter) is Sorter
    assert repr(Sorter[int]) == "spillsort.Sorter[int]"
    with pytest.raises(TypeError, match="pickle"):
        pickle.dumps(sorter)
    with pytest.raises(TypeError):
        sorter.sort(cast("list[int]", cast(object, 3)))


def test_sorting_adds_the_items_given_to_those_added_before(tmp_path: Path) -> None:
    items = features(2_000)
    with Sorter(by_position, max_bytes=512, tmp_dir=tmp_path) as sorter:
        add_all(sorter, items[:500])
        assert list(sorter.sort(items[500:])) == python_sorted(items, key=by_position)
        assert len(sorter) == 2_000


def test_a_sorter_is_sized_but_not_iterable() -> None:
    sorter: Sorter[int] = Sorter()
    assert len(sorter) == 0
    assert not hasattr(sorter, "__iter__")
    with pytest.raises(TypeError):
        iter(cast("Iterable[int]", cast(object, sorter)))


@pytest.mark.parametrize("threads", [1, 2, 4])
def test_items_come_out_the_same_on_any_number_of_threads(tmp_path: Path, threads: int) -> None:
    items = [(index * 7919 % 101, index) for index in range(20_000)]
    sorter = Sorter(itemgetter(0), max_bytes=4_096, tmp_dir=tmp_path, threads=threads)
    assert list(sorter.sort(items)) == python_sorted(items, key=itemgetter(0))


def test_threads_must_be_at_least_one() -> None:
    with pytest.raises(ValueError, match="threads must be at least 1"):
        _ = Sorter[int](threads=0)


def test_spilled_files_go_where_python_puts_temporary_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    sorter: Sorter[int] = Sorter(max_bytes=64)
    add_all(sorter, range(1_000))
    assert spilled(tmp_path)
    assert list(sorter.sort()) == list(range(1_000))
    assert list(tmp_path.iterdir()) == []


def test_threads_are_checked_before_the_tmp_dir(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="threads must be at least 1"):
        _ = Sorter[int](threads=0, tmp_dir=tmp_path / "missing")


def test_adding_takes_its_arguments_by_position() -> None:
    sorter: Sorter[int] = Sorter()
    add: Any = sorter.add
    add_encoded: Any = sorter.add_encoded
    with pytest.raises(TypeError):
        add(item=1)
    with pytest.raises(TypeError):
        add_encoded(key=1, payload=b"")
    assert len(sorter) == 0
