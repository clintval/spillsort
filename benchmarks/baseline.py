"""A pure-Python external sorter with the API of `spillsort.Sorter`, to compare against.

Each item is keyed and encoded as it is added and held as a `(key, payload)` pair. Once the pairs
hold about `max_bytes`, they are sorted by key with `list.sort` and spilled to a temporary file in
pickled blocks. Reading merges the runs and the pairs still held with `heapq.merge`, which breaks
ties by run, so the sort is stable.
"""

import heapq
import pickle
import shutil
import sys
import tempfile
import weakref
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
from operator import itemgetter
from pathlib import Path
from typing import Any
from typing import Generic
from typing import Protocol
from typing import TypeVar

from spillsort import Codec
from typing_extensions import Self

T = TypeVar("T")

BLOCK = 4096
PAIR_OVERHEAD = sys.getsizeof((None, None)) + 8
first = itemgetter(0)


class Writable(Protocol):
    def write(self, data: bytes, /) -> object: ...


class Pickle:
    @staticmethod
    def encode(item: object, /) -> bytes:
        return pickle.dumps(item, 5)

    @staticmethod
    def decode(data: bytes, /) -> Any:
        return pickle.loads(data)


def identity(item: Any) -> Any:
    return item


class Sorter(Generic[T]):
    def __init__(
        self,
        key: Callable[[T], Any] | None = None,
        /,
        *,
        codec: Codec[T] | None = None,
        max_bytes: int = 256 * 2**20,
        tmp_dir: str | Path | None = None,
        max_open: int = 64,
    ) -> None:
        self.key: Callable[[T], Any] = identity if key is None else key
        self.codec: Codec[T] = Pickle() if codec is None else codec
        self.max_bytes: int = max_bytes
        self.tmp_dir: str | Path | None = tmp_dir
        self.max_open: int = max_open
        self.pairs: list[tuple[Any, bytes]] = []
        self.held: int = 0
        self.runs: list[Path] = []
        self.made: int = 0
        self.added: int = 0
        self.directory: Path | None = None
        self.finalizer: weakref.finalize[Any, Any] | None = None
        self.done: bool = False

    def add(self, item: T, /) -> None:
        self.add_encoded(self.key(item), self.codec.encode(item))

    def add_encoded(self, key: Any, payload: bytes, /) -> None:
        if self.done:
            raise RuntimeError("cannot add to a sorter once it has been read")
        self.pairs.append((key, payload))
        self.added += 1
        self.held += len(payload) + sys.getsizeof(key) + PAIR_OVERHEAD
        if self.held >= self.max_bytes:
            self.spill()

    def __len__(self) -> int:
        return self.added

    def sort(self, items: Iterable[T] = (), /) -> Iterator[T]:
        key, encode, add = self.key, self.codec.encode, self.add_encoded
        for item in items:
            add(key(item), encode(item))
        return map(self.codec.decode, self.encoded())

    def encoded(self) -> Iterator[bytes]:
        for _, payload in self.merged():
            yield payload

    def write_to(self, handle: Writable, /) -> None:
        chunk: list[bytes] = []
        size = 0
        for _, payload in self.merged():
            chunk.append(payload)
            size += len(payload)
            if size >= 2**20:
                handle.write(b"".join(chunk))
                chunk, size = [], 0
        if chunk:
            handle.write(b"".join(chunk))

    def close(self) -> None:
        self.done = True
        self.pairs = []
        if self.finalizer is not None:
            self.finalizer()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def sorted_pairs(self) -> list[tuple[Any, bytes]]:
        pairs, self.pairs, self.held = self.pairs, [], 0
        pairs.sort(key=first)
        return pairs

    def new_run(self) -> Path:
        if self.directory is None:
            self.directory = Path(tempfile.mkdtemp(prefix="baseline-", dir=self.tmp_dir))
            self.finalizer = weakref.finalize(self, shutil.rmtree, self.directory, True)
        path = self.directory / f"run-{self.made:06d}"
        self.made += 1
        return path

    @staticmethod
    def write_run(path: Path, pairs: Iterable[tuple[Any, bytes]]) -> None:
        with path.open("wb", buffering=2**20) as handle:
            pairs = iter(pairs)
            while block := [pair for _, pair in zip(range(BLOCK), pairs, strict=False)]:
                pickle.dump(block, handle, pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def read_run(path: Path) -> Iterator[tuple[Any, bytes]]:
        with path.open("rb", buffering=2**20) as handle:
            while True:
                try:
                    block: list[tuple[Any, bytes]] = pickle.load(handle)
                except EOFError:
                    return
                yield from block

    def spill(self) -> None:
        path = self.new_run()
        self.write_run(path, self.sorted_pairs())
        self.runs.append(path)

    def reduce_runs(self) -> None:
        while len(self.runs) > self.max_open:
            runs, self.runs = self.runs, []
            for start in range(0, len(runs), self.max_open):
                group = runs[start : start + self.max_open]
                if len(group) == 1:
                    self.runs.extend(group)
                    continue
                path = self.new_run()
                self.write_run(path, heapq.merge(*map(self.read_run, group), key=first))
                for old in group:
                    old.unlink()
                self.runs.append(path)

    def merged(self) -> Iterator[tuple[Any, bytes]]:
        if self.done:
            raise RuntimeError("a sorter can be read only once")
        pairs = self.sorted_pairs()
        self.done = True
        try:
            if not self.runs:
                yield from pairs
                return
            self.reduce_runs()
            yield from heapq.merge(*map(self.read_run, self.runs), pairs, key=first)
        finally:
            self.close()
