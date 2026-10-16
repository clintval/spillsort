from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
from os import PathLike
from types import TracebackType
from typing import Generic
from typing import Protocol
from typing import final
from typing import overload

from typing_extensions import Self
from typing_extensions import TypeVar

from spillsort._codec import Codec
from spillsort._codec import SortKey

__all__ = ["Sorter"]

_T = TypeVar("_T", default=SortKey)

class _Writable(Protocol):
    def write(self, data: bytes, /) -> object: ...

@final
class Sorter(Generic[_T]):
    """Sort more items than fit in memory: add them, then sort them once.

    Each item is turned into a sort key by `key`, or is its own key, and into bytes by `codec`, or
    by pickle, once, as it is added. Keys and bytes, and 16 bytes more for each item, are held in at
    most `max_bytes` of memory, unless one item alone needs more. Once half of that is full, a
    background thread sorts it and writes it, compressed with LZ4, to a private directory in
    `tmp_dir` while the other half fills. Sorting merges what was written with what is held, so
    items come out in key order, and items with equal keys come out in the order they were
    added. A merge reads at most `max_open` runs at once, each a block of up to 128 KiB at a
    time, holding the block and room for it compressed, about 269 KiB, and its next item, so it
    takes about `max_open` times the largest item, plus those blocks, besides what is held.

    The private directory is removed when reading ends, on `close()`, when a `with` block exits,
    or when the sorter is garbage collected. Threads sharing a sorter take turns, and a sorter
    must not be shared across a fork: make a new one in the child.

    Ctrl-C, or any signal handler that raises, interrupts waiting for a spill, merging, and
    reading; an interrupted merge or read ends the sort and removes what was spilled, which can take
    a moment when much was spilled, and the sorter then refuses to be read again.

    Args:
        key: A function returning the sort key of an item: an int, float, str, bytes, None, or a
            tuple or list of them, compared as Python compares them. None sorts items by
            themselves. NumPy integers, like anything with `__index__`, are ints, and
            `np.float64`, `np.str_` and `np.bytes_` are a float, a str and bytes; other NumPy
            scalars, such as `np.float32` and `np.bool_`, raise TypeError, so convert them first.
        codec: Encodes each item into bytes and decodes it back; pickle if None.
        max_bytes: The most memory to hold keys and encoded items in, counting 16 bytes more for
            each, at least 2.
        tmp_dir: The directory to spill into, which must be writable, or None for
            `tempfile.gettempdir()`.
        max_open: The most spilled runs to merge at once, at least 2; more are merged in passes.
        threads: The threads that sort each half and compress each run: with 1, the thread that
            spills it, and with more, a pool of this sorter's own.

    Raises:
        TypeError: If `key` is not callable, or `codec` lacks `encode` and `decode` methods.
        ValueError: If `max_bytes` or `max_open` is less than 2, or `threads` is less than 1.
        OSError: If `tmp_dir` is not a directory, or a directory cannot be made in it.
    """

    @overload
    def __init__(
        self,
        key: None = None,
        *,
        codec: Codec[_T] | None = None,
        max_bytes: int = 268_435_456,
        tmp_dir: str | PathLike[str] | None = None,
        max_open: int = 64,
        threads: int = 1,
    ) -> None: ...
    @overload
    def __init__(
        self,
        key: Callable[[_T], SortKey],
        *,
        codec: Codec[_T] | None = None,
        max_bytes: int = 268_435_456,
        tmp_dir: str | PathLike[str] | None = None,
        max_open: int = 64,
        threads: int = 1,
    ) -> None: ...
    def add(self, item: _T, /) -> None:
        """Add an item, keyed and encoded now.

        Raises:
            TypeError: If the key is not a sort key, or the codec does not return bytes.
            ValueError: If the sorter is closed.
            RuntimeError: If the sorter has been read, or an earlier error lost items.
            OSError: If spilling to disk fails.
        """

    def add_encoded(self, key: SortKey, payload: bytes, /) -> None:
        """Add bytes that the codec would decode, with their sort key, calling neither."""

    def __len__(self) -> int:
        """The number of items added."""

    def sort(self, items: Iterable[_T] = ..., /) -> Iterator[_T]:
        """Add every item given, then return every item added, decoded, in sorted order.

        Items are keyed and encoded as they are added. A sorter can be sorted only once; items
        given before one that fails stay added, and the sorter is not sorted. Once the iterator
        raises, as when decoding an item fails, it ends, as a generator does.

        Raises:
            TypeError: If a key is not a sort key, or the codec does not return bytes.
            ValueError: If the sorter is closed; also raised by the iterator once it is.
            RuntimeError: If the sorter has been sorted, or an earlier error lost items.
            OSError: If spilling, or reading what was spilled, fails.
        """

    def encoded(self) -> Iterator[bytes]:
        """Return the encoded items, as bytes, in sorted order, decoding none of them."""

    def write_to(self, handle: _Writable, /) -> None:
        """Write every encoded item, in sorted order, to a binary handle, in chunks of about 1 MiB.

        The next chunk is merged on another thread while the handle writes this one. This reads
        the sorter, so it can be done only once.
        """

    def close(self) -> None:
        """Drop every item and remove what was spilled; closing again does nothing.

        A `write_to` already running in another thread is not stopped: it writes every item, then
        removes what was spilled.
        """

    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> None: ...
