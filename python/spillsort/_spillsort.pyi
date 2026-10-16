from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
from os import PathLike
from types import TracebackType
from typing import Generic
from typing import Protocol
from typing import TypeVar
from typing import final

from typing_extensions import Self
from typing_extensions import override

from spillsort._codec import Codec
from spillsort._codec import SortKey

__all__ = ["SortedIterator", "Sorter"]

_T = TypeVar("_T")
_T_co = TypeVar("_T_co", covariant=True)

class _Writable(Protocol):
    def write(self, data: bytes, /) -> object: ...

@final
class Sorter(Generic[_T]):
    """Sort more items than fit in memory: add them, then sort them once.

    Each item is turned into a sort key by `key`, or is its own key, and into bytes by `codec`, or
    by pickle, once, as it is added. Keys and bytes, and 16 bytes more for each item, are held in at
    most `max_bytes` of memory, unless one item alone needs more. Once half of that is full, a
    background thread sorts it and writes it to a private directory in `tmp_dir` while the other
    half fills. Sorting merges what was written with what is held, so items come out in key order,
    and items with equal keys come out in the order they were added.

    The private directory is removed when reading ends, on `close()`, when a `with` block exits,
    or when the sorter is garbage collected. Threads sharing a sorter take turns, and a sorter
    must not be shared across a fork: make a new one in the child.

    Ctrl-C, or any signal handler that raises, interrupts waiting for a spill, merging, and
    reading; an interrupted merge or read ends the sort and removes what was spilled.

    Args:
        key: A function returning the sort key of an item: an int, float, str, bytes, None, or a
            tuple or list of them, compared as Python compares them. None sorts items by
            themselves.
        codec: Encodes each item into bytes and decodes it back; pickle if None.
        max_bytes: The most memory to hold keys and encoded items in, counting 16 bytes more for
            each, at least 2.
        tmp_dir: The directory to spill into, or None for `tempfile.gettempdir()`.
        max_open: The most spilled runs to merge at once, at least 2; more are merged in passes.
        threads: The threads that sort each half: with 1, the thread that sorts it, and with more,
            a pool of this sorter's own.

    Raises:
        TypeError: If `key` is not callable, or `codec` lacks `encode` and `decode` methods.
        ValueError: If `max_bytes` or `max_open` is less than 2, or `threads` is less than 1.
        OSError: If `tmp_dir` is not a directory.
    """

    def __init__(
        self,
        key: Callable[[_T], SortKey] | None = None,
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

    def sort(self, items: Iterable[_T] = ..., /) -> SortedIterator[_T]:
        """Add every item given, then return every item added, decoded, in sorted order.

        Items are keyed and encoded as they are added. A sorter can be sorted only once; items
        given before one that fails stay added, and the sorter is not sorted.

        Raises:
            TypeError: If a key is not a sort key, or the codec does not return bytes.
            ValueError: If the sorter is closed; also raised by the iterator once it is.
            RuntimeError: If the sorter has been sorted, or an earlier error lost items.
            OSError: If spilling, or reading what was spilled, fails.
        """

    def encoded(self) -> SortedIterator[bytes]:
        """Return the encoded items, as bytes, in sorted order, decoding none of them."""

    def write_to(self, handle: _Writable, /) -> None:
        """Write every encoded item, in sorted order, to a binary handle, in chunks of about 1 MiB.

        The next chunk is merged on another thread while the handle writes this one. This reads
        the sorter, so it can be done only once.
        """

    def close(self) -> None:
        """Drop every item and remove what was spilled; closing again does nothing."""

    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> None: ...

@final
class SortedIterator(Iterator[_T_co]):
    """The items of a sorter, merged in order as they are read."""

    @override
    def __iter__(self) -> Self: ...
    @override
    def __next__(self) -> _T_co: ...
