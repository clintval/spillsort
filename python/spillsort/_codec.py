from typing import Protocol
from typing import TypeVar

from typing_extensions import TypeAliasType

T = TypeVar("T")

SortKey = TypeAliasType(
    "SortKey", "int | float | str | bytes | None | tuple[SortKey, ...] | list[SortKey]"
)
"""A key to sort by, compared as Python compares it, with values of one kind in each position."""


class Codec(Protocol[T]):
    """Turns an item into bytes, and back, so that a sorter can hold it and spill it to disk."""

    def encode(self, item: T, /) -> bytes:
        """Return the bytes of an item."""
        ...

    def decode(self, data: bytes, /) -> T:
        """Return the item that was encoded into these bytes."""
        ...
