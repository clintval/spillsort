"""Helpers shared by the tests."""

from collections.abc import Callable
from collections.abc import Iterable
from pathlib import Path
from typing import Any
from typing import TypeVar
from typing import cast

from spillsort import SortKey

T = TypeVar("T")


def python_sorted(items: Iterable[T], key: Callable[[T], SortKey] | None = None) -> list[T]:
    """Sort as Python does, which type checkers cannot check for keys that may be None."""
    values: list[Any] = list(items)
    return sorted(values, key=cast("Any", key))


def spilled(tmp_dir: Path) -> list[Path]:
    """The files a sorter has spilled into its private directory in `tmp_dir`."""
    return sorted(path for directory in tmp_dir.iterdir() for path in directory.iterdir())
