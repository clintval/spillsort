"""Sort more items than fit in memory: sorted runs spill to disk and merge as they are read."""

import importlib.metadata as _metadata

from spillsort._codec import Codec
from spillsort._codec import SortKey
from spillsort._spillsort import Sorter

__version__ = _metadata.version("spillsort")

__all__ = [
    "Codec",
    "SortKey",
    "Sorter",
]
