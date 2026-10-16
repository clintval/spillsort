"""A forked child sorts on threads of its own, whatever its parent sorted on threads before."""

import io
import multiprocessing
from pathlib import Path

import pytest
from spillsort import Sorter

from tests.helpers import spilled

TIMEOUT = 60.0


def sort_spilling(tmp_dir: Path, threads: int) -> None:
    items = list(range(100_000, 0, -1))
    with Sorter[int](max_bytes=1 << 18, tmp_dir=tmp_dir, threads=threads) as sorter:
        for item in items:
            sorter.add(item)
        assert spilled(tmp_dir)
        assert list(sorter.sort()) == sorted(items)
    with Sorter(max_bytes=1 << 18, tmp_dir=tmp_dir, threads=threads) as sorter:
        for item in items:
            sorter.add_encoded(item, b"%d\n" % item)
        assert spilled(tmp_dir)
        out = io.BytesIO()
        sorter.write_to(out)
    assert out.getvalue() == b"".join(b"%d\n" % item for item in sorted(items))


@pytest.mark.skipif(
    "fork" not in multiprocessing.get_all_start_methods(), reason="this platform cannot fork"
)
@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")
@pytest.mark.parametrize("parent_threads", [1, 2])
@pytest.mark.parametrize("child_threads", [1, 2])
def test_a_forked_child_sorts_after_its_parent_sorted_on_threads(
    tmp_path: Path, parent_threads: int, child_threads: int
) -> None:
    sort_spilling(tmp_path, parent_threads)
    child = multiprocessing.get_context("fork").Process(
        target=sort_spilling, args=(tmp_path, child_threads)
    )
    child.start()
    child.join(TIMEOUT)
    if child.is_alive():
        child.kill()
        child.join()
        pytest.fail("the forked child hung while sorting")
    assert child.exitcode == 0
    assert list(tmp_path.iterdir()) == []
