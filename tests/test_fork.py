"""A forked child sorts on threads of its own, whatever its parent sorted on threads before."""

import multiprocessing
from pathlib import Path

import pytest
from spillsort import Sorter

TIMEOUT = 60.0


def sort_spilling(tmp_dir: Path) -> None:
    items = list(range(100_000, 0, -1))
    with Sorter[int](max_bytes=1 << 18, tmp_dir=tmp_dir, threads=2) as sorter:
        assert list(sorter.sort(items)) == sorted(items)


@pytest.mark.skipif(
    "fork" not in multiprocessing.get_all_start_methods(), reason="fork is unavailable"
)
@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")
def test_a_forked_child_sorts_after_its_parent_sorted_on_threads(tmp_path: Path) -> None:
    sort_spilling(tmp_path)
    child = multiprocessing.get_context("fork").Process(target=sort_spilling, args=(tmp_path,))
    child.start()
    child.join(TIMEOUT)
    if child.is_alive():
        child.kill()
        child.join()
        pytest.fail("the forked child hung while sorting")
    assert child.exitcode == 0
    assert list(tmp_path.iterdir()) == []
