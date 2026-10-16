"""Threads sharing one sorter or one iterator take turns, and nothing is lost or repeated."""

import faulthandler
import functools
import pickle
import sys
import threading
import time
from collections.abc import Callable
from collections.abc import Iterator
from pathlib import Path

import pytest
from spillsort import Sorter
from typing_extensions import override

from tests.helpers import spilled

THREADS = 8
TIMEOUT = 60.0


@pytest.fixture(autouse=True)
def watchdog() -> Iterator[None]:
    """Exit with every thread's traceback if a test hangs, even while a thread holds the GIL."""
    faulthandler.dump_traceback_later(2 * TIMEOUT, exit=True)
    yield
    faulthandler.cancel_dump_traceback_later()


class Worker(threading.Thread):
    """A daemon thread that keeps what its call returns or raises."""

    def __init__(self, call: Callable[[], object]) -> None:
        super().__init__(daemon=True)
        self.call: Callable[[], object] = call
        self.result: object = None
        self.error: BaseException | None = None
        self.start()

    @override
    def run(self) -> None:
        try:
            self.result = self.call()
        except BaseException as error:
            self.error = error

    def finish(self) -> object:
        """Wait for the call and return its result, or raise what it raised."""
        self.join(TIMEOUT)
        assert not self.is_alive(), "the thread did not finish"
        if self.error is not None:
            raise self.error
        return self.result


def concurrently(call: Callable[[int], None]) -> None:
    """Call `call` with each thread number, from that many threads started at once."""
    barrier = threading.Barrier(THREADS, timeout=TIMEOUT)

    def run(number: int) -> None:
        barrier.wait()
        call(number)

    workers = [Worker(functools.partial(run, number)) for number in range(THREADS)]
    for worker in workers:
        worker.finish()


def test_threads_adding_to_one_sorter_lose_nothing(tmp_path: Path) -> None:
    sorter: Sorter[tuple[int, int]] = Sorter(
        lambda item: item[1], max_bytes=4_096, tmp_dir=tmp_path
    )

    def add(number: int) -> None:
        for index in range(0, 5_000, 4):
            sorter.add_encoded(index, pickle.dumps((number, index)))
            sorter.add(*((number, following) for following in range(index + 1, index + 4)))

    concurrently(add)
    assert len(sorter) == THREADS * 5_000
    items = list(sorter.sort())
    assert [index for _, index in items] == sorted(index for _, index in items)
    assert sorted(items) == sorted(
        (number, index) for number in range(THREADS) for index in range(5_000)
    )
    for number in range(THREADS):
        own = [index for thread, index in items if thread == number]
        assert own == list(range(5_000))
    assert list(tmp_path.iterdir()) == []


def test_threads_reading_one_iterator_share_its_items(tmp_path: Path) -> None:
    sorter: Sorter[int] = Sorter(max_bytes=4_096, tmp_dir=tmp_path)
    iterator = sorter.sort(range(40_000, 0, -1))
    parts: list[list[int]] = [[] for _ in range(THREADS)]

    def read(number: int) -> None:
        parts[number] = list(iterator)

    concurrently(read)
    assert sorted(value for part in parts for value in part) == list(range(1, 40_001))
    for part in parts:
        assert part == sorted(part)
    assert list(tmp_path.iterdir()) == []


def test_closing_while_threads_add_refuses_the_rest_and_removes_files(tmp_path: Path) -> None:
    sorter: Sorter[int] = Sorter(max_bytes=4_096, tmp_dir=tmp_path)
    started = threading.Barrier(THREADS + 1, timeout=TIMEOUT)

    added = [0] * THREADS

    def add(number: int) -> None:
        started.wait()
        try:
            for value in range(1_000_000):
                sorter.add(value)
                added[number] += 1
        except ValueError as error:
            assert "closed sorter" in str(error)

    def close() -> None:
        started.wait()
        sorter.close()

    closer = Worker(close)
    concurrently(add)
    closer.finish()
    assert all(count < 1_000_000 for count in added)
    assert list(tmp_path.iterdir()) == []


def test_dropping_a_sorter_lets_other_threads_run(tmp_path: Path) -> None:
    sorter: Sorter[int] = Sorter(max_bytes=1_024, tmp_dir=tmp_path)
    for value in range(20_000):
        sorter.add(value)
    assert len(spilled(tmp_path)) > 500
    stamps: list[float] = []
    stop = threading.Event()

    def tick() -> None:
        while not stop.is_set():
            stamps.append(time.perf_counter())
            time.sleep(0.0001)

    interval = sys.getswitchinterval()
    sys.setswitchinterval(TIMEOUT)
    ticker = Worker(tick)
    try:
        time.sleep(0.01)
        start = time.perf_counter()
        del sorter
        end = time.perf_counter()
    finally:
        stop.set()
        sys.setswitchinterval(interval)
    ticker.finish()
    assert list(tmp_path.iterdir()) == []
    assert any(start < stamp < end for stamp in stamps)
