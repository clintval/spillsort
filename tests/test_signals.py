"""A signal handler that raises stops a sorter that is merging or waiting, and removes its files."""

import io
import signal
from collections import deque
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from types import FrameType
from types import SimpleNamespace
from typing import cast

import pytest
from spillsort import Codec
from spillsort import Sorter

pytestmark = pytest.mark.skipif(not hasattr(signal, "setitimer"), reason="needs setitimer")


class InterruptionError(Exception):
    pass


@contextmanager
def interrupted_after(seconds: float) -> Generator[None]:
    """Raise `InterruptionError` from a signal handler once `seconds` have passed."""

    def handler(_signal: int, _frame: FrameType | None) -> None:
        raise InterruptionError

    previous = signal.signal(signal.SIGALRM, handler)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def test_a_signal_stops_merging_and_removes_what_was_spilled(tmp_path: Path) -> None:
    sorter: Sorter[int] = Sorter(max_bytes=4_096, max_open=2, tmp_dir=tmp_path)
    for value in range(100_000, 0, -1):
        sorter.add_encoded(value, b"")
    with interrupted_after(0.01), pytest.raises(InterruptionError):
        sorter.sort()
    assert list(tmp_path.iterdir()) == []
    with pytest.raises(RuntimeError, match="read only once"):
        sorter.sort()


def test_a_signal_stops_reading_and_ends_it(tmp_path: Path) -> None:
    decoded: list[bytes] = []
    codec = cast("Codec[None]", cast(object, SimpleNamespace(encode=bytes, decode=decoded.append)))
    sorter = Sorter(codec=codec, max_bytes=1 << 20, tmp_dir=tmp_path)
    for value in range(2_000_000, 0, -1):
        sorter.add_encoded(value, b"")
    iterator = sorter.sort()
    with interrupted_after(0.005), pytest.raises(InterruptionError):
        deque(iterator, maxlen=0)
    assert 0 < len(decoded) < 2_000_000
    assert next(iterator, None) is None
    assert list(tmp_path.iterdir()) == []


def test_a_signal_stops_writing_and_removes_what_was_spilled(tmp_path: Path) -> None:
    sorter: Sorter[bytes] = Sorter(max_bytes=1 << 20, tmp_dir=tmp_path)
    for value in range(2_000_000, 0, -1):
        sorter.add_encoded(value, b"0123456789")
    out = io.BytesIO()
    with interrupted_after(0.005), pytest.raises(InterruptionError):
        sorter.write_to(out)
    assert len(out.getvalue()) < 20_000_000
    assert list(tmp_path.iterdir()) == []
