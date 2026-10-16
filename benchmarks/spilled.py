"""Count the bytes a method writes to its temporary directory, by watching the files in it.

A separate process lists the directory now and then and keeps the largest size it has seen of each
file, so it counts every run a sorter writes, including those a merge pass writes and removes, as
long as each lasts until the next look, which a run that waits to be merged does. It is a process
rather than a thread because `pysam.sort` holds the GIL while samtools runs.
"""

import json
import os
import subprocess
import sys
import threading
from pathlib import Path
from types import TracebackType

from typing_extensions import Self

INTERVAL = 0.02


class Spilled:
    """Watches the files in `directory`, except those named in `ignore`, while in a `with` block."""

    def __init__(self, directory: Path, ignore: frozenset[str]) -> None:
        self.command: list[str] = [sys.executable, "-m", "benchmarks.spilled", str(directory)]
        self.command += sorted(ignore)
        self.megabytes: float = 0.0
        self.watcher: subprocess.Popen[str] | None = None

    def __enter__(self) -> Self:
        pipe = subprocess.PIPE
        self.watcher = subprocess.Popen(self.command, stdin=pipe, stdout=pipe, text=True)
        assert self.watcher.stdout is not None
        self.watcher.stdout.readline()
        return self

    def __exit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        assert self.watcher is not None
        output, _ = self.watcher.communicate()
        self.megabytes = json.loads(output)["bytes"] / 1e6


def watch(directory: str, ignore: set[str]) -> None:
    """Looks until stdin closes, then prints the bytes of every file seen, at its largest."""
    sizes: dict[str, int] = {}

    def look() -> None:
        for root, _, names in os.walk(directory):
            for name in names:
                if name not in ignore:
                    path = os.path.join(root, name)
                    try:
                        size = os.stat(path).st_size
                    except FileNotFoundError:
                        continue
                    sizes[path] = max(size, sizes.get(path, 0))

    stopped = threading.Event()

    def wait_for_close() -> None:
        sys.stdin.read()
        stopped.set()

    threading.Thread(target=wait_for_close, daemon=True).start()
    look()
    print("watching", flush=True)
    while not stopped.wait(INTERVAL):
        look()
    look()
    print(json.dumps({"bytes": sum(sizes.values())}))


if __name__ == "__main__":
    watch(sys.argv[1], set(sys.argv[2:]))
