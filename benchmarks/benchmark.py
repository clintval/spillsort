"""Compare spillsort with a pure-Python external sorter of the same API and with `sorted()`.

Each method runs in its own process, which reports its time and the most memory it used beyond
what it used after importing. The items are random `(refname, start, end, name)` tuples sorted by
reference and start. `items` methods add them and read them back, pickled while they wait.
`lines` methods add `(key, line)` pairs that are already encoded and write the sorted lines to a
file. The `generate` methods only make the items, which is the least any method can take.

Run with `uv run python -m benchmarks.benchmark [records] [max_megabytes]`.
"""

import json
import os
import platform
import random
import resource
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from collections.abc import Iterator
from operator import itemgetter
from pathlib import Path
from typing import Any

import spillsort

from benchmarks import baseline

Item = tuple[str, int, int, str]
Key = tuple[str, int]
by_position: Callable[[Item], Key] = itemgetter(0, 1)
by_key: Callable[[tuple[Key, bytes]], Key] = itemgetter(0)

METHODS = [
    "generate items",
    "sorted() items",
    "baseline items",
    "spillsort items",
    "generate lines",
    "sorted() lines",
    "baseline lines",
    "spillsort lines",
]


def items(count: int) -> Iterator[Item]:
    rng = random.Random(42)
    refnames = [f"chr{number}" for number in range(1, 23)]
    for index in range(count):
        start = rng.randrange(250_000_000)
        yield rng.choice(refnames), start, start + rng.randrange(50, 500), f"read{index}"


def lines(count: int) -> Iterator[tuple[Key, bytes]]:
    for refname, start, end, name in items(count):
        yield (refname, start), f"{refname}\t{start}\t{end}\t{name}\n".encode()


def peak_megabytes() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / 2**20 if platform.system() == "Darwin" else peak / 2**10


def run(method: str, count: int, max_bytes: int, directory: Path) -> None:
    engine: Any = spillsort.Sorter if method.startswith("spillsort") else baseline.Sorter
    out = directory / "out.txt"
    match method.split():
        case ["generate", "items"]:
            for _ in items(count):
                pass
        case ["generate", "lines"]:
            for _ in lines(count):
                pass
        case ["sorted()", "items"]:
            for _ in sorted(items(count), key=by_position):
                pass
        case ["sorted()", "lines"]:
            with out.open("wb") as handle:
                handle.writelines(line for _, line in sorted(lines(count), key=by_key))
        case [_, "items"]:
            sorter = engine(by_position, max_bytes=max_bytes, tmp_dir=directory)
            sorter.extend(items(count))
            for _ in sorter:
                pass
        case [_, "lines"]:
            sorter = engine(max_bytes=max_bytes, tmp_dir=directory)
            sorter.extend_encoded(lines(count))
            with out.open("wb") as handle:
                sorter.write_to(handle)
        case _:
            raise SystemExit(f"unknown method {method!r}")


def measure(method: str, count: int, max_bytes: int) -> None:
    with tempfile.TemporaryDirectory(prefix="spillsort-benchmark-") as directory:
        before = peak_megabytes()
        start = time.perf_counter()
        run(method, count, max_bytes, Path(directory))
        seconds = time.perf_counter() - start
        peak = peak_megabytes() - before
    print(json.dumps({"method": method, "seconds": seconds, "megabytes": peak}))


def main() -> None:
    if sys.argv[1:2] == ["--measure"]:
        measure(sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))
        return
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 2_000_000
    max_megabytes = int(sys.argv[2]) if len(sys.argv) > 2 else 64
    cores = os.cpu_count() or 1
    print(f"{count:,} records, max_bytes of {max_megabytes} MiB, {cores} cores, best of 3\n")
    print(f"| {'Method':<16} | {'Seconds':>7} | {'Peak MB':>7} |")
    print(f"|{'-' * 18}|{'-' * 8}:|{'-' * 8}:|")
    for method in METHODS:
        results: list[dict[str, Any]] = []
        for _ in range(3):
            command = [sys.executable, "-m", "benchmarks.benchmark", "--measure", method]
            command += [str(count), str(max_megabytes * 2**20)]
            output = subprocess.run(command, check=True, capture_output=True, text=True).stdout
            results.append(json.loads(output))
        best = min(results, key=lambda result: result["seconds"])
        print(f"| {method:<16} | {best['seconds']:7.2f} | {best['megabytes']:7.0f} |", flush=True)


if __name__ == "__main__":
    main()
