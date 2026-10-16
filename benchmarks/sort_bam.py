"""Compare sorting a BAM by coordinate with samtools sort and with spillsort holding pysam records.

Every method reads the same randomly ordered BAM of paired reads and writes a BAM. Each runs in
its own process, which reports its time, the bytes it wrote to temporary files, and the most memory
it used beyond what it used after importing; this includes samtools, since `pysam.sort` runs it in
the same process. The `copy`
methods only read and write, which is the least any method holding pysam records can take. Each
sorted output must index without a sort-order error, every output must hold every input record,
and every sorted output must list the same positions in the same order.

Run with `uv run python -m benchmarks.sort_bam [records] [max_megabytes] [threads]`.
"""

import hashlib
import json
import os
import platform
import random
import resource
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from itertools import accumulate
from itertools import chain
from itertools import islice
from pathlib import Path
from typing import Any

import pysam
import spillsort
from pysam import AlignedSegment
from pysam import AlignmentFile
from pysam import AlignmentHeader
from pysam.version import __samtools_version__ as samtools_version
from pysam.version import __version__ as pysam_version

from benchmarks.spilled import Spilled

METHODS = [
    "copy records",
    "copy text",
    "samtools sort",
    "spillsort text",
    "spillsort codec",
]

REFERENCES = {
    "chr1": 248_956_422,
    "chr2": 242_193_529,
    "chr3": 198_295_559,
    "chr4": 190_214_555,
    "chr5": 181_538_259,
    "chr6": 170_805_979,
    "chr7": 159_345_973,
    "chr8": 145_138_636,
    "chr9": 138_394_717,
    "chr10": 133_797_422,
    "chr11": 135_086_622,
    "chr12": 133_275_309,
    "chr13": 114_364_328,
    "chr14": 107_043_718,
    "chr15": 101_991_189,
    "chr16": 90_338_345,
    "chr17": 83_257_441,
    "chr18": 80_373_285,
    "chr19": 58_617_616,
    "chr20": 64_444_167,
    "chr21": 46_709_983,
    "chr22": 50_818_468,
    "chrX": 156_040_895,
    "chrY": 57_227_415,
    "chrM": 16_569,
}
NAMES = list(REFERENCES)
CUMULATIVE = list(accumulate(REFERENCES.values()))
READ_LENGTH = 150
BASES = b"ACGT" * 64
QUALITIES = b"F" * 200 + b":" * 36 + b"," * 15 + b"#" * 5
UNMAPPED_TAGS = "AS:i:0\tXS:i:0"
HEADER = "".join([
    "@HD\tVN:1.6\tSO:unsorted\n",
    *(f"@SQ\tSN:{name}\tLN:{length}\n" for name, length in REFERENCES.items()),
    "@RG\tID:A\tSM:sample\tLB:library\tPL:ILLUMINA\n",
])
UNPLACED = 2**31

Position = tuple[int, int, bool]


def alignment(rng: random.Random) -> tuple[str, int, int]:
    """A CIGAR, the reference bases it spans, and its edit distance."""
    draw = rng.random()
    mismatches = rng.choices((0, 1, 2, 3), weights=(70, 20, 7, 3))[0]
    if draw < 0.85:
        return f"{READ_LENGTH}M", READ_LENGTH, mismatches
    if draw < 0.93:
        clip = rng.randrange(5, 40)
        kept = READ_LENGTH - clip
        cigar = f"{clip}S{kept}M" if rng.random() < 0.5 else f"{kept}M{clip}S"
        return cigar, kept, mismatches
    at, size = rng.randrange(20, 120), rng.randrange(1, 6)
    if rng.random() < 0.5:
        return f"{at}M{size}I{READ_LENGTH - at - size}M", READ_LENGTH - size, mismatches + size
    return f"{at}M{size}D{READ_LENGTH - at}M", READ_LENGTH + size, mismatches + size


def mapping_quality(rng: random.Random) -> int:
    return 60 if rng.random() < 0.9 else rng.randrange(60)


def tags(rng: random.Random, edits: int, mate_cigar: str | None = None) -> str:
    score = READ_LENGTH - 5 * edits
    mate = "" if mate_cigar is None else f"MC:Z:{mate_cigar}\t"
    return f"NM:i:{edits}\t{mate}AS:i:{score}\tXS:i:{rng.randrange(score)}"


def sam(
    rng: random.Random, name: str, flag: int, place: str, cigar: str, mate: str, extra: str
) -> str:
    sequence = rng.randbytes(READ_LENGTH).translate(BASES).decode()
    qualities = rng.randbytes(READ_LENGTH).translate(QUALITIES).decode()
    return f"{name}\t{flag}\t{place}\t{cigar}\t{mate}\t{sequence}\t{qualities}\t{extra}\tRG:Z:A"


def pair(rng: random.Random, index: int) -> tuple[str, str]:
    """Two mates as SAM lines, read 1 first: both unmapped, one unmapped, or a proper pair."""
    name = f"A00123:8:HFHJKDSXY:1:{1101 + index // 50_000}:{index % 50_000}:{rng.randrange(37_000)}"
    draw = rng.random()
    if draw < 0.01:
        first = sam(rng, name, 77, "*\t0\t0", "*", "*\t0\t0", UNMAPPED_TAGS)
        return first, sam(rng, name, 141, "*\t0\t0", "*", "*\t0\t0", UNMAPPED_TAGS)
    reference = rng.choices(NAMES, cum_weights=CUMULATIVE)[0]
    left = rng.randrange(1, REFERENCES[reference] - 1000)
    cigar, _, edits = alignment(rng)
    if draw < 0.03:
        reverse = 16 * (rng.random() < 0.5)
        placed_first = rng.random() < 0.5
        place, mate = f"{reference}\t{left}", f"=\t{left}\t0"
        flag = 9 + reverse + (64 if placed_first else 128)
        placed = sam(
            rng, name, flag, f"{place}\t{mapping_quality(rng)}", cigar, mate, tags(rng, edits)
        )
        flag = 5 + 2 * reverse + (128 if placed_first else 64)
        unplaced = sam(rng, name, flag, f"{place}\t0", "*", mate, UNMAPPED_TAGS)
        return (placed, unplaced) if placed_first else (unplaced, placed)
    mate_cigar, mate_span, mate_edits = alignment(rng)
    insert = min(max(round(rng.gauss(350, 50)), 200), 800)
    right = left + insert - mate_span
    first_is_left = rng.random() < 0.5
    place = f"{reference}\t{left}\t{mapping_quality(rng)}"
    flag, mate = 99 if first_is_left else 163, f"=\t{right}\t{insert}"
    left_read = sam(rng, name, flag, place, cigar, mate, tags(rng, edits, mate_cigar))
    place = f"{reference}\t{right}\t{mapping_quality(rng)}"
    flag, mate = 147 if first_is_left else 83, f"=\t{left}\t{-insert}"
    right_read = sam(rng, name, flag, place, mate_cigar, mate, tags(rng, mate_edits, cigar))
    return (left_read, right_read) if first_is_left else (right_read, left_read)


def lines(count: int) -> Iterator[str]:
    rng = random.Random(42)
    pairs = (pair(rng, index) for index in range((count + 1) // 2))
    return islice(chain.from_iterable(pairs), count)


def write_input(path: Path, count: int) -> None:
    header = AlignmentHeader.from_text(HEADER)
    partial = path.with_suffix(".partial")
    with AlignmentFile(str(partial), "wb", header=header, threads=4) as out:
        for line in lines(count):
            out.write(AlignedSegment.fromstring(line, header))
    partial.replace(path)


def position(read: AlignedSegment) -> Position:
    """The order of samtools sort: reference, then start, then forward before reverse."""
    reference = read.reference_id
    return reference if reference >= 0 else UNPLACED, read.reference_start, read.is_reverse


class SamText:
    """Encodes a pysam record as a SAM line, and decodes it against a header."""

    def __init__(self, header: AlignmentHeader) -> None:
        self.header: AlignmentHeader = header

    def encode(self, read: AlignedSegment, /) -> bytes:
        return read.to_string().encode()

    def decode(self, data: bytes, /) -> AlignedSegment:
        return AlignedSegment.fromstring(data.decode(), self.header)


def coordinate_header(header: AlignmentHeader) -> AlignmentHeader:
    rest = [line for line in str(header).splitlines() if not line.startswith("@HD")]
    return AlignmentHeader.from_text("\n".join(["@HD\tVN:1.6\tSO:coordinate", *rest, ""]))


def run(method: str, threads: int, max_bytes: int, source: Path, directory: Path) -> Path:
    out = directory / "out.bam"
    if method == "samtools sort":
        megabytes = max(1, max_bytes // threads // 2**20)
        options = ["-@", str(threads), "-m", f"{megabytes}M", "-T", str(directory / "samtools")]
        pysam.sort("-o", str(out), *options, str(source))
        return out
    with AlignmentFile(str(source), "rb", threads=threads) as bam:
        header = bam.header if method.startswith("copy") else coordinate_header(bam.header)
        with AlignmentFile(str(out), "wb", header=header, threads=threads) as sink:
            write, parse = sink.write, AlignedSegment.fromstring
            match method:
                case "copy records":
                    for read in bam:
                        write(read)
                case "copy text":
                    for read in bam:
                        write(parse(read.to_string().encode().decode(), header))
                case "spillsort text":
                    sorter: spillsort.Sorter[AlignedSegment] = spillsort.Sorter(
                        max_bytes=max_bytes, tmp_dir=directory, threads=threads
                    )
                    add = sorter.add_encoded
                    for read in bam:
                        add(position(read), read.to_string().encode())
                    for text in sorter.encoded():
                        write(parse(text.decode(), header))
                case "spillsort codec":
                    codec = SamText(header)
                    sorter = spillsort.Sorter(
                        position,
                        codec=codec,
                        max_bytes=max_bytes,
                        tmp_dir=directory,
                        threads=threads,
                    )
                    for read in sorter.sort(bam):
                        write(read)
                case _:
                    raise SystemExit(f"unknown method {method!r}")
    return out


def digests(path: Path, indexed: bool) -> dict[str, Any]:
    """Count the records, and digest their positions and their order, after indexing if sorted."""
    if indexed:
        pysam.index(str(path))
    positions, records, count = hashlib.sha256(), hashlib.sha256(), 0
    with AlignmentFile(str(path), "rb") as bam:
        for read in bam:
            place = f"{read.reference_id}\t{read.reference_start}\n".encode()
            positions.update(place)
            records.update(f"{read.query_name}\t{read.flag}\t".encode() + place)
            count += 1
    if not indexed:
        return {"records": count, "positions": None, "order": None}
    return {"records": count, "positions": positions.hexdigest(), "order": records.hexdigest()}


def peak_megabytes() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / 2**20 if platform.system() == "Darwin" else peak / 2**10


def measure(method: str, threads: int, max_bytes: int, source: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="spillsort-benchmark-") as directory:
        before = peak_megabytes()
        with Spilled(Path(directory), frozenset({"out.bam"})) as spilled:
            start = time.perf_counter()
            out = run(method, threads, max_bytes, source, Path(directory))
            seconds = time.perf_counter() - start
        peak = peak_megabytes() - before
        checked = digests(out, indexed=not method.startswith("copy"))
    result = {"method": method, "seconds": seconds, "megabytes": peak}
    print(json.dumps({**result, "spilled": spilled.megabytes, **checked}))


def main() -> None:
    if sys.argv[1:2] == ["--measure"]:
        measure(sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), Path(sys.argv[5]))
        return
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
    max_megabytes = int(sys.argv[2]) if len(sys.argv) > 2 else 256
    thread_counts = sorted({1, int(sys.argv[3]) if len(sys.argv) > 3 else 8})
    source = Path(tempfile.gettempdir()) / f"spillsort-benchmark-{count}.bam"
    if not source.exists():
        print(f"Writing {count:,} records to {source}", flush=True)
        write_input(source, count)
    cores, load = os.cpu_count() or 1, os.getloadavg()[0]
    versions = f"pysam {pysam_version} with samtools {samtools_version}"
    print(f"{count:,} records, max_bytes of {max_megabytes} MiB, {cores} cores, best of 3")
    print(f"Python {platform.python_version()} on {platform.machine()}, {versions}", end="")
    print(f", load average {load:.1f}\n")
    print(f"| {'Method':<15} | Threads | {'Seconds':>7} | {'Peak MB':>7} | Spilled MB |")
    print(f"|{'-' * 17}|{'-' * 8}:|{'-' * 8}:|{'-' * 8}:|{'-' * 11}:|")
    positions: set[str] = set()
    orders: set[str] = set()
    for method in METHODS:
        for threads in thread_counts:
            results: list[dict[str, Any]] = []
            for _ in range(3):
                command = [sys.executable, "-m", "benchmarks.sort_bam", "--measure", method]
                command += [str(threads), str(max_megabytes * 2**20), str(source)]
                output = subprocess.run(
                    command, check=True, stdout=subprocess.PIPE, text=True
                ).stdout
                results.append(json.loads(output))
            for result in results:
                if result["records"] != count:
                    raise SystemExit(f"{method} wrote {result['records']:,} of {count:,} records")
                if result["positions"] is not None:
                    positions.add(result["positions"])
                    orders.add(result["order"])
            best = min(results, key=lambda result: result["seconds"])
            seconds, megabytes, spilled = best["seconds"], best["megabytes"], best["spilled"]
            row = f"| {method:<15} | {threads:>7} | {seconds:7.2f} | {megabytes:7.0f} |"
            print(f"{row} {spilled:10.0f} |", flush=True)
    if len(positions) != 1:
        raise SystemExit("the sorted outputs do not list the same positions")
    same = "the same records" if len(orders) == 1 else "records that differ among ties"
    print(f"\nEvery output holds {count:,} records; every sorted output indexes and lists the same")
    print(f"positions, with {same}, in the same order.")


if __name__ == "__main__":
    main()
