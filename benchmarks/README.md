# Benchmarks

`benchmark.py` sorts random `(refname, start, end, name)` tuples by reference and start three ways: with spillsort, with the pure-Python external sorter in `baseline.py`, which has the same API and is built on `list.sort`, pickled runs, and `heapq.merge`, and with `sorted()`, which holds everything in memory.

```console
uv run python -m benchmarks.benchmark 2000000 64
```

The arguments are the number of tuples, 2 million by default, and `max_bytes` in MiB for both external sorters, 64 by default.
Each method runs three times, each in its own process, and the table shows the fastest run with its peak memory beyond what the process used after importing.

| Method            | What it does                                                                                   |
|-------------------|------------------------------------------------------------------------------------------------|
| `generate items`  | Makes the tuples and nothing else, the least any `items` method can take                       |
| `sorted() items`  | `sorted(items, key=...)`, then iterates                                                        |
| `baseline items`  | Adds the tuples to the pure-Python sorter, pickled, then reads them back                       |
| `spillsort items` | Adds the tuples to spillsort, pickled, then reads them back                                    |
| `generate lines`  | Makes `(key, line)` pairs and nothing else, the least any `lines` method can take              |
| `sorted() lines`  | Sorts the pairs by key in memory, then writes the lines to a file                              |
| `baseline lines`  | `add_encoded` of each pair into the pure-Python sorter, then `write_to` a file                 |
| `spillsort lines` | `add_encoded` of each pair into spillsort, then `write_to` a file                              |

Results on an Apple M3 Max with 16 cores and Python 3.11, for 2 million tuples:

| Method           | Seconds | Peak MB |
|------------------|--------:|--------:|
| generate items   |    0.89 |       0 |
| sorted() items   |    3.05 |     580 |
| baseline items   |    4.94 |     112 |
| spillsort items  |    2.06 |      98 |
| generate lines   |    1.20 |       0 |
| sorted() lines   |    4.15 |     519 |
| baseline lines   |    4.38 |     117 |
| spillsort lines  |    1.50 |     100 |

And for 10 million tuples, where `sorted()` needs about 3 GB and each external sorter stays under 150 MB:

| Method           | Seconds | Peak MB |
|------------------|--------:|--------:|
| generate items   |    4.41 |       0 |
| sorted() items   |   18.76 |    2909 |
| baseline items   |   25.52 |     143 |
| spillsort items  |   10.23 |     101 |
| generate lines   |    6.04 |       0 |
| sorted() lines   |   29.67 |    2604 |
| baseline lines   |   23.56 |     143 |
| spillsort lines  |    7.74 |     107 |

spillsort is faster than `sorted()`, even while it spills to disk, because it compares encoded keys as bytes and sorts on every core, rather than comparing Python tuples one pair at a time.

## Sorting BAM records

The `sort_bam.py` benchmark sorts a BAM file by coordinate with samtools sort, run through `pysam.sort`, and with spillsort holding each [pysam](https://pysam.readthedocs.io) record as SAM text.
It shows what sorting records held as Python objects costs, not that spillsort can replace samtools: samtools sort is C working on binary records, and it should win.

```console
uv run python -m benchmarks.sort_bam 1000000 256 8
```

The arguments are the number of records, 1 million by default, `max_bytes` in MiB, 256 by default, and a thread count, 8 by default; every method runs on 1 thread and on that many.
A small run, such as `uv run python -m benchmarks.sort_bam 10000 4 4`, takes a few seconds.
The pysam package is not a dependency of spillsort: it is in the `benchmark` dependency group, which `uv sync` installs on every platform but Windows, where pysam has no wheels.

The input is written once with pysam to the temporary directory, named for its number of records, and reused by every run.
Its header lists chr1 to chr22, chrX, chrY, and chrM with their GRCh38 lengths, an order that is not alphabetical, so a key must sort by reference index rather than name.
Its records are pairs of 150 bp reads, read 1 first, at random positions weighted by reference length, so their coordinates are in random order.
Most pairs are proper pairs with `150M` CIGARs, some with soft clips or a short insertion or deletion; 2% have one mate unmapped, placed at its mate's position, and 1% have both mates unmapped, with no reference or position.
Each record has random bases, binned qualities, and `NM`, `MC`, `AS`, `XS`, and `RG` tags.

Each method runs three times, each in its own process, and the table shows the fastest run with its peak memory beyond what the process used after importing, about 32 MB.
Since `pysam.sort` runs samtools in the same process, the peak includes samtools.
Each method reads and writes BAM with the given number of threads decompressing and compressing it, and writes at the default compression level.

| Method            | What it does                                                                                                   |
|-------------------|----------------------------------------------------------------------------------------------------------------|
| `copy records`    | Reads each record with `pysam.AlignmentFile` and writes it, the least any method holding pysam records can take |
| `copy text`       | Also turns each record into SAM text and back, as the spillsort methods do, but does not sort                  |
| `samtools sort`   | Runs `pysam.sort("-o", out, "-@", threads, "-m", f"{max_megabytes // threads}M", "-T", prefix, input)`        |
| `spillsort text`  | Calls `add_encoded(key, read.to_string().encode())` for each record, then writes `AlignedSegment.fromstring()` of each of `encoded()` |
| `spillsort codec` | Sorts with `Sorter(key, codec=...).sort(reads)`, a codec doing the same, so spillsort calls the key and codec of each record |

The `-m` option of samtools sort is memory per thread, so it is `max_bytes` divided by the threads, and at least 1 MiB, and samtools holds about `max_bytes` in all, counted as binary records rather than SAM text.
Its `-@` threads decompress, sort, and compress; spillsort's `threads` sort, and each pysam file has as many again to decompress or compress.
The spillsort methods write a header marked `SO:coordinate`, as samtools does.

The key is `(reference_id, reference_start, is_reverse)`, with a reference past every real one for records with no reference, which is the order of samtools sort.
It compares the reference index, with records that have none last, then the position, then forward before reverse, and keeps ties in the order they were read, as spillsort does.
The key tests whether a record has a reference, not whether it is unmapped, since samtools sorts an unmapped read placed at its mate's position there, with its mate; a key that puts every unmapped read last writes such reads after reads with no reference, which fails to index.
So the outputs agree record for record, not only in position.

Each output is checked once it is timed: each sorted output must index with `pysam.index`, which fails on a sort-order error, each output must hold every record, and every sorted output must list the same sequence of `(reference_id, reference_start)`.
Indexing accepts references in any order as long as each is contiguous, so only that comparison would catch a key that sorts by reference name.
The benchmark also reports whether the sorted outputs list the records, by name and flag, in the same order; they did in every run below.

Results on an Apple M3 Max with 16 cores and Python 3.11.12 on arm64, with pysam 0.24.1 and samtools 1.24, for 1 million records, while other work kept the load average at about 6 to 9, so times vary by several percent between runs:

| Method          | Threads | Seconds | Peak MB |
|-----------------|--------:|--------:|--------:|
| copy records    |       1 |   15.22 |       1 |
| copy records    |       8 |    2.18 |      10 |
| copy text       |       1 |   16.17 |       1 |
| copy text       |       8 |    2.18 |      10 |
| samtools sort   |       1 |   18.00 |     305 |
| samtools sort   |       8 |    2.30 |     310 |
| spillsort text  |       1 |   16.63 |     335 |
| spillsort text  |       8 |    2.93 |     345 |
| spillsort codec |       1 |   16.70 |     335 |
| spillsort codec |       8 |    2.94 |     345 |

And for 5 million records:

| Method          | Threads | Seconds | Peak MB |
|-----------------|--------:|--------:|--------:|
| copy records    |       1 |   76.57 |       1 |
| copy records    |       8 |   10.81 |      13 |
| copy text       |       1 |   81.16 |       1 |
| copy text       |       8 |   10.84 |      11 |
| samtools sort   |       1 |   91.95 |     309 |
| samtools sort   |       8 |   12.00 |     344 |
| spillsort text  |       1 |   81.93 |     334 |
| spillsort text  |       8 |   14.31 |     398 |
| spillsort codec |       1 |   82.21 |     335 |
| spillsort codec |       8 |   14.30 |     338 |

On 8 threads, samtools sort takes little more than copying the records, and spillsort takes 20 to 30% longer than samtools sort.
On 1 thread, compressing the output takes most of every method's time, and samtools sort is the slowest: it compresses each run it spills and decompresses it to merge, while spillsort spills runs uncompressed.
Sorting with a key function and a codec takes as long as calling `add_encoded` in a Python loop.
Both sorters peak 40 to 130 MB above the 268 MB (256 MiB) they were given, and the peak varies by tens of MB between runs.
A Python program that makes or changes records would otherwise write them unsorted and sort them with samtools, paying for a copy and a sort; spillsort sorts them as they are written.
