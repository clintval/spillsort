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
| `baseline lines`  | `extend_encoded` of the pairs into the pure-Python sorter, then `write_to` a file              |
| `spillsort lines` | `extend_encoded` of the pairs into spillsort, then `write_to` a file                           |

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
