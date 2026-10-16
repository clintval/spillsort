# spillsort

[![Build Status](https://github.com/clintval/spillsort/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/clintval/spillsort/actions/workflows/tests.yml?query=branch%3Amain)
[![Python Versions](https://img.shields.io/badge/python-3.11_|_3.12_|_3.13_|_3.14-blue)](https://github.com/clintval/spillsort)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/clintval/spillsort/blob/main/LICENSE)
[![Language](https://img.shields.io/badge/language-rust-dea588.svg)](https://www.rust-lang.org/)

Sort more items than fit in memory: sorted runs spill to disk and merge as they are read.

Install with pip or uv:

```console
pip install spillsort
```

Or add the Rust crate:

```console
cargo add spillsort
```

## Introduction

This library sorts like `sorted(items, key=...)`, but in a fixed amount of memory.
Each item is keyed and encoded into bytes once, as it is added.
When the memory fills, a background thread sorts it and writes it to a temporary file while you keep adding.
Sorting merges those files with what is still in memory, so items come out in key order, and items with equal keys come out in the order they were added.
The engine is Rust, and it is also a Rust crate that needs no Python.

## Quick Start

Give a key function and sort items, once:

```python
from spillsort import Sorter

reads = [("chr2", 500, "read1"), ("chr1", 900, "read2"), ("chr1", 100, "read3"), ("chr1", 900, "read4")]

with Sorter(lambda read: (read[0], read[1])) as sorter:
    ordered = list(sorter.sort(reads))

assert ordered == [("chr1", 100, "read3"), ("chr1", 900, "read2"), ("chr1", 900, "read4"), ("chr2", 500, "read1")]
```

Add items one at a time as they arrive, bound the memory with `max_bytes`, sort on more threads with `threads`, and choose where to spill with `tmp_dir`:

```python
import random
from tempfile import TemporaryDirectory

numbers = [random.random() for _ in range(100_000)]

with TemporaryDirectory() as tmp_dir, Sorter(max_bytes=2**20, threads=4, tmp_dir=tmp_dir) as sorter:
    for number in numbers:
        sorter.add(number)
    ordered = list(sorter.sort())

assert ordered == sorted(numbers)
```

Items are pickled while they wait unless a codec turns them into bytes and back:

```python
class Line:
    def encode(self, item: str, /) -> bytes:
        return item.encode()

    def decode(self, data: bytes, /) -> str:
        return data.decode()

with Sorter(lambda line: int(line.split("\t")[1]), codec=Line()) as sorter:
    lines = list(sorter.sort(["chr1\t300\n", "chr1\t20\n", "chr1\t1000\n"]))

assert lines == ["chr1\t20\n", "chr1\t300\n", "chr1\t1000\n"]
```

If you already have the bytes, add them with their key, and no Python runs per item.
Write the sorted bytes to any binary file in large chunks, merged on another thread while the file writes:

```python
import io

with Sorter() as sorter:
    sorter.add_encoded(("chr2", 5), b"chr2\t5\t9\n")
    sorter.add_encoded(("chr1", 70), b"chr1\t70\t80\n")
    sorter.add_encoded(("chr1", 8), b"chr1\t8\t9\n")
    out = io.BytesIO()
    sorter.write_to(out)

assert out.getvalue() == b"chr1\t8\t9\nchr1\t70\t80\nchr2\t5\t9\n"
```

From Rust, push keyed byte records and read them back merged:

```rust
use spillsort::{Config, Sorter, key};

fn main() -> Result<(), spillsort::Error> {
    let mut sorter = Sorter::new(Config::default().max_bytes(64 << 20))?;
    for (refname, start) in [("chr2", 5), ("chr1", 70), ("chr1", 8)] {
        let mut sort_key = Vec::new();
        key::push_str(&mut sort_key, refname);
        key::push_int(&mut sort_key, start);
        sorter.push(&sort_key, format!("{refname}\t{start}\n").as_bytes())?;
    }

    let mut out = Vec::new();
    sorter.finish()?.write_values(&mut out)?;
    assert_eq!(out, b"chr1\t8\nchr1\t70\nchr2\t5\n");
    Ok(())
}
```

See the [API documentation](https://docs.rs/spillsort) for the crate.

## Keys

Keys compare exactly as Python compares them, even ints and floats of any size, and NaN sorts last:

```python
with Sorter() as sorter:
    ordered = list(sorter.sort([2**64, float("nan"), 1.5, True, -0.5, 3]))

assert ordered[:-1] == [-0.5, True, 1.5, 3, 2**64]
```

## Development and Testing

See the [contributing guide](https://github.com/clintval/spillsort/blob/main/CONTRIBUTING.md) for more information.
