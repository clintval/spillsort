//! A k-way merge of sorted sources, in which records with equal keys come from earlier sources
//! first.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt;
use std::io::{self, Write};
use std::mem;

use tempfile::TempDir;

use crate::run::RunReader;
use crate::sorter::Chunk;
use crate::{Error, Result};

/// A sorted source of records: a run on disk, or the last chunk, still in memory.
pub(crate) enum Source {
    Run(RunReader),
    Memory { chunk: Chunk, next: usize },
}

impl Source {
    fn read_into(&mut self, key: &mut Vec<u8>, value: &mut Vec<u8>) -> io::Result<bool> {
        match self {
            Source::Run(reader) => reader.read_into(key, value),
            Source::Memory { chunk, next } => {
                if *next == chunk.len() {
                    return Ok(false);
                }
                let (record_key, record_value) = chunk.record(*next);
                key.clear();
                key.extend_from_slice(record_key);
                value.clear();
                value.extend_from_slice(record_value);
                *next += 1;
                Ok(true)
            }
        }
    }
}

/// The next record of a source, ordered so that the heap's greatest is the next to merge.
struct Head {
    key: Vec<u8>,
    value: Vec<u8>,
    source: usize,
}

impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .key
            .cmp(&self.key)
            .then(other.source.cmp(&self.source))
    }
}

impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Head {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Head {}

/// The records of a [`Sorter`](crate::Sorter), merged in order from its runs and the records it
/// still held.
///
/// [`Merged::next_record`] lends each record without copying it, and iterating copies each into
/// owned vectors. The runs are removed once the last record is read, or when the merge is
/// dropped. After an error, the runs are removed and no more records are read.
pub struct Merged {
    sources: Vec<Source>,
    heap: BinaryHeap<Head>,
    current: Option<Head>,
    remaining: u64,
    dir: Option<TempDir>,
    failed: bool,
}

impl Merged {
    pub(crate) fn new(sources: Vec<Source>, dir: Option<TempDir>, records: u64) -> Result<Self> {
        let mut merged = Merged {
            heap: BinaryHeap::with_capacity(sources.len()),
            sources,
            current: None,
            remaining: records,
            dir,
            failed: false,
        };
        for source in 0..merged.sources.len() {
            let mut head = Head {
                key: Vec::new(),
                value: Vec::new(),
                source,
            };
            if merged.sources[source].read_into(&mut head.key, &mut head.value)? {
                merged.heap.push(head);
            }
        }
        if merged.heap.is_empty() {
            merged.release();
        }
        Ok(merged)
    }

    /// Returns the next record's key and value, or None once every record has been read.
    pub fn next_record(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        if self.failed {
            return Err(Error::Failed);
        }
        if let Err(error) = self.advance() {
            self.failed = true;
            self.heap.clear();
            self.current = None;
            self.release();
            return Err(error.into());
        }
        Ok(self
            .current
            .as_ref()
            .map(|head| (head.key.as_slice(), head.value.as_slice())))
    }

    /// Writes the value of every record not yet read, in order, and returns how many there were.
    pub fn write_values(&mut self, mut out: impl Write) -> Result<u64> {
        let mut written = 0;
        while let Some((_, value)) = self.next_record()? {
            out.write_all(value)?;
            written += 1;
        }
        out.flush()?;
        Ok(written)
    }

    /// The number of records not yet read.
    pub fn len(&self) -> u64 {
        self.remaining
    }

    /// Returns true once every record has been read.
    pub fn is_empty(&self) -> bool {
        self.remaining == 0
    }

    fn advance(&mut self) -> io::Result<()> {
        let Some(mut head) = self.current.take() else {
            self.pop();
            return Ok(());
        };
        if !self.sources[head.source].read_into(&mut head.key, &mut head.value)? {
            self.pop();
            return Ok(());
        }
        if let Some(mut top) = self.heap.peek_mut()
            && *top > head
        {
            mem::swap(&mut *top, &mut head);
        }
        self.current = Some(head);
        self.remaining -= 1;
        Ok(())
    }

    fn pop(&mut self) {
        self.current = self.heap.pop();
        match self.current {
            Some(_) => self.remaining -= 1,
            None => self.release(),
        }
    }

    /// Closes the runs and removes them.
    fn release(&mut self) {
        self.sources.clear();
        self.dir = None;
    }
}

impl Iterator for Merged {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    /// Returns the next record, copied; after an error, returns None.
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        self.next_record()
            .map(|record| record.map(|(key, value)| (key.to_vec(), value.to_vec())))
            .transpose()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = usize::try_from(self.remaining).unwrap_or(usize::MAX);
        (remaining, Some(remaining))
    }
}

impl fmt::Debug for Merged {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Merged")
            .field("remaining", &self.remaining)
            .field(
                "sources",
                &(self.heap.len() + usize::from(self.current.is_some())),
            )
            .finish_non_exhaustive()
    }
}
