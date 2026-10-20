//! Sorting records in bounded memory: a full chunk is sorted and written as a run by a background
//! thread while the next chunk fills, and finishing merges the runs with the last chunk.

use std::fmt;
use std::fs;
use std::io;
use std::mem;
use std::panic;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::RecvTimeoutError;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use rayon::slice::ParallelSliceMut;
use rayon::{ThreadPool, ThreadPoolBuilder};
use tempfile::TempDir;

use crate::channel::{self, Receiver};
use crate::merge::{Merged, Source};
use crate::run::{Run, RunReader, RunWriter};
use crate::{Error, Result};

/// The default for [`Config::max_bytes`]: 256 MiB.
pub const DEFAULT_MAX_BYTES: usize = 256 << 20;

/// The default for [`Config::max_open`].
pub const DEFAULT_MAX_OPEN: usize = 64;

/// The least [`Config::max_bytes`] may be.
pub const MIN_MAX_BYTES: usize = 2;

/// The least [`Config::max_open`] may be.
pub const MIN_MAX_OPEN: usize = 2;

/// The default for [`Config::threads`].
pub const DEFAULT_THREADS: usize = 1;

/// The bytes each record costs beyond its key and value: where it is and how long its parts are.
const ENTRY_BYTES: usize = mem::size_of::<Entry>();

/// The least capacity a chunk's arena grows to, unless the memory limit is smaller.
const MIN_ARENA: usize = 64 << 10;

/// The fewest records a chunk makes room for, unless the memory limit is smaller.
const MIN_ENTRIES: usize = 1 << 10;

/// How long a wait goes on before asking whether to stop.
pub(crate) const POLL: Duration = Duration::from_millis(50);

/// How many records a merge pass writes between asking whether to stop.
const RECORDS_PER_ASK: u64 = 1 << 16;

#[derive(Clone, Copy)]
struct Entry {
    offset: usize,
    key_len: u32,
    value_len: u32,
}

/// How a chunk makes room for one more record.
enum Room {
    /// It has room already.
    Ready,
    /// It has room once its arena and its entries have these capacities.
    Resize { arena: usize, entries: usize },
    /// It is full, so it must be spilled first.
    Full,
}

/// Records held in memory, their keys and values side by side in one arena.
#[derive(Default)]
pub(crate) struct Chunk {
    arena: Vec<u8>,
    entries: Vec<Entry>,
}

impl Chunk {
    /// The bytes this chunk has allocated: for keys and values, and for the place of each record.
    fn capacity_bytes(&self) -> usize {
        self.arena.capacity() + self.entries.capacity() * ENTRY_BYTES
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// How to make room for a record of `bytes` bytes without allocating more than `limit`: by
    /// doubling what is short, or, once that would not fit, by sharing `limit` in proportion to
    /// what each part holds, unless the chunk is nearly full. An empty chunk always takes one
    /// record, however large, and a chunk holding one larger than `limit` takes no more.
    fn room(&self, bytes: usize, limit: usize) -> Room {
        let arena = self.arena.len() + bytes;
        let entries = self.entries.len() + 1;
        let oversized = self.capacity_bytes() > limit && !self.is_empty();
        if arena <= self.arena.capacity() && entries <= self.entries.capacity() && !oversized {
            return Room::Ready;
        }
        let grown = |needed: usize, capacity: usize, least: usize| {
            if needed > capacity {
                (capacity * 2).max(least).max(needed)
            } else {
                capacity
            }
        };
        let doubled = (
            grown(arena, self.arena.capacity(), MIN_ARENA),
            grown(entries, self.entries.capacity(), MIN_ENTRIES),
        );
        if doubled.0 + doubled.1 * ENTRY_BYTES <= limit {
            return Room::Resize {
                arena: doubled.0,
                entries: doubled.1,
            };
        }
        if self.is_empty() {
            return Room::Resize {
                arena: arena.max(self.arena.capacity()),
                entries: entries.max(self.entries.capacity()),
            };
        }
        let used = arena + entries * ENTRY_BYTES;
        if used > limit - limit / 8 {
            return Room::Full;
        }
        let shared = (arena as u128 * limit as u128 / used as u128) as usize;
        Room::Resize {
            arena: shared,
            entries: (limit - shared) / ENTRY_BYTES,
        }
    }

    /// Makes room for a record of `bytes` bytes within `limit`, or returns false if it is full.
    fn make_room(&mut self, bytes: usize, limit: usize) -> bool {
        match self.room(bytes, limit) {
            Room::Ready => true,
            Room::Resize { arena, entries } => {
                resize(&mut self.arena, arena);
                resize(&mut self.entries, entries);
                true
            }
            Room::Full => false,
        }
    }

    /// Adds a record, in the room made for it.
    fn push(&mut self, key: &[u8], value: &[u8]) {
        self.entries.push(Entry {
            offset: self.arena.len(),
            key_len: key.len() as u32,
            value_len: value.len() as u32,
        });
        self.arena.extend_from_slice(key);
        self.arena.extend_from_slice(value);
    }

    pub fn record(&self, index: usize) -> (&[u8], &[u8]) {
        let entry = self.entries[index];
        let middle = entry.offset + entry.key_len as usize;
        (
            &self.arena[entry.offset..middle],
            &self.arena[middle..middle + entry.value_len as usize],
        )
    }

    /// Sorts by key, on a pool's threads if given, in place, keeping records with equal keys in
    /// the order pushed: those come in the order of their offsets, and of an empty record and the
    /// record pushed after it at the same offset, the empty one has the shorter value.
    fn sort(&mut self, pool: Option<&ThreadPool>) {
        let arena = &self.arena;
        let entries = &mut self.entries;
        let key = |entry: &Entry| &arena[entry.offset..entry.offset + entry.key_len as usize];
        let order = |a: &Entry, b: &Entry| {
            key(a)
                .cmp(key(b))
                .then(a.offset.cmp(&b.offset))
                .then(a.value_len.cmp(&b.value_len))
        };
        match pool {
            Some(pool) => pool.install(|| entries.par_sort_unstable_by(order)),
            None => entries.sort_unstable_by(order),
        }
    }

    /// The bytes this chunk's records take in a run, before they are compressed.
    fn run_bytes(&self) -> u64 {
        (self.arena.len() + 8 * self.len()) as u64
    }

    fn write_run(&self, path: PathBuf, pool: Option<Arc<ThreadPool>>) -> io::Result<Run> {
        let mut run = RunWriter::create(path, pool)?;
        for index in 0..self.len() {
            let (key, value) = self.record(index);
            run.push(key, value)?;
        }
        run.finish()
    }

    /// Empties the chunk, keeping what it allocated to fill again unless that is more than
    /// `limit`.
    fn clear(&mut self, limit: usize) {
        if self.capacity_bytes() > limit {
            *self = Chunk::default();
        }
        self.arena.clear();
        self.entries.clear();
    }
}

/// Grows or shrinks a vector to hold exactly `capacity` items, or its length if that is more.
fn resize<T>(vector: &mut Vec<T>, capacity: usize) {
    if capacity > vector.capacity() {
        vector.reserve_exact(capacity - vector.len());
    } else if capacity < vector.capacity() {
        vector.shrink_to(capacity);
    }
}

/// How much a [`Sorter`] holds in memory, where it spills, and how many runs it merges at once.
///
/// ```
/// use spillsort::Config;
///
/// let config = Config::default().max_bytes(64 << 20).max_open(16);
/// assert_eq!(config.max_bytes, 64 << 20);
/// assert_eq!(config.tmp_dir, None);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Config {
    /// The most memory to hold records in, counting what is allocated for their keys and values
    /// and 16 bytes for the place of each. Half fills while the other half is sorted, in place,
    /// and spilled, so neither holds more than half of this, unless one record alone needs more.
    pub max_bytes: usize,
    /// The directory in which a private directory of runs is made, or None for the system's
    /// temporary directory, which is `TMPDIR` on Unix.
    pub tmp_dir: Option<PathBuf>,
    /// The most runs to merge at once. Beyond that, the fewest neighboring runs needed are merged
    /// into one first, as many times as it takes. A merge reads each run a block of up to 128 KiB
    /// at a time, holding the block and room for it compressed, about 269 KiB, and its next
    /// record, so it takes about this many times the largest record, plus those blocks.
    pub max_open: usize,
    /// The threads that sort each chunk and compress each run: with 1, the thread that spills it,
    /// and with more, a pool of this sorter's own. Writing a run holds, beyond `max_bytes`, a 1 MiB
    /// buffer of what it writes and, for each block compressed at once, a 128 KiB block and room
    /// for it compressed, about 269 KiB: one block with 1 thread, and with more, two for each
    /// thread, but at most 16, about 4.2 MiB.
    pub threads: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            max_bytes: DEFAULT_MAX_BYTES,
            tmp_dir: None,
            max_open: DEFAULT_MAX_OPEN,
            threads: DEFAULT_THREADS,
        }
    }
}

impl Config {
    /// Sets [`Config::max_bytes`].
    #[must_use]
    pub fn max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    /// Sets [`Config::tmp_dir`].
    #[must_use]
    pub fn tmp_dir(mut self, tmp_dir: impl Into<PathBuf>) -> Self {
        self.tmp_dir = Some(tmp_dir.into());
        self
    }

    /// Sets [`Config::max_open`].
    #[must_use]
    pub fn max_open(mut self, max_open: usize) -> Self {
        self.max_open = max_open;
        self
    }

    /// Sets [`Config::threads`].
    #[must_use]
    pub fn threads(mut self, threads: usize) -> Self {
        self.threads = threads;
        self
    }
}

/// Sorts records pushed in any order, then [`Sorter::finish`] returns them merged in order.
///
/// Runs are written to a private directory, made at the first spill, which is removed when the
/// [`Merged`] records have all been read, or when the sorter or the merge is dropped. After an
/// error while spilling, records may have been lost, so the sorter refuses to go on with
/// [`Error::Failed`]. A sorter must not be shared across a fork, since its threads stay in the
/// parent: make a new one in the child.
pub struct Sorter {
    config: Config,
    pool: Option<Arc<ThreadPool>>,
    limit: usize,
    chunk: Chunk,
    spare: Option<Chunk>,
    dir: Option<TempDir>,
    runs: Vec<Run>,
    made: usize,
    pending: Option<Pending>,
    count: u64,
    failed: bool,
}

/// A spill in progress: the thread that sorts and writes a chunk, and sends it back emptied.
struct Pending {
    handle: JoinHandle<()>,
    done: Receiver<io::Result<Chunk>>,
}

/// Makes a private directory for runs in `root`, or in the system's temporary directory. An
/// error names the directory it tried to make.
fn make_dir(root: Option<&Path>) -> io::Result<TempDir> {
    let root = root.map_or_else(std::env::temp_dir, Path::to_path_buf);
    tempfile::Builder::new()
        .prefix("spillsort-")
        .tempdir_in(root)
}

/// The error for work given up because the caller asked to stop.
fn stopped() -> Error {
    Error::Io(io::ErrorKind::Interrupted.into())
}

impl Sorter {
    /// Makes a sorter, checking that its settings are valid and that a directory can be made in
    /// its `tmp_dir`, by making one and removing it, so a sorter that never spills leaves nothing.
    pub fn new(config: Config) -> Result<Self> {
        if config.max_bytes < MIN_MAX_BYTES {
            return Err(Error::Invalid(format!(
                "max_bytes must be at least {MIN_MAX_BYTES}, not {}",
                config.max_bytes
            )));
        }
        if config.max_open < MIN_MAX_OPEN {
            return Err(Error::Invalid(format!(
                "max_open must be at least {MIN_MAX_OPEN}, not {}",
                config.max_open
            )));
        }
        if config.threads == 0 {
            return Err(Error::Invalid(
                "threads must be at least 1, not 0".to_owned(),
            ));
        }
        if let Some(dir) = &config.tmp_dir {
            let metadata = fs::metadata(dir).map_err(|error| Error::Path {
                path: dir.clone(),
                error,
            })?;
            if !metadata.is_dir() {
                let message = format!("{} is not a directory", dir.display());
                return Err(io::Error::new(io::ErrorKind::NotADirectory, message).into());
            }
        }
        make_dir(config.tmp_dir.as_deref())?;
        let pool = if config.threads > 1 {
            let pool = ThreadPoolBuilder::new()
                .num_threads(config.threads)
                .thread_name(|index| format!("spillsort-sort-{index}"))
                .build()
                .map_err(|error| Error::Io(io::Error::other(error)))?;
            Some(Arc::new(pool))
        } else {
            None
        };
        Ok(Sorter {
            limit: config.max_bytes / 2,
            pool,
            config,
            chunk: Chunk::default(),
            spare: None,
            dir: None,
            runs: Vec::new(),
            made: 0,
            pending: None,
            count: 0,
            failed: false,
        })
    }

    /// The number of records pushed.
    pub fn len(&self) -> u64 {
        self.count
    }

    /// Returns true if no record has been pushed.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The number of runs spilled to disk so far.
    pub fn runs(&self) -> usize {
        self.runs.len()
    }

    /// Adds a record. When the half of `max_bytes` that fills has no room left for it, that half
    /// is spilled in the background first, after waiting for the spill before it to finish.
    pub fn push(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.failed {
            return Err(Error::Failed);
        }
        if u32::try_from(key.len()).is_err() || u32::try_from(value.len()).is_err() {
            return Err(Error::Invalid(
                "a key or value must be shorter than 4 GiB".to_owned(),
            ));
        }
        if !self.chunk.make_room(key.len() + value.len(), self.limit) {
            self.spill().inspect_err(|_| self.failed = true)?;
            self.chunk.make_room(key.len() + value.len(), self.limit);
        }
        self.chunk.push(key, value);
        self.count += 1;
        Ok(())
    }

    /// Returns true if pushing a record of this many bytes would spill, and so might wait.
    #[cfg(feature = "python")]
    pub(crate) fn would_spill(&self, bytes: usize) -> bool {
        matches!(self.chunk.room(bytes, self.limit), Room::Full)
    }

    /// Stops adding and returns every record, merged in order.
    pub fn finish(self) -> Result<Merged> {
        self.finish_or_stop(&mut || false)
    }

    /// Finishes, asking `stop` now and then while waiting and merging whether to give up, which
    /// drops the sorter and so removes what it spilled.
    pub(crate) fn finish_or_stop(mut self, stop: &mut dyn FnMut() -> bool) -> Result<Merged> {
        if self.failed {
            return Err(Error::Failed);
        }
        self.wait_or_stop(stop)?;
        self.spare = None;
        self.reduce(stop)?;
        let mut chunk = mem::take(&mut self.chunk);
        chunk.sort(self.pool.as_deref());
        let mut sources = self
            .runs
            .iter()
            .map(|run| RunReader::open(run).map(Source::Run))
            .collect::<io::Result<Vec<_>>>()?;
        if !chunk.is_empty() {
            sources.push(Source::Memory { chunk, next: 0 });
        }
        Merged::new(sources, self.dir.take(), self.count)
    }

    fn next_path(&mut self) -> Result<PathBuf> {
        let dir = match &mut self.dir {
            Some(dir) => dir,
            empty => empty.insert(make_dir(self.config.tmp_dir.as_deref())?),
        };
        let path = dir.path().join(format!("run-{:06}", self.made));
        self.made += 1;
        Ok(path)
    }

    /// Waits for the spill in progress, keeping its chunk to fill next.
    fn wait(&mut self) -> Result<()> {
        self.wait_or_stop(&mut || false)
    }

    /// Waits for the spill in progress, asking `stop` now and then whether to give up waiting,
    /// which leaves the spill in progress. A spill that failed lost records, so the sorter fails.
    pub(crate) fn wait_or_stop(&mut self, stop: &mut dyn FnMut() -> bool) -> Result<()> {
        let Some(pending) = &self.pending else {
            return Ok(());
        };
        let received = loop {
            match pending.done.recv_timeout(POLL) {
                Ok(spilled) => break Some(spilled),
                Err(RecvTimeoutError::Timeout) if stop() => return Err(stopped()),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break None,
            }
        };
        if let Some(Pending { handle, .. }) = self.pending.take()
            && let Err(panicked) = handle.join()
        {
            panic::resume_unwind(panicked);
        }
        let spilled = received.unwrap_or_else(|| Err(io::Error::other("a spill stopped")));
        self.spare = Some(spilled.inspect_err(|_| self.failed = true)?);
        Ok(())
    }

    fn spill(&mut self) -> Result<()> {
        self.wait()?;
        let path = self.next_path()?;
        let mut chunk = mem::replace(&mut self.chunk, self.spare.take().unwrap_or_default());
        self.runs.push(Run {
            path: path.clone(),
            records: chunk.len() as u64,
            bytes: chunk.run_bytes(),
        });
        let (pool, limit) = (self.pool.clone(), self.limit);
        let (sender, done) = channel::sync_channel(1);
        let handle = thread::Builder::new()
            .name("spillsort-spill".to_owned())
            .spawn(move || {
                chunk.sort(pool.as_deref());
                let written = chunk.write_run(path, pool).map(|_| {
                    chunk.clear(limit);
                    chunk
                });
                let _ = sender.send(written);
            })?;
        self.pending = Some(Pending { handle, done });
        Ok(())
    }

    /// Merges neighboring runs, so equal keys stay stable, until no more than `max_open` remain,
    /// each time merging only as many as are too many, and the neighbors with the fewest bytes.
    /// Before each merge, and now and then during one, it asks `stop` whether to give up.
    fn reduce(&mut self, stop: &mut dyn FnMut() -> bool) -> Result<()> {
        let max_open = self.config.max_open;
        while self.runs.len() > max_open {
            if stop() {
                return Err(stopped());
            }
            let count = (self.runs.len() - max_open + 1).min(max_open);
            let start = self
                .runs
                .windows(count)
                .map(|group| group.iter().map(|run| run.bytes).sum::<u64>())
                .enumerate()
                .min_by_key(|&(_, bytes)| bytes)
                .map_or(0, |(start, _)| start);
            let group = self.runs[start..start + count].to_vec();
            let sources = group
                .iter()
                .map(|run| RunReader::open(run).map(Source::Run))
                .collect::<io::Result<Vec<_>>>()?;
            let records = group.iter().map(|run| run.records).sum();
            let mut merged = Merged::new(sources, None, records)?;
            let mut out = RunWriter::create(self.next_path()?, self.pool.clone())?;
            while let Some((key, value)) = merged.next_record()? {
                out.push(key, value)?;
                if out.records() % RECORDS_PER_ASK == 0 && stop() {
                    return Err(stopped());
                }
            }
            drop(merged);
            self.runs.splice(start..start + count, [out.finish()?]);
            for run in &group {
                fs::remove_file(&run.path)?;
            }
        }
        Ok(())
    }
}

impl Drop for Sorter {
    fn drop(&mut self) {
        if let Some(Pending { handle, .. }) = self.pending.take() {
            let _ = handle.join();
        }
    }
}

impl fmt::Debug for Sorter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sorter")
            .field("config", &self.config)
            .field("len", &self.count)
            .field("runs", &self.runs.len())
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use proptest::prelude::*;

    use super::*;

    fn config(max_bytes: usize, max_open: usize, tmp_dir: &Path) -> Config {
        Config::default()
            .max_bytes(max_bytes)
            .max_open(max_open)
            .tmp_dir(tmp_dir)
    }

    fn entries(path: &Path) -> usize {
        fs::read_dir(path).unwrap().count()
    }

    fn sorted_by_sorter(
        records: &[(u8, u16)],
        max_bytes: usize,
        max_open: usize,
    ) -> Vec<(u8, u16)> {
        let root = tempfile::tempdir().unwrap();
        let mut sorter = Sorter::new(config(max_bytes, max_open, root.path())).unwrap();
        for (key, value) in records {
            sorter.push(&[*key], &value.to_le_bytes()).unwrap();
        }
        assert_eq!(sorter.len(), records.len() as u64);
        let merged = sorter.finish().unwrap();
        assert_eq!(merged.len(), records.len() as u64);
        let sorted = merged
            .map(|record| {
                let (key, value) = record.unwrap();
                (key[0], u16::from_le_bytes([value[0], value[1]]))
            })
            .collect();
        assert_eq!(entries(root.path()), 0);
        sorted
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn sorts_stably_whatever_the_memory(
            records in proptest::collection::vec((0u8..8, any::<u16>()), 0..300),
            max_bytes in 2usize..400,
            max_open in 2usize..5,
        ) {
            let mut expected = records.clone();
            expected.sort_by_key(|record| record.0);
            prop_assert_eq!(sorted_by_sorter(&records, max_bytes, max_open), expected);
        }
    }

    #[test]
    fn checks_threads_before_looking_at_the_tmp_dir() {
        let missing = std::env::temp_dir().join("spillsort-a-directory-that-does-not-exist");
        let error = Sorter::new(Config::default().threads(0).tmp_dir(missing)).unwrap_err();
        assert!(matches!(error, crate::Error::Invalid(_)), "{error}");
    }

    #[test]
    fn merges_many_more_runs_than_it_opens_in_passes() {
        let records: Vec<(u8, u16)> = (0..5_000u16)
            .map(|value| ((value % 7) as u8, value))
            .collect();
        let mut expected = records.clone();
        expected.sort_by_key(|record| record.0);
        assert_eq!(sorted_by_sorter(&records, 64, 3), expected);
    }

    #[test]
    fn sorts_stably_through_large_runs_and_merge_passes() {
        let mut expected = Vec::new();
        for index in 0u32..100_000 {
            let key = index.wrapping_mul(2_654_435_761) % 5_000;
            let value = format!("chr{}\t{key}\t{index}", index % 22);
            expected.push((key.to_be_bytes().to_vec(), value.into_bytes()));
        }
        let mut sorted = expected.clone();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        for threads in [1, 4] {
            let root = tempfile::tempdir().unwrap();
            let config = config(1 << 20, 2, root.path()).threads(threads);
            let mut sorter = Sorter::new(config).unwrap();
            for (key, value) in &expected {
                sorter.push(key, value).unwrap();
            }
            assert!(sorter.runs() > 4);
            let mut merged = sorter.finish().unwrap();
            assert_eq!(entries(root.path()), 1);
            let records: Vec<_> = merged.by_ref().map(Result::unwrap).collect();
            assert!(records == sorted, "{threads} threads");
            assert_eq!(entries(root.path()), 0);
        }
    }

    #[test]
    fn sorts_records_larger_than_a_block_through_runs() {
        let records: Vec<(u8, Vec<u8>)> = (0..24u8)
            .map(|index| {
                let length = (usize::from(index) * 37_000) % 400_000;
                (index % 5, vec![index; length])
            })
            .collect();
        let mut expected = records.clone();
        expected.sort_by_key(|record| record.0);
        for max_bytes in [2, 1 << 20] {
            let root = tempfile::tempdir().unwrap();
            let mut sorter = Sorter::new(config(max_bytes, 3, root.path())).unwrap();
            for (key, value) in &records {
                sorter.push(&[*key], value).unwrap();
            }
            assert!(sorter.runs() > 3);
            let sorted: Vec<(u8, Vec<u8>)> = sorter
                .finish()
                .unwrap()
                .map(|record| {
                    let (key, value) = record.unwrap();
                    (key[0], value)
                })
                .collect();
            assert!(sorted == expected, "max_bytes of {max_bytes}");
            assert_eq!(entries(root.path()), 0);
        }
    }

    #[test]
    fn merges_only_as_many_runs_as_are_too_many_to_open() {
        let root = tempfile::tempdir().unwrap();
        let mut sorter = Sorter::new(config(64, 64, root.path())).unwrap();
        let mut value = 0u32;
        while sorter.runs() < 65 {
            sorter.push(&value.to_be_bytes(), b"").unwrap();
            value += 1;
        }
        sorter.wait().unwrap();
        let records = sorter.runs[0].records;
        assert!(sorter.runs.iter().all(|run| run.records == records));
        sorter.reduce(&mut || false).unwrap();
        assert_eq!(sorter.runs(), 64);
        assert_eq!(sorter.made, 66);
        assert_eq!(sorter.runs[0].records, 2 * records);
        assert!(sorter.runs[1..].iter().all(|run| run.records == records));
        assert_eq!(
            entries(
                &root
                    .path()
                    .read_dir()
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap()
                    .path()
            ),
            64
        );
    }

    #[test]
    fn allocates_no_more_than_max_bytes_however_small_the_records() {
        let root = tempfile::tempdir().unwrap();
        let mut sorter = Sorter::new(config(1 << 20, 64, root.path())).unwrap();
        for value in 0u64..200_000 {
            sorter
                .push(&value.to_be_bytes(), &[value as u8; 9])
                .unwrap();
            let spare = sorter.spare.as_ref().map_or(0, Chunk::capacity_bytes);
            assert!(sorter.chunk.capacity_bytes() <= sorter.limit);
            assert!(spare <= sorter.limit);
        }
        sorter.wait().unwrap();
        let least = (sorter.limit - sorter.limit / 8) / (17 + ENTRY_BYTES);
        assert!(sorter.runs() > 2);
        assert!(sorter.runs.iter().all(|run| run.records as usize >= least));
    }

    #[test]
    fn spills_runs_smaller_than_their_records() {
        let root = tempfile::tempdir().unwrap();
        let mut sorter = Sorter::new(config(1 << 20, 64, root.path())).unwrap();
        for value in (0u32..100_000).rev() {
            let line = format!("chr{}\t{value}\tread{value}\n", value % 22);
            sorter.push(&value.to_be_bytes(), line.as_bytes()).unwrap();
        }
        sorter.wait().unwrap();
        assert!(sorter.runs() > 2);
        let records: u64 = sorter.runs.iter().map(|run| run.bytes).sum();
        let on_disk: u64 = sorter
            .runs
            .iter()
            .map(|run| fs::metadata(&run.path).unwrap().len())
            .sum();
        assert!(on_disk < records / 2, "{on_disk} of {records} bytes");
    }

    #[test]
    fn keeps_records_with_equal_keys_in_order_even_when_empty() {
        let mut chunk = Chunk::default();
        let records: [(&[u8], &[u8]); 5] =
            [(b"", b"b"), (b"", b""), (b"", b"a"), (b"", b""), (b"", b"")];
        for (key, value) in records {
            assert!(chunk.make_room(key.len() + value.len(), 1 << 10));
            chunk.push(key, value);
        }
        chunk.sort(None);
        let values: Vec<&[u8]> = (0..chunk.len())
            .map(|index| chunk.record(index).1)
            .collect();
        assert_eq!(values, [b"b".as_slice(), b"", b"a", b"", b""]);
    }

    #[test]
    fn holds_a_record_larger_than_max_bytes_alone() {
        let root = tempfile::tempdir().unwrap();
        let mut sorter = Sorter::new(config(1 << 10, 64, root.path())).unwrap();
        sorter.push(b"a", b"").unwrap();
        sorter.push(b"b", &[0; 4 << 10]).unwrap();
        sorter.push(b"c", b"").unwrap();
        sorter.wait().unwrap();
        assert_eq!(sorter.runs(), 2);
        assert!(sorter.spare.as_ref().unwrap().capacity_bytes() <= sorter.limit);
        let keys: Vec<Vec<u8>> = sorter
            .finish()
            .unwrap()
            .map(|record| record.unwrap().0)
            .collect();
        assert_eq!(keys, [b"a", b"b", b"c"]);
    }

    #[test]
    fn removes_its_runs_once_merged() {
        let root = tempfile::tempdir().unwrap();
        let mut sorter = Sorter::new(config(64, 2, root.path())).unwrap();
        for value in (0u32..500).rev() {
            sorter.push(&value.to_be_bytes(), b"").unwrap();
        }
        assert!(sorter.runs() > 2);
        let mut merged = sorter.finish().unwrap();
        assert_eq!(entries(root.path()), 1);
        let mut previous = None;
        while let Some((key, _)) = merged.next_record().unwrap() {
            assert!(previous.as_deref() < Some(key));
            previous = Some(key.to_vec());
        }
        assert_eq!(entries(root.path()), 0);
        assert!(merged.next_record().unwrap().is_none());
    }

    #[test]
    fn stops_merging_when_asked_and_removes_its_runs() {
        let root = tempfile::tempdir().unwrap();
        let mut sorter = Sorter::new(config(64, 2, root.path())).unwrap();
        for value in 0u32..500 {
            sorter.push(&value.to_be_bytes(), b"value").unwrap();
        }
        assert!(sorter.runs() > 3);
        let mut asked = 0;
        let error = sorter
            .finish_or_stop(&mut || {
                asked += 1;
                asked > 2
            })
            .unwrap_err();
        assert!(matches!(&error, Error::Io(io) if io.kind() == io::ErrorKind::Interrupted));
        assert_eq!(asked, 3);
        assert_eq!(entries(root.path()), 0);
    }

    #[test]
    fn removes_its_runs_when_dropped() {
        let root = tempfile::tempdir().unwrap();
        let mut sorter = Sorter::new(config(64, 2, root.path())).unwrap();
        for value in 0u32..100 {
            sorter.push(&value.to_be_bytes(), b"value").unwrap();
        }
        assert_eq!(entries(root.path()), 1);
        drop(sorter);
        assert_eq!(entries(root.path()), 0);

        let mut sorter = Sorter::new(config(64, 2, root.path())).unwrap();
        for value in 0u32..100 {
            sorter.push(&value.to_be_bytes(), b"value").unwrap();
        }
        let mut merged = sorter.finish().unwrap();
        merged.next_record().unwrap();
        drop(merged);
        assert_eq!(entries(root.path()), 0);
    }

    #[test]
    fn holds_everything_in_memory_when_it_fits() {
        let root = tempfile::tempdir().unwrap();
        let mut sorter = Sorter::new(config(DEFAULT_MAX_BYTES, 2, root.path())).unwrap();
        for value in [3u8, 1, 2] {
            sorter.push(&[value], &[value]).unwrap();
        }
        assert_eq!(sorter.runs(), 0);
        assert_eq!(entries(root.path()), 0);
        let mut out = Vec::new();
        assert_eq!(sorter.finish().unwrap().write_values(&mut out).unwrap(), 3);
        assert_eq!(out, [1, 2, 3]);
    }

    #[test]
    fn refuses_to_go_on_after_a_failed_spill() {
        let root = tempfile::tempdir().unwrap();
        let mut sorter = Sorter::new(config(64, 2, root.path())).unwrap();
        let mut value = 0u32;
        while sorter.runs() == 0 {
            sorter.push(&value.to_be_bytes(), b"").unwrap();
            value += 1;
        }
        sorter.wait().unwrap();
        for entry in fs::read_dir(root.path()).unwrap() {
            fs::remove_dir_all(entry.unwrap().path()).unwrap();
        }
        let error = loop {
            if let Err(error) = sorter.push(&value.to_be_bytes(), b"") {
                break error;
            }
        };
        let Error::Path { path, error } = &error else {
            panic!("expected an error at a path, not {error:?}");
        };
        assert!(path.starts_with(root.path()), "{}", path.display());
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        assert!(error.raw_os_error().is_some(), "{error:?}");
        assert!(matches!(sorter.push(b"", b""), Err(Error::Failed)));
        assert!(matches!(sorter.finish(), Err(Error::Failed)));
    }

    #[test]
    fn a_failed_merge_removes_its_runs_and_stops() {
        let root = tempfile::tempdir().unwrap();
        let mut sorter = Sorter::new(config(1 << 20, 1_000, root.path())).unwrap();
        for value in 0u32..20_000 {
            sorter
                .push(&value.to_be_bytes(), format!("{value:0100}").as_bytes())
                .unwrap();
        }
        sorter.wait().unwrap();
        assert!(sorter.runs() > 2);
        for run in &sorter.runs {
            assert!(run.bytes > 2 * (128 << 10), "{run:?} has a single block");
            let bytes = fs::read(&run.path).unwrap();
            fs::write(&run.path, &bytes[..bytes.len() - 3]).unwrap();
        }
        let mut merged = sorter.finish().unwrap();
        let error = loop {
            match merged.next_record() {
                Ok(Some(_)) => {}
                Ok(None) => panic!("the merge ended without noticing the short runs"),
                Err(error) => break error,
            }
        };
        assert!(matches!(&error, Error::Io(io) if io.kind() == io::ErrorKind::UnexpectedEof));
        assert!(error.to_string().contains("ended before its last record"));
        assert_eq!(entries(root.path()), 0);
        assert!(matches!(merged.next_record(), Err(Error::Failed)));
        assert!(merged.next().is_none());
    }

    #[test]
    fn checks_its_settings() {
        let root = tempfile::tempdir().unwrap();
        let invalid = |config: Config| match Sorter::new(config) {
            Err(Error::Invalid(message)) => message,
            other => panic!("expected an invalid setting, not {other:?}"),
        };
        assert_eq!(
            invalid(Config::default().max_bytes(1)),
            "max_bytes must be at least 2, not 1"
        );
        assert_eq!(
            invalid(Config::default().max_open(1)),
            "max_open must be at least 2, not 1"
        );
        let missing = root.path().join("missing");
        let error = Sorter::new(Config::default().tmp_dir(&missing)).unwrap_err();
        assert!(matches!(&error, Error::Path { path, .. } if *path == missing));
        assert!(error.to_string().contains("missing"));
        let file = root.path().join("file");
        fs::write(&file, b"").unwrap();
        let error = Sorter::new(Config::default().tmp_dir(&file)).unwrap_err();
        assert!(matches!(&error, Error::Io(io) if io.kind() == io::ErrorKind::NotADirectory));
    }

    #[test]
    fn keeps_the_error_of_the_operating_system_whole() {
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("missing");
        let expected = fs::metadata(&missing).unwrap_err();
        let error = Sorter::new(Config::default().tmp_dir(&missing)).unwrap_err();
        let Error::Path { error, .. } = error else {
            panic!("expected an error at a path, not {error:?}");
        };
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        assert!(error.raw_os_error().is_some(), "{error:?}");
        assert_eq!(error.raw_os_error(), expected.raw_os_error());
        assert_eq!(error.to_string(), expected.to_string());
    }

    #[cfg(unix)]
    #[test]
    fn refuses_a_tmp_dir_it_cannot_write_to_and_leaves_nothing_in_one_it_can() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir().unwrap();
        let sorter = Sorter::new(config(DEFAULT_MAX_BYTES, 2, root.path())).unwrap();
        assert_eq!(entries(root.path()), 0);
        drop(sorter);
        let locked = root.path().join("locked");
        fs::create_dir(&locked).unwrap();
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o500)).unwrap();
        if fs::write(locked.join("probe"), b"").is_err() {
            let error = Sorter::new(config(64, 2, &locked)).unwrap_err();
            assert!(
                matches!(&error, Error::Io(io) if io.kind() == io::ErrorKind::PermissionDenied)
            );
            assert_eq!(error.to_string().matches("locked").count(), 1, "{error}");
        }
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o700)).unwrap();
    }

    #[test]
    fn refuses_records_of_4_gib_or_more() {
        let mut sorter = Sorter::new(Config::default()).unwrap();
        let huge = vec![0u8; 1 << 32];
        assert!(matches!(sorter.push(&huge, b""), Err(Error::Invalid(_))));
        assert!(matches!(sorter.push(b"", &huge), Err(Error::Invalid(_))));
        assert!(sorter.is_empty());
    }
}
