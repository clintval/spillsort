//! Sorting records in bounded memory: a full chunk is sorted and written as a run by a background
//! thread while the next chunk fills, and finishing merges the runs with the last chunk.

use std::fmt;
use std::fs;
use std::io;
use std::mem;
use std::panic;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use rayon::slice::ParallelSliceMut;
use rayon::{ThreadPool, ThreadPoolBuilder};
use tempfile::TempDir;

use crate::merge::{Merged, Source};
use crate::run::{Run, RunReader, RunWriter, record_bytes};
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

#[derive(Clone, Copy)]
struct Entry {
    offset: usize,
    key_len: u32,
    value_len: u32,
}

/// Records held in memory, their keys and values side by side in one arena.
#[derive(Default)]
pub(crate) struct Chunk {
    arena: Vec<u8>,
    entries: Vec<Entry>,
}

impl Chunk {
    /// The bytes this chunk holds: its keys, its values, and the place of each record.
    fn bytes(&self) -> usize {
        self.arena.len() + self.entries.len() * ENTRY_BYTES
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Adds a record, growing the arena geometrically but never past `limit` unless one record
    /// needs more.
    fn push(&mut self, key: &[u8], value: &[u8], limit: usize) {
        let needed = self.arena.len() + key.len() + value.len();
        if needed > self.arena.capacity() {
            let grown = (self.arena.capacity() * 2).max(MIN_ARENA).min(limit);
            self.arena
                .reserve_exact(grown.max(needed) - self.arena.len());
        }
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

    /// Sorts by key, on a pool's threads if given, keeping records with equal keys in the order
    /// pushed.
    fn sort(&mut self, pool: Option<&ThreadPool>) {
        let arena = &self.arena;
        let entries = &mut self.entries;
        let key = |entry: &Entry| &arena[entry.offset..entry.offset + entry.key_len as usize];
        match pool {
            Some(pool) => pool.install(|| entries.par_sort_by(|a, b| key(a).cmp(key(b)))),
            None => entries.sort_by(|a, b| key(a).cmp(key(b))),
        }
    }

    /// The bytes this chunk takes once written as a run.
    fn run_bytes(&self) -> u64 {
        (0..self.len())
            .map(|index| {
                let (key, value) = self.record(index);
                record_bytes(key, value)
            })
            .sum()
    }

    fn write_run(&self, path: PathBuf) -> io::Result<Run> {
        let mut run = RunWriter::create(path)?;
        for index in 0..self.len() {
            let (key, value) = self.record(index);
            run.push(key, value)?;
        }
        run.finish()
    }

    fn clear(&mut self) {
        self.arena.clear();
        self.entries.clear();
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
    /// The most bytes of records to hold in memory, counting each record's key and value and 16
    /// bytes for its place. Half fills while the other half is sorted and spilled.
    pub max_bytes: usize,
    /// The directory in which a private directory of runs is made, or None for the system's
    /// temporary directory, which is `TMPDIR` on Unix.
    pub tmp_dir: Option<PathBuf>,
    /// The most runs to merge at once. Beyond that, the fewest neighboring runs needed are merged
    /// into one first, as many times as it takes.
    pub max_open: usize,
    /// The threads that sort each chunk: with 1, the thread that sorts it, and with more, a pool
    /// of this sorter's own.
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
    pending: Option<JoinHandle<io::Result<Chunk>>>,
    count: u64,
    failed: bool,
}

impl Sorter {
    /// Makes a sorter, checking that its settings are valid and that its `tmp_dir` is a
    /// directory.
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
        if let Some(dir) = &config.tmp_dir {
            let metadata = fs::metadata(dir).map_err(|error| {
                io::Error::new(error.kind(), format!("{}: {error}", dir.display()))
            })?;
            if !metadata.is_dir() {
                let message = format!("{} is not a directory", dir.display());
                return Err(io::Error::new(io::ErrorKind::NotADirectory, message).into());
            }
        }
        if config.threads == 0 {
            return Err(Error::Invalid(
                "threads must be at least 1, not 0".to_owned(),
            ));
        }
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

    /// Adds a record. Once half of `max_bytes` is held, it is spilled in the background, after
    /// waiting for the spill before it to finish.
    pub fn push(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.failed {
            return Err(Error::Failed);
        }
        if u32::try_from(key.len()).is_err() || u32::try_from(value.len()).is_err() {
            return Err(Error::Invalid(
                "a key or value must be shorter than 4 GiB".to_owned(),
            ));
        }
        self.chunk.push(key, value, self.limit);
        self.count += 1;
        if self.chunk.bytes() >= self.limit {
            self.spill().inspect_err(|_| self.failed = true)?;
        }
        Ok(())
    }

    /// Returns true if pushing a record of this many bytes would spill, and so might wait.
    #[cfg(feature = "python")]
    pub(crate) fn would_spill(&self, bytes: usize) -> bool {
        self.chunk.bytes() + bytes + ENTRY_BYTES >= self.limit
    }

    /// Stops adding and returns every record, merged in order.
    pub fn finish(mut self) -> Result<Merged> {
        if self.failed {
            return Err(Error::Failed);
        }
        self.wait()?;
        self.spare = None;
        self.reduce()?;
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
            empty => {
                let mut builder = tempfile::Builder::new();
                builder.prefix("spillsort-");
                let made = match &self.config.tmp_dir {
                    Some(root) => builder.tempdir_in(root),
                    None => builder.tempdir(),
                };
                empty.insert(made?)
            }
        };
        let path = dir.path().join(format!("run-{:06}", self.made));
        self.made += 1;
        Ok(path)
    }

    /// Waits for the spill in progress, keeping its chunk to fill next.
    fn wait(&mut self) -> Result<()> {
        if let Some(handle) = self.pending.take() {
            match handle.join() {
                Ok(spilled) => self.spare = Some(spilled?),
                Err(panicked) => panic::resume_unwind(panicked),
            }
        }
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
        let pool = self.pool.clone();
        let handle = thread::Builder::new()
            .name("spillsort-spill".to_owned())
            .spawn(move || {
                chunk.sort(pool.as_deref());
                chunk.write_run(path)?;
                chunk.clear();
                Ok(chunk)
            })?;
        self.pending = Some(handle);
        Ok(())
    }

    /// Merges neighboring runs, so equal keys stay stable, until no more than `max_open` remain,
    /// each time merging only as many as are too many, and the neighbors with the fewest bytes.
    fn reduce(&mut self) -> Result<()> {
        let max_open = self.config.max_open;
        while self.runs.len() > max_open {
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
            let mut out = RunWriter::create(self.next_path()?)?;
            while let Some((key, value)) = merged.next_record()? {
                out.push(key, value)?;
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
        if let Some(handle) = self.pending.take() {
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
    fn merges_many_more_runs_than_it_opens_in_passes() {
        let records: Vec<(u8, u16)> = (0..5_000u16)
            .map(|value| ((value % 7) as u8, value))
            .collect();
        let mut expected = records.clone();
        expected.sort_by_key(|record| record.0);
        assert_eq!(sorted_by_sorter(&records, 64, 3), expected);
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
        sorter.reduce().unwrap();
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
        assert!(matches!(error, Error::Io(_)), "{error:?}");
        assert!(matches!(sorter.push(b"", b""), Err(Error::Failed)));
        assert!(matches!(sorter.finish(), Err(Error::Failed)));
    }

    #[test]
    fn a_failed_merge_removes_its_runs_and_stops() {
        let root = tempfile::tempdir().unwrap();
        let mut sorter = Sorter::new(config(64, 1_000, root.path())).unwrap();
        for value in 0u32..200 {
            sorter.push(&value.to_be_bytes(), b"value").unwrap();
        }
        sorter.wait().unwrap();
        let dir = fs::read_dir(root.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let record = 4 + 4 + 4 + b"value".len();
        for run in fs::read_dir(&dir).unwrap() {
            let run = run.unwrap().path();
            let bytes = fs::read(&run).unwrap();
            assert_eq!(bytes.len(), 2 * record);
            fs::write(&run, &bytes[..record + 3]).unwrap();
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
        assert!(matches!(&error, Error::Io(io) if io.kind() == io::ErrorKind::NotFound));
        assert!(error.to_string().contains("missing"));
        let file = root.path().join("file");
        fs::write(&file, b"").unwrap();
        let error = Sorter::new(Config::default().tmp_dir(&file)).unwrap_err();
        assert!(matches!(&error, Error::Io(io) if io.kind() == io::ErrorKind::NotADirectory));
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
