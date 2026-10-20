//! Sort more records than fit in memory: sorted runs spill to disk and merge as they are read.
//!
//! A record is a key and a value, both bytes. Records sort by key, compared as bytes, and records
//! with equal keys come out in the order they were pushed. A [`Sorter`] holds at most
//! [`Config::max_bytes`] of records in memory. Once half of that is full, a background thread
//! sorts it, on a pool of [`Config::threads`] threads if there are more than one, and writes it,
//! compressed with LZ4, to a temporary file, a run, while the other half fills.
//! [`Sorter::finish`] merges the runs with the records still held and returns them in order.
//!
//! Runs last only as long as the sort, so they are never synced to disk. A write error that a
//! file system reports late, as NFS and Lustre may at close, goes unseen when a run is written,
//! but no record it lost can come back wrong: reading a run checks its record count and each
//! block's checksum, so the merge fails with an error naming the run.
//!
//! The [`key`] module builds keys from numbers, text, bytes, and tuples of them, and their bytes
//! compare as those values do in Python. A key built from several fields sorts field by field.
//!
//! ```
//! use spillsort::{Config, Sorter, key};
//!
//! let mut sorter = Sorter::new(Config::default().max_bytes(1 << 20))?;
//! for (refname, start) in [("chr2", 5), ("chr1", 70), ("chr1", 8)] {
//!     let mut sort_key = Vec::new();
//!     key::push_str(&mut sort_key, refname);
//!     key::push_int(&mut sort_key, start);
//!     sorter.push(&sort_key, format!("{refname}:{start}").as_bytes())?;
//! }
//!
//! let mut merged = sorter.finish()?;
//! let mut values = Vec::new();
//! while let Some((_, value)) = merged.next_record()? {
//!     values.push(String::from_utf8_lossy(value).into_owned());
//! }
//! assert_eq!(values, ["chr1:8", "chr1:70", "chr2:5"]);
//! # Ok::<(), spillsort::Error>(())
//! ```

mod channel;
pub mod key;
mod merge;
mod run;
mod sorter;

#[cfg(feature = "python")]
mod python;

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

pub use merge::Merged;
pub use sorter::{
    Config, DEFAULT_MAX_BYTES, DEFAULT_MAX_OPEN, DEFAULT_THREADS, MIN_MAX_BYTES, MIN_MAX_OPEN,
    Sorter,
};

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

/// An error from sorting or merging.
///
/// An [`Error::Io`], [`Error::Path`], or [`Error::Output`] shows its I/O error's message, so its
/// source is that error's source, not the error itself, and a report of the chain names each cause
/// once.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// Writing or reading failed, as when a run is corrupt or the directory of runs cannot be made.
    Io(io::Error),
    /// The operating system failed to read or write a path: `tmp_dir`, or a run.
    Path {
        /// The file or directory.
        path: PathBuf,
        /// The error as the operating system reported it, with its kind and its code.
        error: io::Error,
    },
    /// Writing to the output of [`Merged::write_values`] failed, losing the value being written,
    /// so the merge cannot go on.
    Output(io::Error),
    /// A setting or a record is invalid, such as a key of 4 GiB or more.
    Invalid(String),
    /// An earlier error lost records, so the sorter or merge cannot go on.
    Failed,
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        match error.downcast::<PathError>() {
            Ok(PathError { path, error }) => Error::Path { path, error },
            Err(error) => Error::Io(error),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(error) => error.fmt(f),
            Error::Path { path, error } => write!(f, "{}: {error}", path.display()),
            Error::Output(error) => write!(f, "writing the output failed: {error}"),
            Error::Invalid(message) => f.write_str(message),
            Error::Failed => f.write_str("an earlier error lost records, so sorting cannot go on"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(error) | Error::Path { error, .. } | Error::Output(error) => error.source(),
            Error::Invalid(_) | Error::Failed => None,
        }
    }
}

/// An I/O error and the path it happened at, carried in an [`io::Error`] until it becomes an
/// [`Error::Path`].
#[derive(Debug)]
struct PathError {
    path: PathBuf,
    error: io::Error,
}

impl fmt::Display for PathError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.path.display(), self.error)
    }
}

impl std::error::Error for PathError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.error.source()
    }
}

/// Names the path an I/O error happened at, keeping the error, so its kind and code survive.
pub(crate) fn with_path(error: io::Error, path: &Path) -> io::Error {
    let kind = error.kind();
    let path = path.to_path_buf();
    io::Error::new(kind, PathError { path, error })
}

/// A result whose error is an [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::{Config, Sorter};

    /// Prints an error and each of its sources, as `anyhow` prints a chain with `{:#}`.
    fn report(error: &dyn std::error::Error) -> String {
        let mut report = error.to_string();
        let mut source = error.source();
        while let Some(cause) = source {
            report = format!("{report}: {cause}");
            source = cause.source();
        }
        report
    }

    #[test]
    fn a_report_of_an_error_names_its_path_and_reason_once() {
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("missing");
        let reason = fs::metadata(&missing).unwrap_err().to_string();
        let error = Sorter::new(Config::default().tmp_dir(&missing)).unwrap_err();
        let report = report(&error);
        assert_eq!(report, error.to_string());
        let path = missing.to_string_lossy();
        assert_eq!(report.matches(&*path).count(), 1, "{report}");
        assert_eq!(report.matches(&reason).count(), 1, "{report}");
    }
}
