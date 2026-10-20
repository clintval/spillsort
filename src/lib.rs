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

pub use merge::Merged;
pub use sorter::{
    Config, DEFAULT_MAX_BYTES, DEFAULT_MAX_OPEN, DEFAULT_THREADS, MIN_MAX_BYTES, MIN_MAX_OPEN,
    Sorter,
};

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

/// An error from sorting or merging.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// Writing or reading a run failed.
    Io(io::Error),
    /// A setting or a record is invalid, such as a key of 4 GiB or more.
    Invalid(String),
    /// An earlier error lost records, so the sorter or merge cannot go on.
    Failed,
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::Io(error)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(error) => error.fmt(f),
            Error::Invalid(message) => f.write_str(message),
            Error::Failed => f.write_str("an earlier error lost records, so sorting cannot go on"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(error) => Some(error),
            Error::Invalid(_) | Error::Failed => None,
        }
    }
}

/// A result whose error is an [`Error`].
pub type Result<T> = std::result::Result<T, Error>;
