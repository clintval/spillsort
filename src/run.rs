//! Runs: files of sorted records, each a u32 key length, the key, a u32 value length, and the
//! value, all little-endian.

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

const WRITE_BUFFER: usize = 1 << 20;
const READ_BUFFER: usize = 256 << 10;

/// A run on disk and the number of records written to it.
#[derive(Clone, Debug)]
pub(crate) struct Run {
    pub path: PathBuf,
    pub records: u64,
}

fn with_path(error: &io::Error, path: &Path) -> io::Error {
    io::Error::new(error.kind(), format!("{}: {error}", path.display()))
}

/// Writes records to a new run, in the order given.
pub(crate) struct RunWriter {
    out: BufWriter<File>,
    path: PathBuf,
    records: u64,
}

impl RunWriter {
    pub fn create(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| with_path(&error, &path))?;
        Ok(RunWriter {
            out: BufWriter::with_capacity(WRITE_BUFFER, file),
            path,
            records: 0,
        })
    }

    /// Writes a record whose key and value are each shorter than 4 GiB.
    pub fn push(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let written = (|| {
            self.out.write_all(&(key.len() as u32).to_le_bytes())?;
            self.out.write_all(key)?;
            self.out.write_all(&(value.len() as u32).to_le_bytes())?;
            self.out.write_all(value)
        })();
        self.records += 1;
        written.map_err(|error| with_path(&error, &self.path))
    }

    pub fn finish(self) -> io::Result<Run> {
        let RunWriter { out, path, records } = self;
        match out.into_inner() {
            Ok(_) => Ok(Run { path, records }),
            Err(error) => Err(with_path(&error.into_error(), &path)),
        }
    }
}

/// Reads the records of a run, in order, into buffers that are reused.
pub(crate) struct RunReader {
    input: BufReader<File>,
    path: PathBuf,
    left: u64,
    bytes_left: u64,
}

impl RunReader {
    pub fn open(run: &Run) -> io::Result<Self> {
        let opened = File::open(&run.path).and_then(|file| Ok((file.metadata()?.len(), file)));
        let (bytes_left, file) = opened.map_err(|error| with_path(&error, &run.path))?;
        Ok(RunReader {
            input: BufReader::with_capacity(READ_BUFFER, file),
            path: run.path.clone(),
            left: run.records,
            bytes_left,
        })
    }

    /// Reads the next record into `key` and `value`, or returns false after the last record.
    pub fn read_into(&mut self, key: &mut Vec<u8>, value: &mut Vec<u8>) -> io::Result<bool> {
        if self.left == 0 {
            return Ok(false);
        }
        self.read_field(key)
            .and_then(|()| self.read_field(value))
            .map_err(|error| match error.kind() {
                io::ErrorKind::UnexpectedEof => {
                    let message = format!("{} ended before its last record", self.path.display());
                    io::Error::new(io::ErrorKind::UnexpectedEof, message)
                }
                io::ErrorKind::InvalidData => {
                    let message = format!(
                        "{} is corrupt: a record is longer than the rest of the run",
                        self.path.display()
                    );
                    io::Error::new(io::ErrorKind::InvalidData, message)
                }
                _ => with_path(&error, &self.path),
            })?;
        self.left -= 1;
        Ok(true)
    }

    /// Reads a field whose length is bounded by the bytes left, so a corrupt one allocates nothing.
    fn read_field(&mut self, field: &mut Vec<u8>) -> io::Result<()> {
        let mut length = [0u8; 4];
        self.input.read_exact(&mut length)?;
        self.bytes_left = self.bytes_left.saturating_sub(4);
        let length = u64::from(u32::from_le_bytes(length));
        if length > self.bytes_left {
            return Err(io::ErrorKind::InvalidData.into());
        }
        field.clear();
        if (&mut self.input).take(length).read_to_end(field)? as u64 != length {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        self.bytes_left -= length;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn write(path: PathBuf, records: &[(&[u8], &[u8])]) -> Run {
        let mut writer = RunWriter::create(path).unwrap();
        for (key, value) in records {
            writer.push(key, value).unwrap();
        }
        writer.finish().unwrap()
    }

    fn read(run: &Run) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut reader = RunReader::open(run)?;
        let mut records = Vec::new();
        let (mut key, mut value) = (Vec::new(), Vec::new());
        while reader.read_into(&mut key, &mut value)? {
            records.push((key.clone(), value.clone()));
        }
        Ok(records)
    }

    #[test]
    fn records_round_trip_including_empty_ones() {
        let dir = tempfile::tempdir().unwrap();
        let records: [(&[u8], &[u8]); 3] = [(b"", b""), (b"a", b""), (b"\0\0", b"value")];
        let run = write(dir.path().join("run"), &records);
        assert_eq!(run.records, 3);
        let expected: Vec<_> = records
            .iter()
            .map(|(key, value)| (key.to_vec(), value.to_vec()))
            .collect();
        assert_eq!(read(&run).unwrap(), expected);
    }

    #[test]
    fn a_run_that_ends_early_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let run = write(
            dir.path().join("run"),
            &[(b"key", b"value"), (b"key", b"value")],
        );
        let bytes = fs::read(&run.path).unwrap();
        fs::write(&run.path, &bytes[..bytes.len() / 2]).unwrap();
        let error = read(&run).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert!(error.to_string().contains("ended before its last record"));
    }

    #[test]
    fn a_length_past_the_end_of_a_run_is_corrupt_and_allocates_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let run = write(dir.path().join("run"), &[(b"key", b"value")]);
        let mut bytes = fs::read(&run.path).unwrap();
        bytes[..4].copy_from_slice(&u32::MAX.to_le_bytes());
        fs::write(&run.path, &bytes).unwrap();
        let mut reader = RunReader::open(&run).unwrap();
        let (mut key, mut value) = (Vec::new(), Vec::new());
        let error = reader.read_into(&mut key, &mut value).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("is corrupt"), "{error}");
        assert!(key.capacity() < 1 << 10, "{}", key.capacity());
    }

    #[test]
    fn a_missing_run_names_its_path() {
        let dir = tempfile::tempdir().unwrap();
        let run = Run {
            path: dir.path().join("missing"),
            records: 1,
        };
        let error = read(&run).map(|_| ()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        assert!(error.to_string().contains("missing"));
    }

    #[test]
    fn a_run_is_never_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("run");
        write(path.clone(), &[]);
        let error = RunWriter::create(path).map(|_| ()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    }
}
