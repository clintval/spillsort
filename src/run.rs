//! Runs: files of sorted records, each a u32 key length, the key, a u32 value length, and the
//! value, all little-endian. The records are cut into blocks of at most 128 KiB, wherever a record
//! ends, and each block is compressed with LZ4 and written after its compressed and uncompressed
//! lengths and a CRC-32 of those lengths and the compressed block, as u32s, so a run is read a
//! block at a time and a block changed on disk is an error before it is decompressed.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use lz4_flex::block::{compress_into, decompress_into, get_maximum_output_size};
use rayon::ThreadPool;
use rayon::iter::{IntoParallelRefMutIterator, ParallelIterator};

/// The most bytes of records a block holds.
const BLOCK: usize = 128 << 10;

/// The bytes before each block: its compressed and uncompressed lengths, then their checksum.
const HEADER: usize = 12;

/// The bytes of a header that its checksum covers, before the checksum itself.
const LENGTHS: usize = 8;

/// The most blocks a run writer compresses at once, on a pool of any size.
const MOST_BLOCKS: usize = 16;

/// The bytes a run writer gathers before writing them to its file.
const WRITE_BUFFER: usize = 1 << 20;

/// A run on disk, the number of records written to it, and the bytes they take uncompressed.
#[derive(Clone, Debug)]
pub(crate) struct Run {
    pub path: PathBuf,
    pub records: u64,
    pub bytes: u64,
}

/// The bytes a record takes in a run, before it is compressed.
pub(crate) fn record_bytes(key: &[u8], value: &[u8]) -> u64 {
    (8 + key.len() + value.len()) as u64
}

fn with_path(error: &io::Error, path: &Path) -> io::Error {
    io::Error::new(error.kind(), format!("{}: {error}", path.display()))
}

fn corrupt(reason: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason)
}

/// The CRC-32 of a block's lengths and its compressed bytes.
fn checksum(lengths: &[u8], compressed: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(lengths);
    hasher.update(compressed);
    hasher.finalize()
}

/// A block of records, and the room to compress it into after its header.
#[derive(Default)]
struct Block {
    records: Vec<u8>,
    compressed: Vec<u8>,
    written: usize,
}

impl Block {
    fn compress(&mut self) -> io::Result<()> {
        let most = HEADER + get_maximum_output_size(self.records.len());
        if self.compressed.len() < most {
            self.compressed.resize(most, 0);
        }
        let (header, payload) = self.compressed.split_at_mut(HEADER);
        let size = compress_into(&self.records, payload).map_err(io::Error::other)?;
        header[..4].copy_from_slice(&(size as u32).to_le_bytes());
        header[4..LENGTHS].copy_from_slice(&(self.records.len() as u32).to_le_bytes());
        let sum = checksum(&header[..LENGTHS], &payload[..size]);
        header[LENGTHS..].copy_from_slice(&sum.to_le_bytes());
        self.written = HEADER + size;
        Ok(())
    }
}

/// Writes records to a new run, in the order given, a compressed block at a time, or, given a
/// pool, compressing two blocks for each of its threads at once, and at most 16, through a 1 MiB
/// buffer.
pub(crate) struct RunWriter {
    file: BufWriter<File>,
    path: PathBuf,
    pool: Option<Arc<ThreadPool>>,
    blocks: Vec<Block>,
    filling: usize,
    records: u64,
    bytes: u64,
}

impl RunWriter {
    pub fn create(path: PathBuf, pool: Option<Arc<ThreadPool>>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| with_path(&error, &path))?;
        let blocks = pool
            .as_ref()
            .map_or(1, |pool| (2 * pool.current_num_threads()).min(MOST_BLOCKS));
        Ok(RunWriter {
            file: BufWriter::with_capacity(WRITE_BUFFER, file),
            path,
            pool,
            blocks: (0..blocks).map(|_| Block::default()).collect(),
            filling: 0,
            records: 0,
            bytes: 0,
        })
    }

    /// Writes a record whose key and value are each shorter than 4 GiB.
    pub fn push(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let written = (|| {
            self.write_bytes(&(key.len() as u32).to_le_bytes())?;
            self.write_bytes(key)?;
            self.write_bytes(&(value.len() as u32).to_le_bytes())?;
            self.write_bytes(value)
        })();
        self.records += 1;
        self.bytes += record_bytes(key, value);
        written.map_err(|error| with_path(&error, &self.path))
    }

    /// The number of records written so far.
    pub fn records(&self) -> u64 {
        self.records
    }

    pub fn finish(mut self) -> io::Result<Run> {
        self.write_blocks()
            .and_then(|()| self.file.flush())
            .map_err(|error| with_path(&error, &self.path))?;
        Ok(Run {
            path: self.path,
            records: self.records,
            bytes: self.bytes,
        })
    }

    /// Adds bytes to the blocks, writing them each time they are all full.
    fn write_bytes(&mut self, mut bytes: &[u8]) -> io::Result<()> {
        while !bytes.is_empty() {
            let block = &mut self.blocks[self.filling].records;
            if block.capacity() == 0 {
                block.reserve_exact(BLOCK);
            }
            let taken = bytes.len().min(BLOCK - block.len());
            block.extend_from_slice(&bytes[..taken]);
            bytes = &bytes[taken..];
            if block.len() == BLOCK {
                self.filling += 1;
                if self.filling == self.blocks.len() {
                    self.write_blocks()?;
                }
            }
        }
        Ok(())
    }

    /// Compresses the blocks that hold records, on the pool if there is one, and writes them.
    fn write_blocks(&mut self) -> io::Result<()> {
        let filled = self
            .blocks
            .iter()
            .take_while(|block| !block.records.is_empty())
            .count();
        let blocks = &mut self.blocks[..filled];
        match &self.pool {
            Some(pool) if filled > 1 => {
                pool.install(|| blocks.par_iter_mut().try_for_each(Block::compress))?;
            }
            _ => blocks.iter_mut().try_for_each(Block::compress)?,
        }
        for block in blocks {
            self.file.write_all(&block.compressed[..block.written])?;
            block.records.clear();
        }
        self.filling = 0;
        Ok(())
    }
}

/// Reads the records of a run, in order, a block at a time, into a block and room for it
/// compressed, which it allocates once, at their most: about 269 KiB.
pub(crate) struct RunReader {
    file: File,
    path: PathBuf,
    compressed: Vec<u8>,
    ahead: Range<usize>,
    block: Vec<u8>,
    read: usize,
    left: u64,
    bytes_left: u64,
}

impl RunReader {
    pub fn open(run: &Run) -> io::Result<Self> {
        let file = File::open(&run.path).map_err(|error| with_path(&error, &run.path))?;
        Ok(RunReader {
            file,
            path: run.path.clone(),
            compressed: Vec::new(),
            ahead: 0..0,
            block: Vec::new(),
            read: 0,
            left: run.records,
            bytes_left: run.bytes,
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
                    let message = format!("{} is corrupt: {error}", self.path.display());
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
        let mut filled = 0;
        while filled < length.len() {
            let taken = self.unread()?.min(length.len() - filled);
            length[filled..filled + taken].copy_from_slice(&self.block[self.read..][..taken]);
            self.read += taken;
            filled += taken;
        }
        self.bytes_left = self.bytes_left.saturating_sub(4);
        let length = u64::from(u32::from_le_bytes(length));
        if length > self.bytes_left {
            return Err(corrupt("a record is longer than the rest of the run"));
        }
        field.clear();
        let mut wanted = length as usize;
        while wanted > 0 {
            let taken = self.unread()?.min(wanted);
            field.extend_from_slice(&self.block[self.read..][..taken]);
            self.read += taken;
            wanted -= taken;
        }
        self.bytes_left -= length;
        Ok(())
    }

    /// The number of bytes of the block not yet read, reading the next block once none are left.
    fn unread(&mut self) -> io::Result<usize> {
        if self.read == self.block.len() {
            self.read_block()?;
        }
        Ok(self.block.len() - self.read)
    }

    /// Reads and decompresses the next block, checking its lengths before allocating for it and
    /// its checksum before decompressing it, in one read that takes as much of the next block's
    /// header as the file has, kept in `ahead`.
    fn read_block(&mut self) -> io::Result<()> {
        let mut header = [0u8; HEADER];
        let ahead = self.ahead.len();
        header[..ahead].copy_from_slice(&self.compressed[self.ahead.clone()]);
        self.file.read_exact(&mut header[ahead..])?;
        let [c0, c1, c2, c3, u0, u1, u2, u3, s0, s1, s2, s3] = header;
        let size = u32::from_le_bytes([c0, c1, c2, c3]) as usize;
        let length = u32::from_le_bytes([u0, u1, u2, u3]) as usize;
        let sum = u32::from_le_bytes([s0, s1, s2, s3]);
        if length == 0 || length > BLOCK || size == 0 || size > get_maximum_output_size(length) {
            return Err(corrupt("a block has an impossible length"));
        }
        if self.compressed.is_empty() {
            self.compressed = vec![0; get_maximum_output_size(BLOCK) + HEADER];
            self.block.reserve_exact(BLOCK);
        }
        let read = read_at_least(&mut self.file, &mut self.compressed[..size + HEADER], size)?;
        self.ahead = size..read;
        if checksum(&header[..LENGTHS], &self.compressed[..size]) != sum {
            return Err(corrupt("a block does not match its checksum"));
        }
        self.block.resize(length, 0);
        self.read = 0;
        match decompress_into(&self.compressed[..size], &mut self.block) {
            Ok(decompressed) if decompressed == length => Ok(()),
            _ => {
                self.block.clear();
                Err(corrupt("a block does not decompress to its length"))
            }
        }
    }
}

/// Reads at least `least` bytes into `buffer`, and more if they fit and the file has them,
/// returning how many it read.
fn read_at_least(file: &mut File, buffer: &mut [u8], least: usize) -> io::Result<usize> {
    let mut filled = 0;
    while filled < least {
        match file.read(&mut buffer[filled..]) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(read) => filled += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(filled)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::ErrorKind::{InvalidData, UnexpectedEof};
    use std::io::Seek;

    use super::*;

    fn write(path: PathBuf, records: &[(&[u8], &[u8])]) -> Run {
        let mut writer = RunWriter::create(path, None).unwrap();
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

    fn owned(records: &[(&[u8], &[u8])]) -> Vec<(Vec<u8>, Vec<u8>)> {
        records
            .iter()
            .map(|(key, value)| (key.to_vec(), value.to_vec()))
            .collect()
    }

    /// Bytes that do not compress, from a linear congruential generator.
    fn noise(length: usize) -> Vec<u8> {
        let mut state = 0x2545_f491_u32;
        (0..length)
            .map(|_| {
                state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                (state >> 24) as u8
            })
            .collect()
    }

    /// A run of many records, spanning several blocks.
    fn long_run(path: PathBuf) -> Run {
        let mut writer = RunWriter::create(path, None).unwrap();
        for index in 0u32..50_000 {
            let value = format!("chr{}\t{index}\tread{index}\n", index % 22);
            writer.push(&index.to_be_bytes(), value.as_bytes()).unwrap();
        }
        writer.finish().unwrap()
    }

    /// A run of records that do not compress, spanning several blocks.
    fn noise_run(path: PathBuf) -> Run {
        let values = noise(4 * BLOCK);
        let records: Vec<(&[u8], &[u8])> = values
            .chunks(64)
            .map(|value| (&b"key"[..], value))
            .collect();
        write(path, &records)
    }

    /// The offset of each block in a run's bytes.
    fn block_offsets(bytes: &[u8]) -> Vec<usize> {
        let mut offsets = Vec::new();
        let mut offset = 0;
        while offset < bytes.len() {
            offsets.push(offset);
            let size = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
            offset += HEADER + size as usize;
        }
        assert_eq!(offset, bytes.len());
        offsets
    }

    /// Rewrites the checksum of the block at `offset` to match the bytes it now holds.
    fn seal(bytes: &mut [u8], offset: usize) {
        let size = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        let payload = offset + HEADER;
        let sum = checksum(
            &bytes[offset..offset + LENGTHS],
            &bytes[payload..payload + size],
        );
        bytes[offset + LENGTHS..payload].copy_from_slice(&sum.to_le_bytes());
    }

    fn assert_error(error: &io::Error, kind: io::ErrorKind, reason: &str) {
        assert_eq!(error.kind(), kind);
        assert!(error.to_string().contains(reason), "{error}");
    }

    /// Checks that reading a run fails with this kind of error, naming the run and the reason.
    fn assert_unreadable(run: &Run, kind: io::ErrorKind, reason: &str) {
        let error = read(run).unwrap_err();
        assert_error(&error, kind, &run.path.to_string_lossy());
        assert_error(&error, kind, reason);
    }

    #[test]
    fn records_round_trip_including_empty_ones() {
        let dir = tempfile::tempdir().unwrap();
        let records: [(&[u8], &[u8]); 3] = [(b"", b""), (b"a", b""), (b"\0\0", b"value")];
        let run = write(dir.path().join("run"), &records);
        assert_eq!(run.records, 3);
        assert_eq!(run.bytes, 8 + 9 + 15);
        assert_eq!(read(&run).unwrap(), owned(&records));
    }

    #[test]
    fn an_empty_run_is_an_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let run = write(dir.path().join("run"), &[]);
        assert_eq!((run.records, run.bytes), (0, 0));
        assert_eq!(fs::metadata(&run.path).unwrap().len(), 0);
        assert_eq!(read(&run).unwrap(), owned(&[]));
    }

    #[test]
    fn a_run_is_compressed() {
        let dir = tempfile::tempdir().unwrap();
        let run = long_run(dir.path().join("run"));
        let bytes = fs::read(&run.path).unwrap();
        assert!(run.bytes > 4 * BLOCK as u64);
        assert!((bytes.len() as u64) < run.bytes / 2);
        assert!(block_offsets(&bytes).len() > 4);
        let records = read(&run).unwrap();
        assert_eq!(records.len(), 50_000);
        assert_eq!(records[49_999].1, b"chr15\t49999\tread49999\n");
    }

    #[test]
    fn a_pool_compresses_the_same_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(3)
            .build()
            .unwrap();
        let alone = long_run(dir.path().join("alone"));
        let mut writer =
            RunWriter::create(dir.path().join("pooled"), Some(Arc::new(pool))).unwrap();
        assert_eq!(writer.blocks.len(), 6);
        for (key, value) in read(&alone).unwrap() {
            writer.push(&key, &value).unwrap();
        }
        let pooled = writer.finish().unwrap();
        assert_eq!((pooled.records, pooled.bytes), (alone.records, alone.bytes));
        let bytes = fs::read(&pooled.path).unwrap();
        assert!(block_offsets(&bytes).len() > 6);
        assert_eq!(bytes, fs::read(&alone.path).unwrap());
    }

    #[test]
    fn a_pool_compresses_two_blocks_for_each_thread_and_at_most_16() {
        let dir = tempfile::tempdir().unwrap();
        for (threads, blocks) in [(2, 4), (8, 16), (9, 16), (64, 16)] {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap();
            let path = dir.path().join(format!("{threads}"));
            let writer = RunWriter::create(path, Some(Arc::new(pool))).unwrap();
            assert_eq!(writer.blocks.len(), blocks, "{threads} threads");
        }
    }

    #[test]
    fn a_writer_gathers_blocks_before_writing_them() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = RunWriter::create(dir.path().join("run"), None).unwrap();
        while writer.bytes < 4 * BLOCK as u64 {
            writer
                .push(b"key", b"a value that compresses well")
                .unwrap();
        }
        assert_eq!(fs::metadata(&writer.path).unwrap().len(), 0);
        let run = writer.finish().unwrap();
        assert!(block_offsets(&fs::read(&run.path).unwrap()).len() > 4);
    }

    #[test]
    fn a_block_is_read_with_the_header_after_it() {
        let dir = tempfile::tempdir().unwrap();
        let run = long_run(dir.path().join("run"));
        let offsets = block_offsets(&fs::read(&run.path).unwrap());
        let mut reader = RunReader::open(&run).unwrap();
        let (mut key, mut value) = (Vec::new(), Vec::new());
        assert!(reader.read_into(&mut key, &mut value).unwrap());
        let position = reader.file.stream_position().unwrap();
        assert_eq!(position, (offsets[1] + HEADER) as u64);
    }

    #[test]
    fn records_larger_than_a_block_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let (key, value, zeros) = (noise(BLOCK + 1), noise(3 * BLOCK + 7), vec![0; BLOCK]);
        let records: [(&[u8], &[u8]); 4] = [
            (b"a", b"first"),
            (&key, &value),
            (b"b", &zeros),
            (b"c", b"last"),
        ];
        let run = write(dir.path().join("run"), &records);
        let bytes = fs::read(&run.path).unwrap();
        assert!(block_offsets(&bytes).len() >= 5);
        assert_eq!(read(&run).unwrap(), owned(&records));
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
        assert_unreadable(&run, UnexpectedEof, "ended before its last record");
    }

    #[test]
    fn a_run_that_ends_after_a_whole_block_or_within_a_header_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let run = long_run(dir.path().join("run"));
        let bytes = fs::read(&run.path).unwrap();
        for offset in block_offsets(&bytes).into_iter().skip(1) {
            for cut in offset..offset + HEADER {
                fs::write(&run.path, &bytes[..cut]).unwrap();
                assert_unreadable(&run, UnexpectedEof, "ended before its last record");
            }
        }
    }

    #[test]
    fn a_changed_byte_in_a_block_or_its_checksum_is_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let run = noise_run(dir.path().join("run"));
        let bytes = fs::read(&run.path).unwrap();
        let offsets = block_offsets(&bytes);
        let (start, end) = (offsets[1], offsets[2]);
        for at in [
            start + LENGTHS,
            start + HEADER - 1,
            start + HEADER,
            start + HEADER + 200,
            end - 1,
        ] {
            let mut changed = bytes.clone();
            changed[at] ^= 1;
            fs::write(&run.path, &changed).unwrap();
            assert_unreadable(
                &run,
                InvalidData,
                "is corrupt: a block does not match its checksum",
            );
        }
        let last = offsets[offsets.len() - 1] + 4;
        let length = u32::from_le_bytes(bytes[last..last + 4].try_into().unwrap());
        assert!(length < BLOCK as u32);
        for wrong in [length - 1, length + 1] {
            let mut changed = bytes.clone();
            changed[last..last + 4].copy_from_slice(&wrong.to_le_bytes());
            fs::write(&run.path, &changed).unwrap();
            assert_unreadable(
                &run,
                InvalidData,
                "is corrupt: a block does not match its checksum",
            );
        }
    }

    #[test]
    fn a_flipped_bit_in_any_header_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let run = noise_run(dir.path().join("run"));
        let bytes = fs::read(&run.path).unwrap();
        for offset in block_offsets(&bytes) {
            for at in offset..offset + HEADER {
                for bit in 0..8 {
                    let mut changed = bytes.clone();
                    changed[at] ^= 1 << bit;
                    fs::write(&run.path, &changed).unwrap();
                    let error = read(&run).unwrap_err();
                    assert!(
                        matches!(error.kind(), InvalidData | UnexpectedEof),
                        "{error}"
                    );
                    assert!(error.to_string().contains(&*run.path.to_string_lossy()));
                }
            }
        }
    }

    #[test]
    fn a_block_that_matches_its_checksum_but_does_not_decompress_is_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let run = long_run(dir.path().join("run"));
        let bytes = fs::read(&run.path).unwrap();
        let offsets = block_offsets(&bytes);
        let mut garbled = bytes.clone();
        garbled[offsets[1] + HEADER..offsets[2]].fill(0);
        seal(&mut garbled, offsets[1]);
        fs::write(&run.path, &garbled).unwrap();
        assert_unreadable(&run, InvalidData, "is corrupt: a block does not decompress");

        let last = offsets[offsets.len() - 1] + 4;
        let length = u32::from_le_bytes(bytes[last..last + 4].try_into().unwrap());
        assert!(length < BLOCK as u32);
        for wrong in [length - 1, length + 1] {
            let mut garbled = bytes.clone();
            garbled[last..last + 4].copy_from_slice(&wrong.to_le_bytes());
            seal(&mut garbled, last - 4);
            fs::write(&run.path, &garbled).unwrap();
            assert_unreadable(&run, InvalidData, "is corrupt: a block does not decompress");
        }
    }

    #[test]
    fn an_impossible_block_length_is_corrupt_and_allocates_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let run = long_run(dir.path().join("run"));
        let bytes = fs::read(&run.path).unwrap();
        let block = (BLOCK as u32).to_le_bytes();
        let headers: [[&[u8]; 2]; 5] = [
            [&u32::MAX.to_le_bytes(), &block],
            [&bytes[..4], &u32::MAX.to_le_bytes()],
            [&bytes[..4], &(BLOCK as u32 + 1).to_le_bytes()],
            [&bytes[..4], &0u32.to_le_bytes()],
            [&0u32.to_le_bytes(), &block],
        ];
        for [size, length] in headers {
            let garbled = [size, length, &bytes[LENGTHS..]].concat();
            fs::write(&run.path, &garbled).unwrap();
            let mut reader = RunReader::open(&run).unwrap();
            let (mut key, mut value) = (Vec::new(), Vec::new());
            let error = reader.read_into(&mut key, &mut value).unwrap_err();
            assert_error(
                &error,
                InvalidData,
                "is corrupt: a block has an impossible length",
            );
            assert_eq!(reader.compressed.capacity() + reader.block.capacity(), 0);
        }
    }

    #[test]
    fn a_length_past_the_end_of_a_run_is_corrupt_and_allocates_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = RunWriter::create(dir.path().join("run"), None).unwrap();
        writer.write_bytes(&u32::MAX.to_le_bytes()).unwrap();
        writer.write_bytes(&[7; 100]).unwrap();
        let run = Run {
            records: 1,
            bytes: 104,
            ..writer.finish().unwrap()
        };
        let mut reader = RunReader::open(&run).unwrap();
        let (mut key, mut value) = (Vec::new(), Vec::new());
        let error = reader.read_into(&mut key, &mut value).unwrap_err();
        assert_error(&error, InvalidData, "is corrupt: a record is longer");
        assert!(key.capacity() < 1 << 10, "{}", key.capacity());
    }

    #[test]
    fn a_reader_holds_a_block_and_the_most_it_compresses_to_however_well_blocks_compress() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = RunWriter::create(dir.path().join("run"), None).unwrap();
        let mut state = 5u64;
        let mut random = || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            state >> 33
        };
        for alphabet in (1..=256).step_by(15) {
            for _ in 0..BLOCK / 200 {
                let value: Vec<u8> = (0..192).map(|_| (random() % alphabet) as u8).collect();
                writer.push(b"key", &value).unwrap();
            }
        }
        let run = writer.finish().unwrap();
        let bytes = fs::read(&run.path).unwrap();
        let sizes: Vec<usize> = block_offsets(&bytes)
            .windows(2)
            .map(|pair| pair[1] - pair[0])
            .collect();
        assert!(
            sizes.windows(2).any(|pair| pair[1] > 2 * pair[0]),
            "{sizes:?}"
        );
        let mut reader = RunReader::open(&run).unwrap();
        let (mut key, mut value) = (Vec::new(), Vec::new());
        let mut held = Vec::new();
        while reader.read_into(&mut key, &mut value).unwrap() {
            held.push(reader.block.capacity() + reader.compressed.capacity());
        }
        let most = BLOCK + get_maximum_output_size(BLOCK) + HEADER;
        assert!(
            held.iter().all(|&bytes| bytes <= most),
            "{:?}",
            held.iter().max()
        );
        assert!(held.iter().all(|&bytes| bytes == most));
    }

    #[test]
    fn a_missing_run_names_its_path() {
        let dir = tempfile::tempdir().unwrap();
        let run = Run {
            path: dir.path().join("missing"),
            records: 1,
            bytes: 10,
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
        let error = RunWriter::create(path, None).map(|_| ()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    }
}
