//! The `spillsort._spillsort` extension module.

#![allow(clippy::doc_markdown)]

use std::io;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, TryLockError};
use std::thread;

use pyo3::exceptions::{PyOSError, PyRecursionError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyBytes, PyFloat, PyInt, PyList, PyString, PyTuple};
use pyo3::{PyTraverseError, PyVisit};

use crate::channel;
use crate::key;
use crate::sorter::POLL;
use crate::{
    Config, DEFAULT_MAX_BYTES, DEFAULT_MAX_OPEN, Error, MIN_MAX_BYTES, MIN_MAX_OPEN, Merged,
    Sorter as CoreSorter,
};

const CHUNK: usize = 1 << 20;
const BATCH: usize = 4096;
const MAX_DEPTH: usize = 256;
const PICKLE_PROTOCOL: u8 = 5;

const _: () = assert!(DEFAULT_MAX_BYTES == 268_435_456 && DEFAULT_MAX_OPEN == 64);

const ADDING: u8 = 0;
const READ: u8 = 1;
const CLOSED: u8 = 2;

fn to_python(error: Error) -> PyErr {
    match error {
        Error::Io(error) => os_error(error, None),
        Error::Path { path, error } => os_error(error, Some(path)),
        Error::Invalid(message) => PyValueError::new_err(message),
        error @ Error::Failed => PyRuntimeError::new_err(error.to_string()),
    }
}

/// The OSError Python raises when a system call fails, with its errno, its reason, and the path,
/// from which Python picks the subclass, or, for an error with no code, the subclass of its kind.
fn os_error(error: io::Error, path: Option<PathBuf>) -> PyErr {
    let Some(code) = error.raw_os_error() else {
        let error = match path {
            Some(path) => io::Error::new(error.kind(), format!("{}: {error}", path.display())),
            None => error,
        };
        return error.into();
    };
    let message = error.to_string();
    let suffix = format!(" (os error {code})");
    let reason = message.strip_suffix(&suffix).unwrap_or(&message).to_owned();
    // Python reads the code as winerror on Windows, where it is one, and ignores it elsewhere.
    PyOSError::new_err((code, reason, path.map(PathBuf::into_os_string), code))
}

fn closed_error() -> PyErr {
    PyValueError::new_err("I/O operation on a closed sorter")
}

fn read_twice_error() -> PyErr {
    PyRuntimeError::new_err("a sorter can be read only once")
}

fn added_after_read_error() -> PyErr {
    PyRuntimeError::new_err("cannot add to a sorter once it has been read")
}

fn stopped_error() -> PyErr {
    PyRuntimeError::new_err("the sorter stopped when reading it failed or was interrupted")
}

fn check_signals(py: Python<'_>) -> PyResult<()> {
    py.check_signals()
}

/// Runs any signal handlers, as Python does between bytecodes, from a thread that has detached,
/// and returns true once one has raised, keeping what it raised.
fn interrupted(raised: &mut Option<PyErr>) -> bool {
    if raised.is_none()
        && let Err(error) = Python::attach(check_signals)
    {
        *raised = Some(error);
    }
    raised.is_some()
}

/// What a signal handler raised, if one did, or else this error.
fn raised_or(raised: &mut Option<PyErr>, error: Error) -> PyErr {
    raised.take().unwrap_or_else(|| to_python(error))
}

/// Returns an address that no other running thread shares.
fn this_thread() -> usize {
    thread_local! {
        static MARK: u8 = const { 0 };
    }
    MARK.with(|mark| std::ptr::from_ref(mark).addr())
}

/// A mutex that threads wait for without the GIL, and that raises on a reentrant call.
struct Lock<T> {
    mutex: Mutex<T>,
    owner: AtomicUsize,
}

struct Guard<'a, T> {
    inner: MutexGuard<'a, T>,
    owner: &'a AtomicUsize,
}

impl<T> Deref for Guard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T> DerefMut for Guard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T> Drop for Guard<'_, T> {
    fn drop(&mut self) {
        self.owner.store(0, Ordering::Relaxed);
    }
}

impl<T: Send> Lock<T> {
    fn new(value: T) -> Self {
        Self {
            mutex: Mutex::new(value),
            owner: AtomicUsize::new(0),
        }
    }

    fn guard<'a>(&'a self, inner: MutexGuard<'a, T>, thread: usize) -> Guard<'a, T> {
        self.owner.store(thread, Ordering::Relaxed);
        Guard {
            inner,
            owner: &self.owner,
        }
    }

    fn try_lock(&self) -> Option<Guard<'_, T>> {
        let inner = match self.mutex.try_lock() {
            Ok(inner) => inner,
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => return None,
        };
        Some(self.guard(inner, this_thread()))
    }

    /// Waits for the lock; called without the GIL.
    fn lock(&self) -> PyResult<Guard<'_, T>> {
        let thread = this_thread();
        if self.owner.load(Ordering::Relaxed) == thread {
            return Err(PyRuntimeError::new_err("reentrant call"));
        }
        let inner = self.mutex.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(self.guard(inner, thread))
    }
}

/// Values read from the merge, so that the GIL is released once per batch rather than per item.
#[derive(Default)]
struct Batch {
    buffer: Vec<u8>,
    ends: Vec<usize>,
    next: usize,
}

impl Batch {
    fn is_drained(&self) -> bool {
        self.next == self.ends.len()
    }

    fn fill(&mut self, merged: &mut Merged) -> crate::Result<()> {
        self.buffer.clear();
        self.ends.clear();
        self.next = 0;
        while self.ends.len() < BATCH && self.buffer.len() < CHUNK {
            let Some((_, value)) = merged.next_record()? else {
                break;
            };
            self.buffer.extend_from_slice(value);
            self.ends.push(self.buffer.len());
        }
        Ok(())
    }

    fn take(&mut self) -> Option<&[u8]> {
        let end = *self.ends.get(self.next)?;
        let start = self.next.checked_sub(1).map_or(0, |last| self.ends[last]);
        self.next += 1;
        Some(&self.buffer[start..end])
    }
}

enum State {
    Adding(CoreSorter),
    Reading(Merged, Batch),
    Read,
    Stopped,
    Closed,
}

impl State {
    fn refill_if_drained(&mut self) -> PyResult<()> {
        let State::Reading(merged, batch) = self else {
            return Ok(());
        };
        if !batch.is_drained() {
            return Ok(());
        }
        let filled = batch.fill(merged);
        if filled.is_err() {
            *self = State::Stopped;
        } else if batch.is_drained() {
            *self = State::Read;
        }
        filled.map_err(to_python)
    }

    fn needs_refill(&self) -> bool {
        matches!(self, State::Reading(_, batch) if batch.is_drained())
    }

    /// Stops reading after an error or a signal, dropping the merge, which removes what was
    /// spilled.
    fn end(&mut self) {
        if matches!(self, State::Reading(..)) {
            *self = State::Stopped;
        }
    }

    fn next_value(&mut self) -> PyResult<Option<&[u8]>> {
        match self {
            State::Reading(_, batch) => Ok(batch.take()),
            State::Adding(_) | State::Read | State::Stopped => Ok(None),
            State::Closed => Err(closed_error()),
        }
    }
}

enum Encoder {
    Pickle(Py<PyAny>),
    Codec(Py<PyAny>),
}

impl Encoder {
    fn encode<'py>(&self, item: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Encoder::Pickle(dumps) => dumps.bind(item.py()).call1((item, PICKLE_PROTOCOL)),
            Encoder::Codec(encode) => encode.bind(item.py()).call1((item,)),
        }
    }

    fn function(&self) -> &Py<PyAny> {
        match self {
            Encoder::Pickle(function) | Encoder::Codec(function) => function,
        }
    }
}

/// Encodes a Python sort key into bytes that compare as the key does.
fn encode_key(value: &Bound<'_, PyAny>, out: &mut Vec<u8>, depth: usize) -> PyResult<()> {
    if let Ok(int) = value.cast::<PyInt>() {
        push_int(int, out)?;
    } else if let Ok(text) = value.cast::<PyString>() {
        push_text(text, out)?;
    } else if let Ok(float) = value.cast::<PyFloat>() {
        key::push_float(out, float.value());
    } else if let Ok(tuple) = value.cast::<PyTuple>() {
        key::begin_tuple(out);
        encode_items(tuple.iter(), out, depth)?;
        key::end_sequence(out);
    } else if value.is_none() {
        key::push_none(out);
    } else if let Ok(bytes) = value.cast::<PyBytes>() {
        key::push_bytes(out, bytes.as_bytes());
    } else if let Ok(list) = value.cast::<PyList>() {
        key::begin_list(out);
        encode_items(list.iter(), out, depth)?;
        key::end_sequence(out);
    } else if value.get_type().hasattr("__index__")? {
        let index = value.py().import("operator")?.getattr("index")?;
        push_int(index.call1((value,))?.cast::<PyInt>()?, out)?;
    } else {
        return Err(PyTypeError::new_err(format!(
            "a sort key must be an int, float, str, bytes, None, or a tuple or list of them, not {}",
            value.get_type().name()?
        )));
    }
    Ok(())
}

fn encode_items<'py>(
    items: impl Iterator<Item = Bound<'py, PyAny>>,
    out: &mut Vec<u8>,
    depth: usize,
) -> PyResult<()> {
    if depth == MAX_DEPTH {
        return Err(PyRecursionError::new_err(format!(
            "a sort key is nested more than {MAX_DEPTH} deep"
        )));
    }
    for item in items {
        encode_key(&item, out, depth + 1)?;
    }
    Ok(())
}

fn push_int(int: &Bound<'_, PyInt>, out: &mut Vec<u8>) -> PyResult<()> {
    if let Ok(value) = int.extract::<i64>() {
        key::push_int(out, value);
        return Ok(());
    }
    let py = int.py();
    let int_type = py.get_type::<PyInt>();
    let bits: usize = int_type.call_method1("bit_length", (int,))?.extract()?;
    let signed = [("signed", true)].into_py_dict(py)?;
    let encoded = int_type.call_method("to_bytes", (int, bits / 8 + 1, "big"), Some(&signed))?;
    let mut magnitude = encoded.cast::<PyBytes>()?.as_bytes().to_vec();
    let negative = magnitude[0] & 0x80 != 0;
    if negative {
        for byte in &mut magnitude {
            *byte = !*byte;
        }
        for byte in magnitude.iter_mut().rev() {
            let (sum, carry) = byte.overflowing_add(1);
            *byte = sum;
            if !carry {
                break;
            }
        }
    }
    key::push_big_int(out, negative, &magnitude);
    Ok(())
}

fn push_text(text: &Bound<'_, PyString>, out: &mut Vec<u8>) -> PyResult<()> {
    if let Ok(utf8) = text.to_str() {
        key::push_str(out, utf8);
        return Ok(());
    }
    let encoded = text
        .py()
        .get_type::<PyString>()
        .call_method1("encode", (text, "utf-8", "surrogatepass"))?;
    key::push_utf8(out, encoded.cast::<PyBytes>()?.as_bytes());
    Ok(())
}

fn setting(name: &str, value: i64, least: usize) -> PyResult<usize> {
    usize::try_from(value)
        .ok()
        .filter(|&value| value >= least)
        .ok_or_else(|| {
            PyValueError::new_err(format!("{name} must be at least {least}, not {value}"))
        })
}

fn type_name(value: &Bound<'_, PyAny>) -> String {
    value
        .get_type()
        .name()
        .map_or_else(|_| "?".to_owned(), |name| name.to_string())
}

/// Sort more items than fit in memory: add them, then sort them once.
///
/// Each item is turned into a sort key by `key`, or is its own key, and into bytes by `codec`, or
/// by pickle, once, as it is added. Keys and bytes, and 16 bytes more for each item, are held in at
/// most `max_bytes` of memory, unless one item alone needs more. Once half of that is full, a
/// background thread sorts it and writes it, compressed with LZ4, to a private directory in
/// `tmp_dir` while the other half fills. Sorting merges what was written with what is held, so
/// items come out in key order, and items with equal keys come out in the order they were
/// added. A merge reads at most `max_open` runs at once, each a block of up to 128 KiB at a
/// time, holding the block and room for it compressed, about 269 KiB, and its next item, so it
/// takes about `max_open` times the largest item, plus those blocks, besides what is held.
///
/// The private directory is removed when reading ends, on `close()`, when a `with` block exits,
/// or when the sorter is garbage collected. Threads sharing a sorter take turns, and a sorter
/// must not be shared across a fork: make a new one in the child.
///
/// Ctrl-C, or any signal handler that raises, interrupts waiting for a spill, merging, and
/// reading; an interrupted merge or read ends the sort and removes what was spilled, which can take
/// a moment when much was spilled, and the sorter then refuses to be read again.
///
/// Args:
///     key: A function returning the sort key of an item: an int, float, str, bytes, None, or a
///         tuple or list of them, compared as Python compares them. None sorts items by
///         themselves. NumPy integers, like anything with `__index__`, are ints, and
///         `np.float64`, `np.str_` and `np.bytes_` are a float, a str and bytes; other NumPy
///         scalars, such as `np.float32` and `np.bool_`, raise TypeError, so convert them first.
///     codec: Encodes each item into bytes and decodes it back; pickle if None.
///     max_bytes: The most memory to hold keys and encoded items in, counting 16 bytes more for
///         each, at least 2.
///     tmp_dir: The directory to spill into, which must be writable, or None for
///         `tempfile.gettempdir()`.
///     max_open: The most spilled runs to merge at once, at least 2; more are merged in passes.
///     threads: The threads that sort each half and compress each run: with 1, the thread that
///         spills it, and with more, a pool of this sorter's own. Writing a run holds, beyond
///         `max_bytes`, a 1 MiB buffer of what it writes and, for each block compressed at once,
///         a 128 KiB block and room for it compressed, about 269 KiB: one block with 1 thread, and
///         with more, two for each thread, but at most 16, about 4.2 MiB.
///
/// Raises:
///     TypeError: If `key` is not callable, or `codec` lacks `encode` and `decode` methods.
///     ValueError: If `max_bytes` or `max_open` is less than 2, or `threads` is less than 1.
///     OSError: If `tmp_dir` is not a directory, or a directory cannot be made in it.
#[pyclass(module = "spillsort", name = "Sorter", frozen, generic)]
struct PySorter {
    state: Arc<Lock<State>>,
    status: AtomicU8,
    added: AtomicU64,
    key: Option<Py<PyAny>>,
    encoder: Encoder,
    decode: Py<PyAny>,
}

impl PySorter {
    fn check_adding(&self) -> PyResult<()> {
        match self.status.load(Ordering::Acquire) {
            ADDING => Ok(()),
            CLOSED => Err(closed_error()),
            _ => match self.state.try_lock() {
                Some(state) if matches!(*state, State::Stopped) => Err(stopped_error()),
                _ => Err(added_after_read_error()),
            },
        }
    }

    /// Adds a record, first waiting without the GIL, as a signal allows, for a spill that pushing
    /// it would wait for.
    fn push(&self, py: Python<'_>, key: &[u8], payload: &[u8]) -> PyResult<()> {
        let push = |state: &mut State| match state {
            State::Adding(sorter) => {
                if sorter.would_spill(key.len() + payload.len()) {
                    let mut raised = None;
                    let waited = sorter.wait_or_stop(&mut || interrupted(&mut raised));
                    waited.map_err(|error| raised_or(&mut raised, error))?;
                }
                sorter.push(key, payload).map_err(to_python)
            }
            State::Reading(..) | State::Read => Err(added_after_read_error()),
            State::Stopped => Err(stopped_error()),
            State::Closed => Err(closed_error()),
        };
        match self.state.try_lock() {
            Some(mut guard) => {
                let state = &mut *guard;
                let may_wait = matches!(
                    state,
                    State::Adding(sorter) if sorter.would_spill(key.len() + payload.len())
                );
                if may_wait {
                    py.detach(|| push(state))
                } else {
                    push(state)
                }
            }
            None => py.detach(|| push(&mut *self.state.lock()?)),
        }?;
        self.added.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn push_item(
        &self,
        py: Python<'_>,
        item: &Bound<'_, PyAny>,
        key: &mut Vec<u8>,
    ) -> PyResult<()> {
        key.clear();
        match &self.key {
            Some(function) => encode_key(&function.bind(py).call1((item,))?, key, 0)?,
            None => encode_key(item, key, 0)?,
        }
        let encoded = self.encoder.encode(item)?;
        let payload = encoded.cast::<PyBytes>().map_err(|_| {
            PyTypeError::new_err(format!(
                "a codec must encode an item into bytes, not {}",
                type_name(&encoded)
            ))
        })?;
        self.push(py, key, payload.as_bytes())
    }

    fn push_encoded(
        &self,
        py: Python<'_>,
        sort_key: &Bound<'_, PyAny>,
        payload: &[u8],
        key: &mut Vec<u8>,
    ) -> PyResult<()> {
        key.clear();
        encode_key(sort_key, key, 0)?;
        self.push(py, key, payload)
    }

    /// Stops adding and merges, keeping the merge to iterate, or handing it back to write.
    fn finish(&self, py: Python<'_>, iterate: bool) -> PyResult<Option<Merged>> {
        py.detach(|| {
            let mut state = self.state.lock()?;
            match mem::replace(&mut *state, State::Read) {
                State::Adding(sorter) => {
                    self.status.store(READ, Ordering::Release);
                    let mut raised = None;
                    let finished = sorter.finish_or_stop(&mut || interrupted(&mut raised));
                    let merged = finished.map_err(|error| {
                        *state = State::Stopped;
                        raised_or(&mut raised, error)
                    })?;
                    if iterate {
                        *state = State::Reading(merged, Batch::default());
                        Ok(None)
                    } else {
                        Ok(Some(merged))
                    }
                }
                State::Closed => {
                    *state = State::Closed;
                    Err(closed_error())
                }
                State::Stopped => {
                    *state = State::Stopped;
                    Err(stopped_error())
                }
                other => {
                    *state = other;
                    Err(read_twice_error())
                }
            }
        })
    }

    fn iterator(&self, py: Python<'_>, decode: Option<Py<PyAny>>) -> PyResult<SortedIterator> {
        self.finish(py, true)?;
        Ok(SortedIterator {
            state: Arc::clone(&self.state),
            decode,
        })
    }
}

#[pymethods]
impl PySorter {
    #[new]
    #[pyo3(signature = (key = None, *, codec = None, max_bytes = 268_435_456, tmp_dir = None, max_open = 64, threads = 1))]
    fn new(
        py: Python<'_>,
        key: Option<Bound<'_, PyAny>>,
        codec: Option<Bound<'_, PyAny>>,
        max_bytes: i64,
        tmp_dir: Option<PathBuf>,
        max_open: i64,
        threads: i64,
    ) -> PyResult<Self> {
        if let Some(function) = &key
            && !function.is_callable()
        {
            return Err(PyTypeError::new_err(format!(
                "key must be a function returning a sort key, not {}",
                type_name(function)
            )));
        }
        let (encoder, decode) = match codec {
            Some(codec) => {
                let method =
                    |name: &str| codec.getattr(name).ok().filter(PyAnyMethods::is_callable);
                let (Some(encode), Some(decode)) = (method("encode"), method("decode")) else {
                    return Err(PyTypeError::new_err(format!(
                        "a codec must have encode and decode methods, unlike {}",
                        type_name(&codec)
                    )));
                };
                (Encoder::Codec(encode.unbind()), decode.unbind())
            }
            None => {
                let pickle = py.import("pickle")?;
                let dumps = pickle.getattr("dumps")?.unbind();
                (Encoder::Pickle(dumps), pickle.getattr("loads")?.unbind())
            }
        };
        let tmp_dir = match tmp_dir {
            Some(dir) => dir,
            None => py
                .import("tempfile")?
                .call_method0("gettempdir")?
                .extract::<PathBuf>()?,
        };
        let config = Config {
            max_bytes: setting("max_bytes", max_bytes, MIN_MAX_BYTES)?,
            tmp_dir: Some(tmp_dir),
            max_open: setting("max_open", max_open, MIN_MAX_OPEN)?,
            threads: setting("threads", threads, 1)?,
        };
        let sorter = CoreSorter::new(config).map_err(to_python)?;
        Ok(PySorter {
            state: Arc::new(Lock::new(State::Adding(sorter))),
            status: AtomicU8::new(ADDING),
            added: AtomicU64::new(0),
            key: key.map(Bound::unbind),
            encoder,
            decode,
        })
    }

    /// Add an item, keyed and encoded now.
    ///
    /// Raises:
    ///     TypeError: If the key is not a sort key, or the codec does not return bytes.
    ///     ValueError: If the sorter is closed.
    ///     RuntimeError: If the sorter has been read, or an earlier error lost items.
    ///     OSError: If spilling to disk fails.
    #[pyo3(signature = (item, /))]
    fn add(&self, py: Python<'_>, item: &Bound<'_, PyAny>) -> PyResult<()> {
        self.check_adding()?;
        self.push_item(py, item, &mut Vec::with_capacity(64))
    }

    /// Add bytes that the codec would decode, with their sort key, calling neither.
    #[pyo3(signature = (key, payload, /))]
    fn add_encoded(&self, py: Python<'_>, key: &Bound<'_, PyAny>, payload: &[u8]) -> PyResult<()> {
        self.check_adding()?;
        self.push_encoded(py, key, payload, &mut Vec::with_capacity(64))
    }

    /// The number of items added.
    fn __len__(&self) -> usize {
        self.added.load(Ordering::Relaxed) as usize
    }

    /// Add every item given, then return every item added, decoded, in sorted order.
    ///
    /// Items are keyed and encoded as they are added. A sorter can be sorted only once; items
    /// given before one that fails stay added, and the sorter is not sorted. Once the iterator
    /// raises, as when decoding an item fails, it ends, as a generator does.
    ///
    /// Raises:
    ///     TypeError: If a key is not a sort key, or the codec does not return bytes.
    ///     ValueError: If the sorter is closed; also raised by the iterator once it is.
    ///     RuntimeError: If the sorter has been sorted, or an earlier error lost items.
    ///     OSError: If spilling, or reading what was spilled, fails.
    #[pyo3(signature = (items = None, /))]
    fn sort(&self, py: Python<'_>, items: Option<&Bound<'_, PyAny>>) -> PyResult<SortedIterator> {
        if let Some(items) = items {
            self.check_adding()?;
            let mut key = Vec::with_capacity(64);
            for item in items.try_iter()? {
                self.push_item(py, &item?, &mut key)?;
            }
        }
        self.iterator(py, Some(self.decode.clone_ref(py)))
    }

    /// Return the encoded items, as bytes, in sorted order, decoding none of them.
    fn encoded(&self, py: Python<'_>) -> PyResult<SortedIterator> {
        self.iterator(py, None)
    }

    /// Write every encoded item, in sorted order, to a binary handle, in chunks of about 1 MiB.
    ///
    /// The next chunk is merged on another thread while the handle writes this one. This reads
    /// the sorter, so it can be done only once.
    fn write_to(&self, py: Python<'_>, handle: &Bound<'_, PyAny>) -> PyResult<()> {
        let write = handle.getattr("write")?;
        let Some(mut merged) = self.finish(py, false)? else {
            return Ok(());
        };
        let (sender, receiver) = channel::sync_channel::<crate::Result<Vec<u8>>>(2);
        let merger = thread::Builder::new()
            .name("spillsort-merge".to_owned())
            .spawn(move || {
                loop {
                    let mut chunk = Vec::with_capacity(CHUNK);
                    let filled = (|| -> crate::Result<bool> {
                        while chunk.len() < CHUNK {
                            let Some((_, value)) = merged.next_record()? else {
                                return Ok(false);
                            };
                            chunk.extend_from_slice(value);
                        }
                        Ok(true)
                    })();
                    let more = matches!(filled, Ok(true));
                    if sender.send(filled.map(|_| chunk)).is_err() || !more {
                        return;
                    }
                }
            })?;
        let written = (|| -> PyResult<()> {
            loop {
                match py.detach(|| receiver.recv_timeout(POLL)) {
                    Ok(message) => write_all(&write, &message.map_err(to_python)?)?,
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => return Ok(()),
                }
                py.check_signals()?;
            }
        })();
        drop(receiver);
        py.detach(|| merger.join())
            .map_err(|_| PyRuntimeError::new_err("the merging thread panicked"))?;
        written
    }

    /// Drop every item and remove what was spilled; closing again does nothing.
    ///
    /// A `write_to` already running in another thread is not stopped: it writes every item, then
    /// removes what was spilled.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        self.status.store(CLOSED, Ordering::Release);
        py.detach(|| {
            let previous = mem::replace(&mut *self.state.lock()?, State::Closed);
            drop(previous);
            Ok(())
        })
    }

    fn __enter__(slf: Bound<'_, Self>) -> Bound<'_, Self> {
        slf
    }

    #[pyo3(signature = (*_exc_info))]
    fn __exit__(&self, py: Python<'_>, _exc_info: &Bound<'_, PyTuple>) -> PyResult<()> {
        self.close(py)
    }

    #[allow(clippy::needless_pass_by_value)]
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        if let Some(key) = &self.key {
            visit.call(key)?;
        }
        visit.call(self.encoder.function())?;
        visit.call(&self.decode)
    }
}

impl Drop for PySorter {
    fn drop(&mut self) {
        release(&mut self.state);
    }
}

/// Drops a sorter's state once nothing else shares it, without the GIL if it is attached, since
/// waiting for a spill and removing what was spilled can take a while.
fn release(state: &mut Arc<Lock<State>>) {
    let Some(lock) = Arc::get_mut(state) else {
        return;
    };
    let inner = lock.mutex.get_mut().unwrap_or_else(PoisonError::into_inner);
    let state = mem::replace(inner, State::Closed);
    if matches!(state, State::Adding(_) | State::Reading(..)) {
        Python::try_attach(|py| py.detach(move || drop(state)));
    }
}

/// Writes all of `data` to a handle, again after a short write.
fn write_all(write: &Bound<'_, PyAny>, data: &[u8]) -> PyResult<()> {
    let mut rest = data;
    while !rest.is_empty() {
        let written = write.call1((PyBytes::new(write.py(), rest),))?;
        match written.extract::<usize>() {
            Ok(0) => return Err(PyOSError::new_err("the handle wrote no bytes")),
            Ok(count) if count < rest.len() => rest = &rest[count..],
            _ => break,
        }
    }
    Ok(())
}

/// The items of a sorter, merged in order as they are read.
#[pyclass(module = "spillsort", name = "_SortedIterator", frozen)]
struct SortedIterator {
    state: Arc<Lock<State>>,
    decode: Option<Py<PyAny>>,
}

#[pymethods]
impl SortedIterator {
    fn __iter__(slf: Bound<'_, Self>) -> Bound<'_, Self> {
        slf
    }

    /// Returns the next item, merging the next batch first if this one is drained and no signal
    /// handler raises, which would end reading, as an error decoding the item does.
    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let value = match self.state.try_lock() {
            Some(mut guard) => {
                let state = &mut *guard;
                if state.needs_refill() {
                    if let Err(error) = py.check_signals() {
                        py.detach(|| state.end());
                        return Err(error);
                    }
                    py.detach(|| state.refill_if_drained())?;
                }
                state.next_value()?.map(|value| PyBytes::new(py, value))
            }
            None => {
                let mut raised = None;
                let value = py.detach(|| -> PyResult<Option<Vec<u8>>> {
                    let mut state = self.state.lock()?;
                    if state.needs_refill() && interrupted(&mut raised) {
                        state.end();
                        return Ok(None);
                    }
                    state.refill_if_drained()?;
                    Ok(state.next_value()?.map(<[u8]>::to_vec))
                })?;
                if let Some(error) = raised {
                    return Err(error);
                }
                value.map(|value| PyBytes::new(py, &value))
            }
        };
        let Some(value) = value else {
            return Ok(None);
        };
        match &self.decode {
            Some(decode) => match decode.bind(py).call1((value,)) {
                Ok(item) => Ok(Some(item.unbind())),
                Err(error) => {
                    py.detach(|| {
                        if let Ok(mut state) = self.state.lock() {
                            state.end();
                        }
                    });
                    Err(error)
                }
            },
            None => Ok(Some(value.into_any().unbind())),
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        if let Some(decode) = &self.decode {
            visit.call(decode)?;
        }
        Ok(())
    }
}

impl Drop for SortedIterator {
    fn drop(&mut self) {
        release(&mut self.state);
    }
}

#[pymodule(gil_used = false)]
fn _spillsort(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySorter>()?;
    Ok(())
}
