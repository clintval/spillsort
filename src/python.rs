//! The `spillsort._spillsort` extension module.

#![allow(clippy::doc_markdown)]

use std::mem;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, TryLockError};
use std::thread;

use pyo3::exceptions::{PyOSError, PyRecursionError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyBytes, PyFloat, PyInt, PyList, PyString, PyTuple};
use pyo3::{PyTraverseError, PyVisit};

use crate::key;
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
        Error::Io(error) => error.into(),
        Error::Invalid(message) => PyValueError::new_err(message),
        error @ Error::Failed => PyRuntimeError::new_err(error.to_string()),
    }
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
        if filled.is_err() || batch.is_drained() {
            *self = State::Read;
        }
        filled.map_err(to_python)
    }

    fn needs_refill(&self) -> bool {
        matches!(self, State::Reading(_, batch) if batch.is_drained())
    }

    fn next_value(&mut self) -> PyResult<Option<&[u8]>> {
        match self {
            State::Reading(_, batch) => Ok(batch.take()),
            State::Adding(_) | State::Read => Ok(None),
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

/// Sort more items than fit in memory: add them, then read them once, sorted.
///
/// Each item is turned into a sort key by `key`, or is its own key, and into bytes by `codec`,
/// or by pickle, once, as it is added. Keys and bytes are held in up to `max_bytes` of memory.
/// Once half of that fills, a background thread sorts it and writes it to a private directory in
/// `tmp_dir` while the other half fills. Reading merges what was written with what is held, so
/// items come out in key order, and items with equal keys come out in the order they were added.
///
/// The private directory is removed when reading ends, on `close()`, when a `with` block exits,
/// or when the sorter is garbage collected. Threads sharing a sorter take turns.
///
/// Args:
///     key: A function returning the sort key of an item: an int, float, str, bytes, None, or a
///         tuple or list of them, compared as Python compares them. None sorts items by
///         themselves.
///     codec: Encodes each item into bytes and decodes it back; pickle if None.
///     max_bytes: The most bytes of keys and encoded items to hold in memory, at least 2.
///     tmp_dir: The directory to spill into, or None for the system's temporary directory.
///     max_open: The most spilled runs to merge at once, at least 2; more are merged in passes.
///
/// Raises:
///     TypeError: If `key` is not callable, or `codec` lacks `encode` and `decode` methods.
///     ValueError: If `max_bytes` or `max_open` is less than 2.
///     OSError: If `tmp_dir` is not a directory.
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
            _ => Err(added_after_read_error()),
        }
    }

    fn push(&self, py: Python<'_>, key: &[u8], payload: &[u8]) -> PyResult<()> {
        let push = |state: &mut State| match state {
            State::Adding(sorter) => sorter.push(key, payload).map_err(to_python),
            State::Reading(..) | State::Read => Err(added_after_read_error()),
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
                    let merged = sorter.finish().map_err(to_python)?;
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
    #[pyo3(signature = (key = None, /, *, codec = None, max_bytes = 268_435_456, tmp_dir = None, max_open = 64))]
    fn new(
        py: Python<'_>,
        key: Option<Bound<'_, PyAny>>,
        codec: Option<Bound<'_, PyAny>>,
        max_bytes: i64,
        tmp_dir: Option<PathBuf>,
        max_open: i64,
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
        let config = Config {
            max_bytes: setting("max_bytes", max_bytes, MIN_MAX_BYTES)?,
            tmp_dir,
            max_open: setting("max_open", max_open, MIN_MAX_OPEN)?,
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
    fn add(&self, py: Python<'_>, item: &Bound<'_, PyAny>) -> PyResult<()> {
        self.check_adding()?;
        self.push_item(py, item, &mut Vec::with_capacity(64))
    }

    /// Add every item, keyed and encoded now; items before one that fails stay added.
    fn extend(&self, py: Python<'_>, items: &Bound<'_, PyAny>) -> PyResult<()> {
        self.check_adding()?;
        let mut key = Vec::with_capacity(64);
        for item in items.try_iter()? {
            self.push_item(py, &item?, &mut key)?;
        }
        Ok(())
    }

    /// Add bytes that the codec would decode, with their sort key, calling neither.
    fn add_encoded(&self, py: Python<'_>, key: &Bound<'_, PyAny>, payload: &[u8]) -> PyResult<()> {
        self.check_adding()?;
        self.push_encoded(py, key, payload, &mut Vec::with_capacity(64))
    }

    /// Add every `(key, payload)` pair, calling neither the key function nor the codec.
    fn extend_encoded(&self, py: Python<'_>, pairs: &Bound<'_, PyAny>) -> PyResult<()> {
        self.check_adding()?;
        let mut buffer = Vec::with_capacity(64);
        for pair in pairs.try_iter()? {
            let pair = pair?;
            let (key, payload) = pair
                .extract::<(Bound<'_, PyAny>, Bound<'_, PyBytes>)>()
                .map_err(|_| {
                    PyTypeError::new_err(format!(
                        "extend_encoded takes (key, bytes) tuples, not {}",
                        type_name(&pair)
                    ))
                })?;
            self.push_encoded(py, &key, payload.as_bytes(), &mut buffer)?;
        }
        Ok(())
    }

    /// The number of items added.
    fn __len__(&self) -> usize {
        self.added.load(Ordering::Relaxed) as usize
    }

    /// Return the items, decoded, in sorted order; a sorter can be read only once.
    ///
    /// Raises:
    ///     ValueError: If the sorter is closed; also raised by the iterator once it is.
    ///     RuntimeError: If the sorter has been read, or an earlier error lost items.
    ///     OSError: If reading what was spilled fails.
    fn __iter__(&self, py: Python<'_>) -> PyResult<SortedIterator> {
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
        let (sender, receiver) = mpsc::sync_channel::<crate::Result<Vec<u8>>>(2);
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
        let mut receiver = Some(receiver);
        let written = (|| -> PyResult<()> {
            loop {
                let waiting = receiver
                    .take()
                    .expect("the receiver is put back after each message");
                let (message, waiting) = py.detach(move || (waiting.recv(), waiting));
                receiver = Some(waiting);
                let Ok(message) = message else {
                    return Ok(());
                };
                write_all(&write, &message.map_err(to_python)?)?;
            }
        })();
        drop(receiver);
        py.detach(|| merger.join())
            .map_err(|_| PyRuntimeError::new_err("the merging thread panicked"))?;
        written
    }

    /// Drop every item and remove what was spilled; closing again does nothing.
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
#[pyclass(module = "spillsort", frozen, generic)]
struct SortedIterator {
    state: Arc<Lock<State>>,
    decode: Option<Py<PyAny>>,
}

#[pymethods]
impl SortedIterator {
    fn __iter__(slf: Bound<'_, Self>) -> Bound<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let value = match self.state.try_lock() {
            Some(mut guard) => {
                let state = &mut *guard;
                if state.needs_refill() {
                    py.detach(|| state.refill_if_drained())?;
                }
                state.next_value()?.map(|value| PyBytes::new(py, value))
            }
            None => py
                .detach(|| -> PyResult<Option<Vec<u8>>> {
                    let mut state = self.state.lock()?;
                    state.refill_if_drained()?;
                    Ok(state.next_value()?.map(<[u8]>::to_vec))
                })?
                .map(|value| PyBytes::new(py, &value)),
        };
        let Some(value) = value else {
            return Ok(None);
        };
        match &self.decode {
            Some(decode) => Ok(Some(decode.bind(py).call1((value,))?.unbind())),
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

#[pymodule(gil_used = false)]
fn _spillsort(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySorter>()?;
    m.add_class::<SortedIterator>()?;
    Ok(())
}
