//! A bounded channel whose waits block on a condition variable rather than on std's thread parker,
//! which on macOS is a semaphore that a forked child cannot wait on once its parent has.

use std::collections::VecDeque;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

struct Shared<T> {
    state: Mutex<State<T>>,
    changed: Condvar,
}

struct State<T> {
    queue: VecDeque<T>,
    capacity: usize,
    sending: bool,
    receiving: bool,
}

impl<T> Shared<T> {
    fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Sends values to a [`Receiver`], in order.
pub(crate) struct Sender<T>(Arc<Shared<T>>);

/// Receives the values a [`Sender`] sent, in order.
pub(crate) struct Receiver<T>(Arc<Shared<T>>);

/// A channel that holds up to `capacity` values, and at least one, that are not yet received.
pub(crate) fn sync_channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            queue: VecDeque::with_capacity(capacity),
            capacity: capacity.max(1),
            sending: true,
            receiving: true,
        }),
        changed: Condvar::new(),
    });
    (Sender(Arc::clone(&shared)), Receiver(shared))
}

impl<T> Sender<T> {
    /// Sends a value, waiting while the channel is full, or gives it back once nothing receives.
    pub fn send(&self, value: T) -> Result<(), T> {
        let state = self.0.lock();
        let mut state = self
            .0
            .changed
            .wait_while(state, |state| {
                state.receiving && state.queue.len() == state.capacity
            })
            .unwrap_or_else(PoisonError::into_inner);
        if !state.receiving {
            return Err(value);
        }
        state.queue.push_back(value);
        self.0.changed.notify_all();
        Ok(())
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.0.lock().sending = false;
        self.0.changed.notify_all();
    }
}

impl<T> Receiver<T> {
    /// Receives the next value, waiting at most `timeout` for one, or fails once none is left and
    /// the sender is gone.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        let state = self.0.lock();
        let (mut state, _) = self
            .0
            .changed
            .wait_timeout_while(state, timeout, |state| {
                state.queue.is_empty() && state.sending
            })
            .unwrap_or_else(PoisonError::into_inner);
        match state.queue.pop_front() {
            Some(value) => {
                self.0.changed.notify_all();
                Ok(value)
            }
            None if state.sending => Err(RecvTimeoutError::Timeout),
            None => Err(RecvTimeoutError::Disconnected),
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let unread = {
            let mut state = self.0.lock();
            state.receiving = false;
            std::mem::take(&mut state.queue)
        };
        self.0.changed.notify_all();
        drop(unread);
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Instant;

    use super::*;

    const WAIT: Duration = Duration::from_secs(10);

    #[test]
    fn values_arrive_in_order_and_then_the_channel_disconnects() {
        let (sender, receiver) = sync_channel(2);
        let sending = thread::spawn(move || {
            for value in 0..1_000 {
                sender.send(value).unwrap();
            }
        });
        let received: Vec<i32> = (0..1_000)
            .map(|_| receiver.recv_timeout(WAIT).unwrap())
            .collect();
        sending.join().unwrap();
        assert_eq!(received, (0..1_000).collect::<Vec<_>>());
        assert_eq!(
            receiver.recv_timeout(WAIT),
            Err(RecvTimeoutError::Disconnected)
        );
    }

    #[test]
    fn a_receive_times_out_while_the_sender_lives() {
        let (sender, receiver) = sync_channel::<()>(1);
        let started = Instant::now();
        let timeout = Duration::from_millis(20);
        assert_eq!(
            receiver.recv_timeout(timeout),
            Err(RecvTimeoutError::Timeout)
        );
        assert!(started.elapsed() >= timeout);
        drop(sender);
    }

    #[test]
    fn a_waiting_receive_wakes_when_the_sender_drops() {
        let (sender, receiver) = sync_channel::<()>(1);
        let dropping = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            drop(sender);
        });
        assert_eq!(
            receiver.recv_timeout(WAIT),
            Err(RecvTimeoutError::Disconnected)
        );
        dropping.join().unwrap();
    }

    #[test]
    fn a_full_channel_gives_a_value_back_once_the_receiver_drops() {
        let (sender, receiver) = sync_channel(1);
        sender.send(1).unwrap();
        let sending = thread::spawn(move || sender.send(2));
        thread::sleep(Duration::from_millis(20));
        drop(receiver);
        assert_eq!(sending.join().unwrap(), Err(2));
    }
}
