//! Magnetic contains a set of high-performance queues useful for developing
//! low-latency applications. All queues are FIFO unless otherwise specified.
//!
//! # Examples
//!
//! ```
//! use std::thread::spawn;
//! use magnetic::spsc::spsc_queue;
//! use magnetic::buffer::dynamic::DynamicBuffer;
//! use magnetic::{Producer, Consumer};
//!
//! let (p, c) = spsc_queue(DynamicBuffer::new(32).unwrap());
//!
//! // Push and pop within a single thread
//! p.push(1).unwrap();
//! assert_eq!(c.pop(), Ok(1));
//!
//! // Push and pop from multiple threads. Since this example is using the
//! // SPSC queue, only one producer and one consumer are allowed.
//! let t1 = spawn(move || {
//!     for i in 0..10 {
//!         println!("Producing {}", i);
//!         p.push(i).unwrap();
//!     }
//!     p
//! });
//!
//! let t2 = spawn(move || {
//!     loop {
//!         let i = c.pop().unwrap();
//!         println!("Consumed {}", i);
//!         if i == 9 { break; }
//!     }
//! });
//!
//! t1.join().unwrap();
//! t2.join().unwrap();
//! ```

#![deny(missing_docs)]

use std::fmt;

pub mod buffer;
pub mod mpmc;
pub mod mpsc;
pub mod spmc;
pub mod spsc;
mod util;

/// Possible errors for `Producer::push`
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PushError<T> {
    /// Consumer was destroyed
    Disconnected(T),
}

impl<T> fmt::Debug for PushError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disconnected(_) => f.pad("Disconnected(_)"),
        }
    }
}

impl<T> fmt::Display for PushError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disconnected(_) => "queue abandoned".fmt(f),
        }
    }
}

impl<T> std::error::Error for PushError<T> {}

/// Possible errors for `Producer::try_push`
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TryPushError<T> {
    /// Queue was full
    Full(T),
    /// Consumer was destroyed
    Disconnected(T),
}

impl<T> fmt::Debug for TryPushError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => f.pad("Full(_)"),
            Self::Disconnected(_) => f.pad("Disconnected(_)"),
        }
    }
}

impl<T> fmt::Display for TryPushError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => "queue full".fmt(f),
            Self::Disconnected(_) => "queue abandoned".fmt(f),
        }
    }
}

impl<T> std::error::Error for TryPushError<T> {}

/// Possible errors for `Consumer::pop`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PopError {
    /// Producer was destroyed
    Disconnected,
}

impl fmt::Display for PopError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disconnected => "queue abandoned".fmt(f),
        }
    }
}

impl std::error::Error for PopError {}

/// Possible errors for `Consumer::try_pop`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryPopError {
    /// Queue was empty
    Empty,
    /// Producer was destroyed
    Disconnected,
}

impl fmt::Display for TryPopError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => "queue empty".fmt(f),
            Self::Disconnected => "queue abandoned".fmt(f),
        }
    }
}

impl std::error::Error for TryPopError {}

/// The consumer end of the queue allows for sending data. `Producer<T>` is
/// always `Send`, but is only `Sync` for multi-producer (MPSC, MPMC) queues.
pub trait Producer<T> {
    /// Check if this channel is closed.
    fn is_closed(&self) -> bool;

    /// Add value to front of the queue. This method will block if the queue
    /// is currently full.
    /// If the channel is closed, this function will continue succeeding until
    /// the queue becomes full.
    fn push(&self, value: T) -> Result<(), PushError<T>>;

    /// Attempt to add a value to the front of the queue. If the value was
    /// added successfully, `None` will be returned. If unsuccessful, `value`
    /// will be returned. An unsuccessful push indicates that the queue was
    /// full.
    /// If the channel is closed, this function will continue succeeding until
    /// the queue becomes full.
    fn try_push(&self, value: T) -> Result<(), TryPushError<T>>;
}

/// The consumer end of the queue allows for receiving data. `Consumer<T>` is
/// always `Send`, but is only `Sync` for multi-consumer (SPMC, MPMC) queues.
pub trait Consumer<T> {
    /// Check if this channel is closed.
    fn is_closed(&self) -> bool;

    /// Remove value from the end of the queue. This method will block if the
    /// queue is currently empty.
    fn pop(&self) -> Result<T, PopError>;

    /// Attempt to remove a value from the end of the queue. If the value was
    /// removed successfully, `Some(T)` will be returned. If unsuccessful,
    /// `None` will be returned. An unsuccessful pop indicates that the queue
    /// was empty.
    fn try_pop(&self) -> Result<T, TryPopError>;
}
