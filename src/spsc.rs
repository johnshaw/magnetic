//! Single-producer single-consumer queue
//!
//! The SPSC queue allows for pushing from one thread and popping from another.
//! Each end of the queue can only be owned and accessed from a single thread.
//! In other words, both the `SPSCProducer` and `SPSCConsumer` are `Send` and
//! `!Sync`.

use std::cell::Cell;
use std::hint::spin_loop;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_utils::CachePadded;

use super::buffer::Buffer;
use super::{Consumer, PopError, Producer, PushError, TryPopError, TryPushError};
use crate::util::{buf_read, buf_write};

struct SPSCQueue<T, B: Buffer<T>> {
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    buf: B,
    _marker: PhantomData<T>,
}

/// Consumer end of the queue. Implements the trait `Consumer<T>`.
pub struct SPSCConsumer<T, B: Buffer<T>> {
    queue: Arc<SPSCQueue<T, B>>,
    /// A copy of `queue.head` for quick access.
    ///
    /// This value can be stale and sometimes needs to be resynchronized with `queue.head`.
    cached_head: Cell<usize>,
}

/// Producer end of the queue. Implements the trait `Producer<T>`.
pub struct SPSCProducer<T, B: Buffer<T>> {
    queue: Arc<SPSCQueue<T, B>>,
    /// A copy of `queue.tail` for quick access.
    ///
    /// This value can be stale and sometimes needs to be resynchronized with `queue.tail`.
    cached_tail: Cell<usize>,
}

/// Creates a new SPSC queue
///
/// # Examples
///
/// ```
/// use magnetic::spsc::spsc_queue;
/// use magnetic::buffer::dynamic::DynamicBuffer;
/// use magnetic::{Producer, Consumer};
///
/// let (p, c) = spsc_queue(DynamicBuffer::new(32).unwrap());
///
/// p.push(1).unwrap();
/// assert_eq!(c.pop(), Ok(1));
/// ```
pub fn spsc_queue<T, B: Buffer<T>>(buf: B) -> (SPSCProducer<T, B>, SPSCConsumer<T, B>) {
    let queue = SPSCQueue {
        head: CachePadded::new(AtomicUsize::new(0)),
        tail: CachePadded::new(AtomicUsize::new(0)),
        buf,
        _marker: PhantomData,
    };

    let queue = Arc::new(queue);

    (
        SPSCProducer {
            queue: queue.clone(),
            cached_tail: Cell::new(0),
        },
        SPSCConsumer {
            queue,
            cached_head: Cell::new(0),
        },
    )
}

impl<T, B: Buffer<T>> Drop for SPSCQueue<T, B> {
    fn drop(&mut self) {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        for pos in tail..head {
            unsafe { buf_read(&self.buf, pos) };
        }
    }
}

impl<T, B: Buffer<T>> Producer<T> for SPSCProducer<T, B> {
    fn is_closed(&self) -> bool {
        Arc::strong_count(&self.queue) < 2
    }

    fn push(&self, value: T) -> Result<(), PushError<T>> {
        let q = &self.queue;
        let head = q.head.load(Ordering::Relaxed);

        if self.cached_tail.get() + q.buf.size() <= head {
            loop {
                let tail = q.tail.load(Ordering::Acquire);
                if tail + q.buf.size() > head {
                    self.cached_tail.set(tail);
                    break;
                } else if Arc::strong_count(q) < 2 {
                    return Err(PushError::Disconnected(value));
                }
                spin_loop();
            }
        }

        unsafe { buf_write(&q.buf, head, value) };
        q.head.store(head + 1, Ordering::Release);
        Ok(())
    }

    fn try_push(&self, value: T) -> Result<(), TryPushError<T>> {
        let q = &self.queue;
        let head = q.head.load(Ordering::Relaxed);

        if self.cached_tail.get() + q.buf.size() <= head {
            let tail = q.tail.load(Ordering::Acquire);
            if tail + q.buf.size() <= head {
                return if Arc::strong_count(q) < 2 {
                    Err(TryPushError::Disconnected(value))
                } else {
                    Err(TryPushError::Full(value))
                };
            }
            self.cached_tail.set(tail);
        }

        unsafe { buf_write(&q.buf, head, value) };
        q.head.store(head + 1, Ordering::Release);
        Ok(())
    }
}

impl<T, B: Buffer<T>> Consumer<T> for SPSCConsumer<T, B> {
    fn is_closed(&self) -> bool {
        Arc::strong_count(&self.queue) < 2
    }

    fn pop(&self) -> Result<T, PopError> {
        let q = &self.queue;
        let tail = q.tail.load(Ordering::Relaxed);
        let tail_plus_one = tail + 1;

        if self.cached_head.get() < tail_plus_one {
            loop {
                let head = q.head.load(Ordering::Acquire);
                if head >= tail_plus_one {
                    self.cached_head.set(head);
                    break;
                } else if Arc::strong_count(q) < 2 {
                    return Err(PopError::Disconnected);
                }
                spin_loop();
            }
        }

        let v = unsafe { buf_read(&q.buf, tail) };
        q.tail.store(tail_plus_one, Ordering::Release);
        Ok(v)
    }

    fn try_pop(&self) -> Result<T, TryPopError> {
        let q = &self.queue;
        let tail = q.tail.load(Ordering::Relaxed);
        let tail_plus_one = tail + 1;

        if self.cached_head.get() < tail_plus_one {
            let head = q.head.load(Ordering::Acquire);
            if head < tail_plus_one {
                return if Arc::strong_count(q) < 2 {
                    Err(TryPopError::Disconnected)
                } else {
                    Err(TryPopError::Empty)
                };
            }
            self.cached_head.set(head);
        }

        let v = unsafe { buf_read(&q.buf, tail) };
        q.tail.store(tail_plus_one, Ordering::Release);
        Ok(v)
    }
}

#[cfg(test)]
mod test {
    use std::thread::spawn;

    use super::super::buffer::dynamic::DynamicBuffer;
    use super::super::{Consumer, Producer, TryPopError, TryPushError};
    use super::*;

    #[test]
    fn one_thread() {
        let (p, c) = spsc_queue(DynamicBuffer::new(2).unwrap());

        p.push(1).unwrap();
        p.push(2).unwrap();
        assert_eq!(p.try_push(3), Err(TryPushError::Full(3)));
        assert_eq!(c.pop(), Ok(1));
        assert_eq!(p.try_push(4), Ok(()));
        assert_eq!(c.pop(), Ok(2));
        assert_eq!(c.try_pop(), Ok(4));
        assert_eq!(c.try_pop(), Err(TryPopError::Empty));
    }

    #[test]
    fn two_thread_seq() {
        let (p, c) = spsc_queue(DynamicBuffer::new(3).unwrap());

        let p = spawn(move || {
            p.push(vec![1; 5]).unwrap();
            p.push(vec![2; 7]).unwrap();
            p.push(vec![3; 3]).unwrap();
            p
        })
        .join()
        .unwrap();

        let c = spawn(move || {
            assert_eq!(c.pop(), Ok(vec![1; 5]));
            assert_eq!(c.pop(), Ok(vec![2; 7]));
            assert_eq!(c.pop(), Ok(vec![3; 3]));
            assert_eq!(c.try_pop(), Err(TryPopError::Empty));
            c
        })
        .join()
        .unwrap();

        drop(p);

        assert_eq!(c.try_pop(), Err(TryPopError::Disconnected));
    }

    #[test]
    fn two_thread_par() {
        let (p, c) = spsc_queue(DynamicBuffer::new(32).unwrap());

        let count = 10_000_000;

        let t1 = spawn(move || {
            for i in 0..count {
                p.push(i).unwrap();
            }
        });

        let t2 = spawn(move || {
            for i in 0..count {
                assert_eq!(c.pop(), Ok(i));
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }

    #[test]
    fn disconnect() {
        let (p, c) = spsc_queue(DynamicBuffer::new(3).unwrap());
        p.push(1).unwrap();
        p.push(2).unwrap();
        std::mem::drop(p);
        assert_eq!(c.pop(), Ok(1));
        assert_eq!(c.pop(), Ok(2));
        assert_eq!(c.pop(), Err(PopError::Disconnected));
        assert_eq!(c.try_pop(), Err(TryPopError::Disconnected));

        let (p, c) = spsc_queue(DynamicBuffer::new(2).unwrap());
        p.push(1).unwrap();
        std::mem::drop(c);
        assert!(p.is_closed());
        p.push(1).unwrap();
        assert_eq!(p.push(2), Err(PushError::Disconnected(2)));
        assert_eq!(p.try_push(2), Err(TryPushError::Disconnected(2)));

        let (p, c) = spsc_queue(DynamicBuffer::new(1).unwrap());
        p.push(1).unwrap();
        assert_eq!(p.try_push(2), Err(TryPushError::Full(2)));
        std::mem::drop(c);
        assert_eq!(p.push(2), Err(PushError::Disconnected(2)));
        assert_eq!(p.try_push(2), Err(TryPushError::Disconnected(2)));
    }
}
