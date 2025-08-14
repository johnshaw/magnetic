//! Single-producer multiple-consumer queue
//!
//! The SPMC queue allows for pushing from one thread and popping from another.
//! The producer end of the queue may be accessed by a single thread while the
//! consumer end may be accessed by multiple threads. In other words,
//! `SPMCProducer` is `Send` and `!Sync` while `SPMCConsumer` is `Send` and
//! `Sync`.

use std::cell::Cell;
use std::hint::spin_loop;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_utils::CachePadded;

use super::buffer::Buffer;
use super::{Consumer, PopError, Producer, PushError, TryPopError, TryPushError};
use crate::util::{buf_read, buf_write, AtomicPair};

struct SPMCQueue<T, B: Buffer<T>> {
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicPair>,
    buf: B,
    producer: AtomicBool,
    _marker: PhantomData<T>,
}

/// Consumer end of the queue. Implements the trait `Consumer<T>`.
pub struct SPMCConsumer<T, B: Buffer<T>> {
    queue: Arc<SPMCQueue<T, B>>,
}

unsafe impl<T: Send, B: Buffer<T>> Sync for SPMCConsumer<T, B> {}

impl<T, B: Buffer<T>> Clone for SPMCConsumer<T, B> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
        }
    }
}

/// Producer end of the queue. Implements the trait `Producer<T>`.
pub struct SPMCProducer<T, B: Buffer<T>> {
    queue: Arc<SPMCQueue<T, B>>,
    /// A copy of `queue.tail` for quick access.
    ///
    /// This value can be stale and sometimes needs to be resynchronized with `queue.tail`.
    cached_tail: Cell<usize>,
}

/// Creates a new SPMC queue
///
/// # Examples
///
/// ```
/// use magnetic::spmc::spmc_queue;
/// use magnetic::buffer::dynamic::DynamicBuffer;
/// use magnetic::{Producer, Consumer};
///
/// let (p, c) = spmc_queue(DynamicBuffer::new(32).unwrap());
///
/// p.push(1).unwrap();
/// assert_eq!(c.pop(), Ok(1));
/// ```
pub fn spmc_queue<T, B: Buffer<T>>(buf: B) -> (SPMCProducer<T, B>, SPMCConsumer<T, B>) {
    let queue = SPMCQueue {
        head: CachePadded::new(AtomicUsize::new(0)),
        tail: CachePadded::new(AtomicPair::default()),
        buf,
        producer: AtomicBool::new(true),
        _marker: PhantomData,
    };

    let queue = Arc::new(queue);

    (
        SPMCProducer {
            queue: queue.clone(),
            cached_tail: Cell::new(0),
        },
        SPMCConsumer { queue },
    )
}

impl<T, B: Buffer<T>> Drop for SPMCQueue<T, B> {
    fn drop(&mut self) {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.curr.load(Ordering::Relaxed);
        for pos in tail..head {
            unsafe { buf_read(&self.buf, pos) };
        }
    }
}

impl<T, B: Buffer<T>> Producer<T> for SPMCProducer<T, B> {
    fn is_closed(&self) -> bool {
        Arc::strong_count(&self.queue) < 2
    }

    fn push(&self, value: T) -> Result<(), PushError<T>> {
        let q = &self.queue;
        let head = q.head.load(Ordering::Relaxed);

        if self.cached_tail.get() + q.buf.size() <= head {
            loop {
                let tail = q.tail.curr.load(Ordering::Acquire);
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
            let tail = q.tail.curr.load(Ordering::Acquire);
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

impl<T, B: Buffer<T>> Consumer<T> for SPMCConsumer<T, B> {
    fn is_closed(&self) -> bool {
        !self.queue.producer.load(Ordering::Relaxed)
    }

    fn pop(&self) -> Result<T, PopError> {
        let q = &self.queue;
        let tail = q.tail.next.fetch_add(1, Ordering::Relaxed);
        let tail_plus_one = tail + 1;

        loop {
            if q.head.load(Ordering::Acquire) >= tail_plus_one {
                break;
            } else if !q.producer.load(Ordering::Relaxed) {
                return Err(PopError::Disconnected);
            }
            spin_loop();
        }

        let v = unsafe { buf_read(&q.buf, tail) };
        while q.tail.curr.load(Ordering::Relaxed) < tail {
            spin_loop();
        }
        q.tail.curr.store(tail_plus_one, Ordering::Release);
        Ok(v)
    }

    fn try_pop(&self) -> Result<T, TryPopError> {
        let q = &self.queue;
        loop {
            let tail = q.tail.curr.load(Ordering::Relaxed);
            let tail_plus_one = tail + 1;

            if q.head.load(Ordering::Acquire) < tail_plus_one {
                // buffer is empty, check whether it's closed.
                // relaxed is fine since Producer.drop does an acquire/release on .head
                return if !q.producer.load(Ordering::Relaxed) {
                    Err(TryPopError::Disconnected)
                } else {
                    Err(TryPopError::Empty)
                };
            } else if q
                .tail
                .next
                .compare_exchange_weak(tail, tail_plus_one, Ordering::Acquire, Ordering::Acquire)
                .is_ok()
            {
                let v = unsafe { buf_read(&q.buf, tail) };
                q.tail.curr.store(tail_plus_one, Ordering::Release);
                return Ok(v);
            }
        }
    }
}

impl<T, B: Buffer<T>> Drop for SPMCProducer<T, B> {
    fn drop(&mut self) {
        self.queue.producer.store(false, Ordering::Relaxed);
        // Acquire/Release .head to ensure other threads see new .closed
        self.queue.head.fetch_add(0, Ordering::AcqRel);
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
        let (p, c) = spmc_queue(DynamicBuffer::new(2).unwrap());

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
        let (p, c) = spmc_queue(DynamicBuffer::new(3).unwrap());

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
    fn four_thread_par() {
        if num_cpus::get() < 4 {
            // Test will not finish in time
            eprintln!("skipping four thread test due to cpu resource constraints");
            return;
        }

        let (p, c) = spmc_queue(DynamicBuffer::new(32).unwrap());

        let count = 10_000_000;
        let total = count * 3;

        let t1 = spawn(move || {
            for i in 0..total {
                p.push(i).unwrap();
            }
        });

        let mut consumers = Vec::new();
        for _ in 0..3 {
            let c = c.clone();
            consumers.push(spawn(move || {
                (0..count).fold(0u64, |a, _| a + c.pop().unwrap())
            }));
        }

        t1.join().unwrap();
        let sum: u64 = consumers.into_iter().map(|t| t.join().unwrap()).sum();
        assert_eq!(sum, ((total - 1) * ((total - 1) + 1)) / 2);
    }

    #[test]
    fn disconnect() {
        let (p, c) = spmc_queue(DynamicBuffer::new(32).unwrap());
        p.push(1).unwrap();
        p.push(2).unwrap();
        std::mem::drop(p);
        assert_eq!(c.pop(), Ok(1));
        assert_eq!(c.pop(), Ok(2));
        assert_eq!(c.pop(), Err(PopError::Disconnected));
        assert_eq!(c.try_pop(), Err(TryPopError::Disconnected));

        let (p, c) = spmc_queue(DynamicBuffer::new(2).unwrap());
        p.push(1).unwrap();
        std::mem::drop(c);
        assert!(p.is_closed());
        p.push(1).unwrap();
        assert_eq!(p.push(2), Err(PushError::Disconnected(2)));
        assert_eq!(p.try_push(2), Err(TryPushError::Disconnected(2)));

        let (p, c) = spmc_queue(DynamicBuffer::new(1).unwrap());
        p.push(1).unwrap();
        assert_eq!(p.try_push(2), Err(TryPushError::Full(2)));
        std::mem::drop(c);
        assert_eq!(p.push(2), Err(PushError::Disconnected(2)));
        assert_eq!(p.try_push(2), Err(TryPushError::Disconnected(2)));
    }
}
