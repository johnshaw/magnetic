//! Single-producer multiple-consumer queue
//!
//! The SPMC queue allows for pushing from one thread and popping from another.
//! The producer end of the queue may be accessed by a single thread while the
//! consumer end may be accessed by multiple threads. In other words,
//! `SPMCProducer` is `Send` and `!Sync` while `SPMCConsumer` is `Send` and
//! `Sync`.

use std::cell::UnsafeCell;
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
    ok: AtomicBool,
    _marker: PhantomData<T>,
}

unsafe impl<T, B: Buffer<T>> Sync for SPMCQueue<T, B> {}

/// Consumer end of the queue. Implements the trait `Consumer<T>`.
pub struct SPMCConsumer<T, B: Buffer<T>> {
    queue: Arc<UnsafeCell<SPMCQueue<T, B>>>,
}

unsafe impl<T: Send, B: Buffer<T>> Send for SPMCConsumer<T, B> {}
unsafe impl<T: Send, B: Buffer<T>> Sync for SPMCConsumer<T, B> {}

/// Producer end of the queue. Implements the trait `Producer<T>`.
pub struct SPMCProducer<T, B: Buffer<T>> {
    queue: Arc<UnsafeCell<SPMCQueue<T, B>>>,
}

unsafe impl<T: Send, B: Buffer<T>> Send for SPMCProducer<T, B> {}

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
        ok: AtomicBool::new(true),
        _marker: PhantomData,
    };

    let queue = Arc::new(UnsafeCell::new(queue));

    (
        SPMCProducer {
            queue: queue.clone(),
        },
        SPMCConsumer { queue },
    )
}

impl<T, B: Buffer<T>> Drop for SPMCQueue<T, B> {
    fn drop(&mut self) {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.curr.load(Ordering::Relaxed);
        for pos in tail..head {
            buf_read(&self.buf, pos);
        }
    }
}

impl<T, B: Buffer<T>> Producer<T> for SPMCProducer<T, B> {
    fn push(&self, value: T) -> Result<(), PushError<T>> {
        let q = unsafe { &mut *self.queue.get() };
        let head = q.head.load(Ordering::Relaxed);

        loop {
            if !q.ok.load(Ordering::Acquire) {
                return Err(PushError::Disconnected(value));
            } else if q.tail.curr.load(Ordering::Acquire) + q.buf.size() > head {
                break;
            }
            spin_loop();
        }

        buf_write(&mut q.buf, head, value);
        q.head.store(head + 1, Ordering::Release);
        Ok(())
    }

    fn try_push(&self, value: T) -> Result<(), TryPushError<T>> {
        let q = unsafe { &mut *self.queue.get() };
        let head = q.head.load(Ordering::Relaxed);
        if !q.ok.load(Ordering::Acquire) {
            Err(TryPushError::Disconnected(value))
        } else if q.tail.curr.load(Ordering::Acquire) + q.buf.size() <= head {
            Err(TryPushError::Full(value))
        } else {
            buf_write(&mut q.buf, head, value);
            q.head.store(head + 1, Ordering::Release);
            Ok(())
        }
    }
}

impl<T, B: Buffer<T>> Consumer<T> for SPMCConsumer<T, B> {
    fn pop(&self) -> Result<T, PopError> {
        let q = unsafe { &mut *self.queue.get() };

        let tail = q.tail.next.fetch_add(1, Ordering::Relaxed);
        let tail_plus_one = tail + 1;
        loop {
            if tail_plus_one <= q.head.load(Ordering::Acquire) {
                break;
            } else if !q.ok.load(Ordering::Acquire) {
                return Err(PopError::Disconnected);
            }
            spin_loop();
        }

        let v = buf_read(&q.buf, tail);

        while q.tail.curr.load(Ordering::Relaxed) < tail {
            spin_loop();
        }
        q.tail.curr.store(tail_plus_one, Ordering::Release);
        Ok(v)
    }

    fn try_pop(&self) -> Result<T, TryPopError> {
        let q = unsafe { &mut *self.queue.get() };
        loop {
            let tail = q.tail.curr.load(Ordering::Relaxed);
            let tail_plus_one = tail + 1;
            if tail_plus_one > q.head.load(Ordering::Acquire) {
                if q.ok.load(Ordering::Acquire) {
                    return Err(TryPopError::Empty);
                } else {
                    return Err(TryPopError::Disconnected);
                }
            } else if q
                .tail
                .next
                .compare_exchange_weak(tail, tail_plus_one, Ordering::Acquire, Ordering::Acquire)
                .is_ok()
            {
                let v = buf_read(&q.buf, tail);
                q.tail.curr.store(tail_plus_one, Ordering::Release);
                return Ok(v);
            }
        }
    }
}

impl<T, B: Buffer<T>> Drop for SPMCProducer<T, B> {
    fn drop(&mut self) {
        let q = unsafe { &mut *self.queue.get() };
        q.ok.store(false, Ordering::Release);
    }
}

impl<T, B: Buffer<T>> Drop for SPMCConsumer<T, B> {
    fn drop(&mut self) {
        let q = unsafe { &mut *self.queue.get() };
        q.ok.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
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

        let c2 = Arc::new(c);
        let mut consumers = Vec::new();
        for _ in 0..3 {
            let c = c2.clone();
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

        let (p, c) = spmc_queue(DynamicBuffer::new(32).unwrap());
        p.push(1).unwrap();
        std::mem::drop(c);
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
