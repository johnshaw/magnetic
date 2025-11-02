//! Multiple-producer multiple-consumer queue
//!
//! The MPMC queue allows for pushing from one thread and popping from another.
//! Both the producer and consumer ends of the queue may be accessed by
//! multiple threads. In other words, both `MPMCProducer` and `MPMCConsumer`
//! are `Send` and `Sync`.

use std::hint::spin_loop;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_utils::CachePadded;

use super::buffer::Buffer;
use super::{Consumer, PopError, Producer, PushError, TryPopError, TryPushError};
use crate::util::{buf_read, buf_write, AtomicPair};

struct MPMCQueue<T, B: Buffer<T>> {
    head: CachePadded<AtomicPair>,
    tail: CachePadded<AtomicPair>,
    buf: B,
    producers: AtomicUsize,
    consumers: AtomicUsize,
    _marker: PhantomData<T>,
}

/// Consumer end of the queue. Implements the trait `Consumer<T>`.
pub struct MPMCConsumer<T, B: Buffer<T>> {
    queue: Arc<MPMCQueue<T, B>>,
}

unsafe impl<T: Send, B: Buffer<T>> Sync for MPMCConsumer<T, B> {}

impl<T, B: Buffer<T>> Clone for MPMCConsumer<T, B> {
    fn clone(&self) -> Self {
        self.queue.consumers.fetch_add(1, Ordering::Relaxed);
        MPMCConsumer {
            queue: self.queue.clone(),
        }
    }
}

/// Producer end of the queue. Implements the trait `Producer<T>`.
pub struct MPMCProducer<T, B: Buffer<T>> {
    queue: Arc<MPMCQueue<T, B>>,
}

unsafe impl<T: Send, B: Buffer<T>> Sync for MPMCProducer<T, B> {}

impl<T, B: Buffer<T>> Clone for MPMCProducer<T, B> {
    fn clone(&self) -> Self {
        self.queue.producers.fetch_add(1, Ordering::Relaxed);
        MPMCProducer {
            queue: self.queue.clone(),
        }
    }
}

/// Creates a new MPMC queue
///
/// # Examples
///
/// ```
/// use magnetic::mpmc::mpmc_queue;
/// use magnetic::buffer::dynamic::DynamicBuffer;
/// use magnetic::{Producer, Consumer};
///
/// let (p, c) = mpmc_queue(DynamicBuffer::new(32).unwrap());
///
/// p.push(1).unwrap();
/// assert_eq!(c.pop(), Ok(1));
/// ```
pub fn mpmc_queue<T, B: Buffer<T>>(buf: B) -> (MPMCProducer<T, B>, MPMCConsumer<T, B>) {
    let queue = MPMCQueue {
        head: CachePadded::new(AtomicPair::default()),
        tail: CachePadded::new(AtomicPair::default()),
        buf,
        producers: AtomicUsize::new(1),
        consumers: AtomicUsize::new(1),
        _marker: PhantomData,
    };

    let queue = Arc::new(queue);

    (
        MPMCProducer {
            queue: queue.clone(),
        },
        MPMCConsumer { queue },
    )
}

impl<T, B: Buffer<T>> Drop for MPMCQueue<T, B> {
    fn drop(&mut self) {
        let head = self.head.curr.load(Ordering::Relaxed);
        let tail = self.tail.curr.load(Ordering::Relaxed);
        for pos in tail..head {
            unsafe { buf_read(&self.buf, pos) };
        }
    }
}

impl<T, B: Buffer<T>> Producer<T> for MPMCProducer<T, B> {
    fn is_closed(&self) -> bool {
        self.queue.consumers.load(Ordering::Relaxed) == 0
    }

    fn push(&self, value: T) -> Result<(), PushError<T>> {
        let q = &self.queue;
        let head = q.head.next.fetch_add(1, Ordering::Relaxed);

        loop {
            if q.tail.curr.load(Ordering::Acquire) + q.buf.size() > head {
                break;
            } else if q.consumers.load(Ordering::Relaxed) == 0 {
                return Err(PushError::Disconnected(value));
            }
            spin_loop();
        }

        unsafe { buf_write(&q.buf, head, value) };
        while q.head.curr.load(Ordering::Relaxed) < head {
            spin_loop();
        }
        q.head.curr.store(head + 1, Ordering::Release);
        Ok(())
    }

    fn try_push(&self, value: T) -> Result<(), TryPushError<T>> {
        let q = &self.queue;
        loop {
            let head = q.head.curr.load(Ordering::Relaxed);
            let head_plus_one = head + 1;

            if q.tail.curr.load(Ordering::Acquire) + q.buf.size() <= head {
                // buffer is full, check whether it's closed.
                // relaxed is fine since Consumer.drop does an acquire/release on .tail
                return if q.consumers.load(Ordering::Relaxed) == 0 {
                    Err(TryPushError::Disconnected(value))
                } else {
                    Err(TryPushError::Full(value))
                };
            } else if q
                .head
                .next
                .compare_exchange_weak(head, head_plus_one, Ordering::Acquire, Ordering::Acquire)
                .is_ok()
            {
                unsafe { buf_write(&q.buf, head, value) };
                q.head.curr.store(head_plus_one, Ordering::Release);
                return Ok(());
            }
        }
    }
}

impl<T, B: Buffer<T>> Consumer<T> for MPMCConsumer<T, B> {
    fn is_closed(&self) -> bool {
        self.queue.producers.load(Ordering::Relaxed) == 0
    }

    fn pop(&self) -> Result<T, PopError> {
        let q = &self.queue;
        let tail = q.tail.next.fetch_add(1, Ordering::Relaxed);
        let tail_plus_one = tail + 1;

        loop {
            if q.head.curr.load(Ordering::Acquire) >= tail_plus_one {
                break;
            } else if q.producers.load(Ordering::Relaxed) == 0 {
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

            if q.head.curr.load(Ordering::Acquire) < tail_plus_one {
                // buffer is empty, check whether it's closed.
                // relaxed is fine since Producer.drop does an acquire/release on .head
                return if q.producers.load(Ordering::Relaxed) == 0 {
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

impl<T, B: Buffer<T>> Drop for MPMCProducer<T, B> {
    fn drop(&mut self) {
        self.queue.producers.fetch_sub(1, Ordering::Relaxed);
        // Acquire/Release .head to ensure other threads see new .closed
        self.queue.head.curr.fetch_add(0, Ordering::AcqRel);
    }
}

impl<T, B: Buffer<T>> Drop for MPMCConsumer<T, B> {
    fn drop(&mut self) {
        self.queue.consumers.fetch_sub(1, Ordering::Relaxed);
        // Acquire/Release .tail to ensure other threads see new .closed
        self.queue.tail.curr.fetch_add(0, Ordering::AcqRel);
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
        let (p, c) = mpmc_queue(DynamicBuffer::new(2).unwrap());

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
        let (p, c) = mpmc_queue(DynamicBuffer::new(3).unwrap());

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

        let (p, c) = mpmc_queue(DynamicBuffer::new(32).unwrap());

        let count = 10_000_000;

        let mut producers = Vec::new();
        for _ in 0..2 {
            let p = p.clone();
            producers.push(spawn(move || {
                for i in 0..count {
                    p.push(i).unwrap();
                }
            }));
        }

        let mut consumers = Vec::new();
        for _ in 0..2 {
            let c = c.clone();
            consumers.push(spawn(move || {
                (0..count).fold(0u64, |a, _| a + c.pop().unwrap())
            }));
        }

        for t in producers.into_iter() {
            t.join().unwrap();
        }

        let sum: u64 = consumers.into_iter().map(|t| t.join().unwrap()).sum();
        assert_eq!(sum, (count - 1) * ((count - 1) + 1));
    }

    #[test]
    fn disconnect() {
        let (p, c) = mpmc_queue(DynamicBuffer::new(32).unwrap());
        p.push(1).unwrap();
        p.push(2).unwrap();
        std::mem::drop(p);
        assert_eq!(c.pop(), Ok(1));
        assert_eq!(c.pop(), Ok(2));
        assert_eq!(c.pop(), Err(PopError::Disconnected));
        assert_eq!(c.try_pop(), Err(TryPopError::Disconnected));

        let (p, c) = mpmc_queue(DynamicBuffer::new(2).unwrap());
        p.push(1).unwrap();
        std::mem::drop(c);
        assert!(p.is_closed());
        p.push(1).unwrap();
        assert_eq!(p.push(2), Err(PushError::Disconnected(2)));
        assert_eq!(p.try_push(2), Err(TryPushError::Disconnected(2)));

        let (p, c) = mpmc_queue(DynamicBuffer::new(1).unwrap());
        p.push(1).unwrap();
        assert_eq!(p.try_push(2), Err(TryPushError::Full(2)));
        std::mem::drop(c);
        assert_eq!(p.push(2), Err(PushError::Disconnected(2)));
        assert_eq!(p.try_push(2), Err(TryPushError::Disconnected(2)));
    }
}
