//! Multiple-producer single-consumer queue
//!
//! The MPSC queue allows for pushing from one thread and popping from another.
//! The producer end of the queue may be accessed by multiple threads while
//! the consumer end may only be accessed by a single thread. In other words,
//! the `MPSCProducer` is `Send` and `Sync` while the `MPSCConsumer` is `Send`
//! and `!Sync`.

use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::{Consumer, Producer, PushError, TryPushError, PopError, TryPopError};
use super::buffer::Buffer;
use util::{pause, buf_read, buf_write};

//#[repr(C)]
struct MPSCQueue<T, B: Buffer<T>> {
    head: AtomicUsize,
    next_head: AtomicUsize,
    _pad1: [u8; 48],
    tail: AtomicUsize,
    _pad2: [u8; 56],
    buf: B,
    ok: AtomicBool,
    _marker: PhantomData<T>
}

unsafe impl<T, B: Buffer<T>> Sync for MPSCQueue<T, B> {}

/// Consumer end of the queue. Implements the trait `Consumer<T>`.
pub struct MPSCConsumer<T, B: Buffer<T>> {
    queue: Arc<UnsafeCell<MPSCQueue<T, B>>>,
}

unsafe impl<T: Send, B: Buffer<T>> Send for MPSCConsumer<T, B> {}

/// Producer end of the queue. Implements the trait `Producer<T>`.
pub struct MPSCProducer<T, B: Buffer<T>> {
    queue: Arc<UnsafeCell<MPSCQueue<T, B>>>,
}

unsafe impl<T: Send, B: Buffer<T>> Send for MPSCProducer<T, B> {}
unsafe impl<T: Send, B: Buffer<T>> Sync for MPSCProducer<T, B> {}

/// Creates a new MPSC queue
///
/// # Examples
///
/// ```
/// use magnetic::mpsc::mpsc_queue;
/// use magnetic::buffer::dynamic::DynamicBuffer;
/// use magnetic::{Producer, Consumer};
///
/// let (p, c) = mpsc_queue(DynamicBuffer::new(32).unwrap());
///
/// p.push(1).unwrap();
/// assert_eq!(c.pop(), Ok(1));
/// ```
pub fn mpsc_queue<T, B: Buffer<T>>(buf: B)
        -> (MPSCProducer<T, B>, MPSCConsumer<T, B>) {
    let queue = MPSCQueue {
        head: AtomicUsize::new(0),
        next_head: AtomicUsize::new(0),
        _pad1: [0; 48],
        tail: AtomicUsize::new(0),
        _pad2: [0; 56],
        buf: buf,
        ok: AtomicBool::new(true),
        _marker: PhantomData
    };

    let queue = Arc::new(UnsafeCell::new(queue));

    (
        MPSCProducer { queue: queue.clone() },
        MPSCConsumer { queue: queue },
    )
}

impl<T, B: Buffer<T>> Drop for MPSCQueue<T, B> {
    fn drop(&mut self) {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        for pos in tail..head {
            buf_read(&self.buf, pos);
        }
    }
}

impl<T, B: Buffer<T>> Producer<T> for MPSCProducer<T, B> {
    fn push(&self, value: T) -> Result<(), PushError<T>> {
        let q = unsafe { &mut *self.queue.get() };

        let head = q.next_head.fetch_add(1, Ordering::Relaxed);
        while q.tail.load(Ordering::Acquire) + q.buf.size() <= head {
            if !q.ok.load(Ordering::Relaxed) {
                return Err(PushError::Disconnected(value));
            }
            pause();
        }

        buf_write(&mut q.buf, head, value);

        while q.head.load(Ordering::Relaxed) < head { pause(); }
        q.head.store(head + 1, Ordering::Release);
        Ok(())
    }

    fn try_push(&self, value: T) -> Result<(), TryPushError<T>> {
        let q = unsafe { &mut *self.queue.get() };

        let head = q.head.load(Ordering::Relaxed);

        if q.tail.load(Ordering::Relaxed) + q.buf.size() <= head {
            if q.ok.load(Ordering::Relaxed) {
                return Err(TryPushError::Full(value))
            } else {
                return Err(TryPushError::Disconnected(value));
            }
        } else {
            let next = head + 1;
            if q.next_head.compare_and_swap(head, next, Ordering::Acquire) == head {
                buf_write(&mut q.buf, head, value);
                q.head.store(next, Ordering::Release);
                Ok(())
            } else {
                if q.ok.load(Ordering::Relaxed) {
                    return Err(TryPushError::Full(value))
                } else {
                    return Err(TryPushError::Disconnected(value));
                }
            }
        }
    }
}

impl<T, B: Buffer<T>> Consumer<T> for MPSCConsumer<T, B> {
    fn pop(&self) -> Result<T, PopError> {
        let q = unsafe { &mut *self.queue.get() };

        let tail = q.tail.load(Ordering::Relaxed);
        let tail_plus_one = tail + 1;
        while tail_plus_one > q.head.load(Ordering::Acquire) {
            if !q.ok.load(Ordering::Relaxed) {
                return Err(PopError::Disconnected);
            }
            pause();
        }

        let v = buf_read(&q.buf, tail);

        q.tail.store(tail_plus_one, Ordering::Release);
        Ok(v)
    }

    fn try_pop(&self) -> Result<T, TryPopError> {
        let q = unsafe { &mut *self.queue.get() };
        let tail = q.tail.load(Ordering::Relaxed);
        let tail_plus_one = tail + 1;

        if tail_plus_one > q.head.load(Ordering::Acquire) {
            if q.ok.load(Ordering::Relaxed) {
                Err(TryPopError::Empty)
            } else {
                Err(TryPopError::Disconnected)
            }
        } else {
            let v = buf_read(&q.buf, tail);
            q.tail.store(tail_plus_one, Ordering::Release);
            Ok(v)
        }
    }
}

impl<T, B: Buffer<T>> Drop for MPSCProducer<T, B> {
    fn drop(&mut self) {
        let q = unsafe { &mut *self.queue.get() };
        q.ok.store(false, Ordering::Relaxed);
    }
}

impl<T, B: Buffer<T>> Drop for MPSCConsumer<T, B> {
    fn drop(&mut self) {
        let q = unsafe { &mut *self.queue.get() };
        q.ok.store(false, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::thread::spawn;

    use super::*;
    use super::super::{Consumer, Producer, TryPushError, TryPopError};
    use super::super::buffer::dynamic::DynamicBuffer;

    #[test]
    fn one_thread() {
        let (p, c) = mpsc_queue(DynamicBuffer::new(2).unwrap());

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
        let (p, c) = mpsc_queue(DynamicBuffer::new(3).unwrap());

        let p = spawn(move || {
            p.push(vec![1; 5]).unwrap();
            p.push(vec![2; 7]).unwrap();
            p.push(vec![3; 3]).unwrap();
            p
        }).join().unwrap();

        let c = spawn(move || {
            assert_eq!(c.pop(), Ok(vec![1; 5]));
            assert_eq!(c.pop(), Ok(vec![2; 7]));
            assert_eq!(c.pop(), Ok(vec![3; 3]));
            assert_eq!(c.try_pop(), Err(TryPopError::Empty));
            c
        }).join().unwrap();

        drop(p);

        assert_eq!(c.try_pop(), Err(TryPopError::Disconnected));
    }

    #[test]
    fn four_thread_par() {
        let (p, c) = mpsc_queue(DynamicBuffer::new(32).unwrap());

        let count = 10_000_000u64;

        let p2 = Arc::new(p);

        let mut producers = Vec::new();
        for _ in 0..3 {
            let p = p2.clone();
            producers.push(spawn(move || {
                for i in 0..count {
                    p.push(i).unwrap();
                }
            }));
        }

        let total = count * producers.len() as u64;
        let t2 = spawn(move || {
            (0..total).fold(0u64, |a, _| a + c.pop().unwrap())
        });

        for t in producers.into_iter() {
            t.join().unwrap();
        }

        let sum = t2.join().unwrap();
        assert_eq!(sum, (count-1) * ((count-1) + 1) * 3 / 2);
    }
}

#[cfg(all(feature = "unstable", test))]
mod bench {
    use std::thread::spawn;
    use std::sync::mpsc::channel;

    use test::Bencher;

    use super::*;
    use super::super::{Consumer, Producer};
    use super::super::buffer::dynamic::DynamicBuffer;

    #[bench]
    fn ping_pong(b: &mut Bencher) {
        let (p1, c1) = mpsc_queue(DynamicBuffer::new(32).unwrap());
        let (p2, c2) = mpsc_queue(DynamicBuffer::new(32).unwrap());

        let pong = spawn(move || {
            loop {
                let n = c1.pop().unwrap();
                p2.push(n).unwrap();
                if n == 0 {
                    break
                }
            }
            (c1, p2)
        });

        b.iter(|| {
            p1.push(1234).unwrap();
            c2.pop().unwrap();
        });

        p1.push(0).unwrap();
        c2.pop().unwrap();
        pong.join().unwrap();
    }

    #[bench]
    fn ping_pong_try(b: &mut Bencher) {
        let (p1, c1) = mpsc_queue(DynamicBuffer::new(32).unwrap());
        let (p2, c2) = mpsc_queue(DynamicBuffer::new(32).unwrap());

        let pong = spawn(move || {
            loop {
                match c1.try_pop() {
                    Ok(n) => {
                        while let Err(_) = p2.try_push(n) {}
                        if n == 0 {
                            break
                        }
                    },
                    Err(_) => {}
                }
            }
            (c1, p2)
        });

        b.iter(|| {
            while let Err(_) = p1.try_push(1234) {};
            while let Err(_) = c2.try_pop() {};
        });

        p1.push(0).unwrap();
        c2.pop().unwrap();
        pong.join().unwrap();
    }

    #[bench]
    fn ping_pong_std(b: &mut Bencher) {
        let (p1, c1) = channel();
        let (p2, c2) = channel();

        let pong = spawn(move || {
            loop {
                let n = c1.recv().unwrap();
                p2.send(n).unwrap();
                if n == 0 {
                    break
                }
            }
        });

        b.iter(|| {
            p1.send(1234).unwrap();
            c2.recv().unwrap();
        });

        p1.send(0).unwrap();
        c2.recv().unwrap();
        pong.join().unwrap();
    }
}
