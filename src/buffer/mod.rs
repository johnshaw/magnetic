//! Memory buffer API
//!
//! The `Buffer` trait defined here allows the memory buffer of the queue to
//! be defined independently from the queue implementation.

use std::{cell::UnsafeCell, mem::MaybeUninit};

pub mod array;
pub mod dynamic;

/// All buffers must implement this trait to be used with any of the queues.
pub trait Buffer<T>: Sync {
    /// Return the size of the buffer
    fn size(&self) -> usize;

    /// Return a pointer to data at the given index. It is expected that this
    /// function use modular arithmetic since `idx` may refer to a location
    /// beyond the end of the buffer.
    fn at(&self, idx: usize) -> *const UnsafeCell<MaybeUninit<T>>;
}

impl<T, B: Buffer<T>> Buffer<T> for &mut B {
    fn size(&self) -> usize {
        (**self).size()
    }

    fn at(&self, idx: usize) -> *const UnsafeCell<MaybeUninit<T>> {
        (**self).at(idx)
    }
}

impl<T, B: Buffer<T>> Buffer<T> for Box<B> {
    fn size(&self) -> usize {
        (**self).size()
    }

    fn at(&self, idx: usize) -> *const UnsafeCell<MaybeUninit<T>> {
        (**self).at(idx)
    }
}
