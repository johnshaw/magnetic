//! Buffer backed by an array

use std::{cell::UnsafeCell, mem::MaybeUninit};

use super::Buffer;

/// Holds data locally in an array (no heap allocation)
pub struct ArrayBuffer<T, const CAP: usize>([UnsafeCell<MaybeUninit<T>>; CAP]);

impl<T, const CAP: usize> ArrayBuffer<T, CAP> {
    /// Create a new `ArrayBuffer`. This method will return an error if the capacity is not valid.
    pub fn new() -> Result<Self, &'static str> {
        if CAP > 0 {
            // Safety: An array of MaybeUninit is "initialized" to uninit values
            Ok(ArrayBuffer(unsafe { MaybeUninit::uninit().assume_init() }))
        } else {
            Err("Buffer size must be greater than 0")
        }
    }
}

unsafe impl<T: Send, const CAP: usize> Send for ArrayBuffer<T, CAP> {}
unsafe impl<T, const CAP: usize> Sync for ArrayBuffer<T, CAP> {}

impl<T, const CAP: usize> Buffer<T> for ArrayBuffer<T, CAP> {
    #[inline(always)]
    fn size(&self) -> usize {
        CAP
    }

    #[inline(always)]
    fn at(&self, idx: usize) -> *const UnsafeCell<MaybeUninit<T>> {
        &self.0[idx % CAP] as *const _
    }
}

#[cfg(test)]
mod test {
    use super::super::Buffer;
    use super::*;

    #[test]
    fn bad_buf() {
        assert!(ArrayBuffer::<u8, 0>::new().is_err());
    }

    fn test_at<T, B: Buffer<T>>(buf: &mut B) {
        let size = buf.size();
        for i in 0..size {
            assert_eq!(buf.at(i), buf.at(i));
            assert_eq!(buf.at(i), buf.at(size + i));
            assert!(buf.at(i) != buf.at(i + 1));
        }
    }

    #[test]
    fn array_buf() {
        let mut buf = ArrayBuffer::<i32, 3>::new().unwrap();
        assert_eq!(buf.size(), 3);
        test_at(&mut buf);
    }
}
