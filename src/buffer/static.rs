//! Buffer backed by an array

use std::mem::MaybeUninit;

use super::Buffer;

/// Holds data locally in an array (no heap allocation)
pub struct StaticBuffer<T, const CAP: usize>([MaybeUninit<T>; CAP]);

impl<T, const CAP: usize> StaticBuffer<T, CAP> {
    /// Create a new `StaticBuffer`. This method will return an error if the capacity is not valid.
    pub fn new() -> Result<Self, &'static str> {
        if CAP > 0 {
            // Safety: An array of MaybeUninit is "initialized" to uninit values
            Ok(StaticBuffer(unsafe { MaybeUninit::uninit().assume_init() }))
        } else {
            Err("Buffer size must be greater than 0")
        }
    }
}

unsafe impl<T: Send, const CAP: usize> Send for StaticBuffer<T, CAP> {}

impl<T, const CAP: usize> Buffer<T> for StaticBuffer<T, CAP> {
    #[inline(always)]
    fn size(&self) -> usize {
        CAP
    }

    #[inline(always)]
    fn at(&self, idx: usize) -> *const T {
        self.0[idx % CAP].as_ptr()
    }

    fn at_mut(&mut self, idx: usize) -> *mut T {
        self.0[idx % CAP].as_mut_ptr()
    }
}

#[cfg(test)]
mod test {
    use super::super::Buffer;
    use super::*;

    #[test]
    fn bad_buf() {
        assert!(StaticBuffer::<u8, 0>::new().is_err());
    }

    fn test_at<T, B: Buffer<T>>(buf: &mut B) {
        let size = buf.size();
        for i in 0..size {
            assert_eq!(buf.at(i), buf.at(i));
            assert_eq!(buf.at_mut(i), buf.at_mut(i));
            assert_eq!(buf.at(i), buf.at(size + i));
            assert_eq!(buf.at_mut(i), buf.at_mut(size + i));
            assert!(buf.at(i) != buf.at(i + 1));
            assert!(buf.at_mut(i) != buf.at_mut(i + 1));
        }
    }

    #[test]
    fn static_buf() {
        let mut buf = StaticBuffer::<i32, 3>::new().unwrap();
        assert_eq!(buf.size(), 3);
        test_at(&mut buf);
    }
}
