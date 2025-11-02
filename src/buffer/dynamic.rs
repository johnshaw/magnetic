//! Buffer which is allocated at run time

use std::{cell::UnsafeCell, mem::MaybeUninit};

use super::Buffer;

/// Holds data allocated from the heap at run time
pub struct DynamicBuffer<T> {
    items: Box<[UnsafeCell<MaybeUninit<T>>]>,
}

impl<T> DynamicBuffer<T> {
    /// Create a new `DynamicBuffer` of the given size. This method will
    /// return an error if the requested number of bytes could not be
    /// allocated.
    pub fn new(size: usize) -> Result<Self, &'static str> {
        if size > 0 {
            let mut vec = Vec::with_capacity(size);
            unsafe { vec.set_len(size) };
            Ok(DynamicBuffer {
                items: vec.into_boxed_slice(),
            })
        } else {
            Err("Buffer size must be greater than 0")
        }
    }
}

unsafe impl<T: Send> Send for DynamicBuffer<T> {}
unsafe impl<T> Sync for DynamicBuffer<T> {}

impl<T> Buffer<T> for DynamicBuffer<T> {
    #[inline(always)]
    fn size(&self) -> usize {
        self.items.len()
    }

    #[inline(always)]
    fn at(&self, idx: usize) -> *const UnsafeCell<MaybeUninit<T>> {
        &self.items[idx % self.items.len()] as *const _
    }
}

/// Holds data allocated from the heap at run time. Similar to `DynamicBuffer`
/// except that the size must be a power of two. This will result in slightly
/// faster runtime performance due to the use of a mask instead of modulus
/// when computing buffer indexes.
pub struct DynamicBufferP2<T> {
    items: Box<[UnsafeCell<MaybeUninit<T>>]>,
}

impl<T> DynamicBufferP2<T> {
    /// Create a new `DynamicBufferP2` of the given size. This method will
    /// return an error if the requested number of bytes could not be
    /// allocated or size is not a power of two.
    pub fn new(size: usize) -> Result<Self, &'static str> {
        match size {
            0 => Err("Buffer size must be greater than 0"),
            _ if ((size - 1) & size) == 0 => Ok(DynamicBufferP2 {
                items: {
                    let mut vec = Vec::with_capacity(size);
                    unsafe { vec.set_len(size) };
                    vec.into_boxed_slice()
                },
            }),
            _ => Err("Buffer size must be a power of two"),
        }
    }
}

unsafe impl<T: Send> Send for DynamicBufferP2<T> {}
unsafe impl<T> Sync for DynamicBufferP2<T> {}

impl<T> Buffer<T> for DynamicBufferP2<T> {
    #[inline(always)]
    fn size(&self) -> usize {
        self.items.len()
    }

    #[inline(always)]
    fn at(&self, idx: usize) -> *const UnsafeCell<MaybeUninit<T>> {
        &self.items[idx & (self.items.len() - 1)] as *const _
    }
}

#[cfg(test)]
mod test {
    use super::super::Buffer;
    use super::*;

    #[test]
    fn bad_buf() {
        assert!(DynamicBuffer::<u8>::new(0).is_err());
        assert!(DynamicBufferP2::<u8>::new(0).is_err());
        assert!(DynamicBufferP2::<u8>::new(3).is_err());
    }

    #[test]
    fn p2_buf() {
        for i in 0..25 {
            let size = 1 << i;
            assert!(DynamicBufferP2::<u8>::new(size).is_ok());
        }
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
    fn dynamic_buf() {
        let mut buf = DynamicBuffer::<i32>::new(3).unwrap();
        assert_eq!(buf.size(), 3);
        test_at(&mut buf);
    }

    #[test]
    fn dynamic_buf_p2() {
        let mut buf = DynamicBufferP2::<i32>::new(4).unwrap();
        assert_eq!(buf.size(), 4);
        test_at(&mut buf);
    }
}
