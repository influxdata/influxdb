//! Extensions traits for [`LinearBuffer`] to simplify common tasks.

use std::mem::MaybeUninit;

use crate::LinearBuffer;

/// Extension methods for [`LinearBuffer`] that are a safe combination of [`tail`](LinearBuffer::tail) and
/// [`bump`](LinearBuffer::bump).
pub trait LinearBufferExtend {
    /// Append data to buffer.
    ///
    /// # Panic
    /// There must be enough space left. In case of a panic, the buffer will be left untouched.
    ///
    /// # Example
    /// ```
    /// # use linear_buffer::{LinearBuffer, LinearBufferExtend};
    /// let mut buffer = LinearBuffer::new(6);
    ///
    /// buffer.append(b"foo");
    /// buffer.append(b"bar");
    ///
    /// assert_eq!(
    ///     buffer.slice_initialized_part(..).as_ref(),
    ///     b"foobar",
    /// );
    /// ```
    fn append(&mut self, data: &[u8]);

    /// Extend buffer with constant value.
    ///
    /// This can be used for example to zero-extend the buffer without allocating a temporary slice for
    /// [`append`](Self::append).
    ///
    /// # Panic
    /// There must be enough space left. In case of a panic, the buffer will be left untouched.
    ///
    /// # Example
    /// ```
    /// # use linear_buffer::{LinearBuffer, LinearBufferExtend};
    /// let mut buffer = LinearBuffer::new(6);
    ///
    /// buffer.fill(0, 2);
    /// buffer.fill(0xff, 4);
    ///
    /// assert_eq!(
    ///     buffer.slice_initialized_part(..).as_ref(),
    ///     [0, 0, 0xff, 0xff, 0xff, 0xff],
    /// );
    /// ```
    fn fill(&mut self, value: u8, n: usize);
}

impl LinearBufferExtend for LinearBuffer {
    fn append(&mut self, data: &[u8]) {
        let space_left = self.space_left();
        assert!(
            data.len() <= space_left,
            "want to append {} bytes but buffer only has {space_left} bytes left",
            data.len(),
        );

        let tail = self.tail();

        // SAFETY: we've just checked that there is enough space left
        let target = unsafe { tail.get_unchecked_mut(0..data.len()) };

        // there is no good stable way to write a slice, see:
        // https://github.com/rust-lang/rust/issues/79995
        // so we gonna hand-roll that

        // SAFETY: &[T] and &[MaybeUninit<T>] have the same layout
        let uninit_src: &[MaybeUninit<u8>] = unsafe { std::mem::transmute(data) };
        target.copy_from_slice(uninit_src);

        // SAFETY: we just wrote that data
        unsafe { self.bump(data.len()) };
    }

    fn fill(&mut self, value: u8, n: usize) {
        let space_left = self.space_left();
        assert!(
            n <= space_left,
            "want to fill {n} bytes but buffer only has {space_left} bytes left",
        );

        let tail = self.tail();

        // SAFETY: we've just checked that there is enough space left
        let target = unsafe { tail.get_unchecked_mut(0..n) };

        // filling `MaybeUninit` is currently not simple on stable, see
        // https://github.com/rust-lang/rust/issues/117428
        //
        // So we just hand-roll it. In contrast to the stdlib implementation though, we don't need to care about `Drop` because `u8` doesn't need it.
        for x in target.iter_mut() {
            x.write(value);
        }

        // SAFETY: we just wrote that data
        unsafe { self.bump(n) };
    }
}

#[cfg(test)]
mod test {
    use std::panic::AssertUnwindSafe;

    use super::*;

    #[test]
    fn append() {
        let mut buffer = LinearBuffer::new(5);
        buffer.append(b"foo");
        buffer.append(b"ba");
        assert_eq!(buffer.slice_initialized_part(..).as_ref(), b"fooba");
    }

    #[test]
    fn append_empty() {
        let mut buffer = LinearBuffer::new(3);
        fill_buffer_with_ff(&mut buffer);

        buffer.append(b"");

        // buffer init position didn't change
        assert_eq!(buffer.space_left(), 3);

        // our pre-initialized tail wasn't overridden
        unsafe {
            buffer.bump(3);
        }
        assert_eq!(
            buffer.slice_initialized_part(0..3).as_ref(),
            [0xff, 0xff, 0xff]
        );
    }

    #[test]
    fn panic_append_to_much() {
        let mut buffer = LinearBuffer::new(3);
        fill_buffer_with_ff(&mut buffer);

        buffer.append(b"ab");
        assert_eq!(buffer.slice_initialized_part(0..2).as_ref(), b"ab");

        let err = std::panic::catch_unwind(AssertUnwindSafe(|| {
            buffer.append(b"cd");
        }))
        .unwrap_err();
        assert_eq!(
            err.downcast_ref::<String>().unwrap(),
            "want to append 2 bytes but buffer only has 1 bytes left",
        );

        // buffer init position didn't change
        assert_eq!(buffer.space_left(), 1);

        // our pre-initialized tail wasn't overridden
        unsafe {
            buffer.bump(1);
        }
        assert_eq!(buffer.slice_initialized_part(0..3).as_ref(), b"ab\xff");
    }

    #[test]
    fn fill() {
        let mut buffer = LinearBuffer::new(5);
        buffer.fill(1, 3);
        buffer.fill(42, 2);
        assert_eq!(
            buffer.slice_initialized_part(..).as_ref(),
            [1, 1, 1, 42, 42]
        );
    }

    #[test]
    fn fill_n_0() {
        let mut buffer = LinearBuffer::new(3);
        fill_buffer_with_ff(&mut buffer);

        buffer.fill(1, 0);

        // buffer init position didn't change
        assert_eq!(buffer.space_left(), 3);

        // our pre-initialized tail wasn't overridden
        unsafe {
            buffer.bump(3);
        }
        assert_eq!(
            buffer.slice_initialized_part(0..3).as_ref(),
            [0xff, 0xff, 0xff]
        );
    }

    #[test]
    fn panic_fill_to_much() {
        let mut buffer = LinearBuffer::new(3);
        fill_buffer_with_ff(&mut buffer);

        buffer.fill(0, 2);
        assert_eq!(buffer.slice_initialized_part(0..2).as_ref(), [0, 0]);

        let err = std::panic::catch_unwind(AssertUnwindSafe(|| {
            buffer.fill(0, 2);
        }))
        .unwrap_err();
        assert_eq!(
            err.downcast_ref::<String>().unwrap(),
            "want to fill 2 bytes but buffer only has 1 bytes left",
        );

        // buffer init position didn't change
        assert_eq!(buffer.space_left(), 1);

        // our pre-initialized tail wasn't overridden
        unsafe {
            buffer.bump(1);
        }
        assert_eq!(buffer.slice_initialized_part(0..3).as_ref(), [0, 0, 0xff]);
    }

    #[test]
    fn test_fill_buffer_with_ff() {
        let mut buffer = LinearBuffer::new(3);
        fill_buffer_with_ff(&mut buffer);
        unsafe {
            buffer.bump(3);
        }
        assert_eq!(
            buffer.slice_initialized_part(0..3).as_ref(),
            [0xff, 0xff, 0xff]
        );
    }

    /// Fill buffer with pattern `0xff` without advancing the "initialized" position so we can check certain behavior.
    fn fill_buffer_with_ff(buffer: &mut LinearBuffer) {
        for x in buffer.tail().iter_mut() {
            x.write(0xff);
        }
    }
}
