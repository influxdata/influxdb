//! Implementation of the buffer construct itself.
use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    num::NonZeroUsize,
    ops::{Bound, Deref, Range, RangeBounds},
    sync::Arc,
};

use crate::allocation::Allocation;

/// Fixed-size buffer that supports [append] and
/// [reading initialized parts](Self::slice_initialized_part) at the same time.
///
/// # Use Case
/// This construct allows you to [append] data to a buffer but at the same time hand out slices to the
/// already-initialized part of it. This is normally not possible with Rust's borrowing rules. An example is when you
/// receive data from a network and want to cache data in-memory (like an entire file), but also want to run write
/// operations for the already-received data to disk (e.g. for caching).
///
/// Furthermore, the buffer can be [initialized with a desired alignment](Self::with_alignment).
///
/// Neither of this is possible with purely safe standard library tooling nor with the famous [`bytes`] crate.
///
/// # Implementation
/// The data layout looks like this:
///
/// ```text
/// |<-----------------total_size------------------------------>|
/// |                                                           |
/// [============== allocation =================================]
/// [✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓????????????????????????????????????????]
/// |                   |                                       |
/// |<---initialized--->|                                       |
///                     |<---unitialized / space left / tail--->|
///                     ^
///                     |
///            first_uninit_element
/// ```
///
/// The _allocation_ is held as a [`MaybeUninit`] slice to avoid zeroing the buffer just to overwrite the data shortly
/// after. The _allocation_ NEVER moves and is only dropped when the [`LinearBuffer`] and all [`Slice`]s are dropped.
///
/// The user can [get slices of the _initialized_ part](Self::slice_initialized_part). At the same time there exists
/// only at max one [`LinearBuffer`] which acts as a mutable reference to the uninitialized part:
///
/// ```text
/// |<----------------LinearBuffer----------------------------->|
/// |                                                           |
/// |                                                           |
/// V                                                           V
/// [============== allocation =================================]
/// [✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓??????????????????????????????????]
///  ^ ^           ^   ^
///  | |           |   |
///  | |           |   |
///  | |<---Slice 1--->|
///  |             |
///  |             |
///  |<--Slice 2-->|
/// ```
///
///
/// [append]: crate::LinearBufferExtend::append
/// [`bytes`]: https://crates.io/crates/bytes
#[derive(Debug)]
pub struct LinearBuffer {
    data: SharedAllocation,
    first_uninit_element: usize,
}

impl LinearBuffer {
    /// Allocate new buffer of given size in bytes.
    ///
    /// # Panic
    /// If we cannot allocate the buffer, we panic.
    pub fn new(len: usize) -> Self {
        Self::with_alignment(len, NonZeroUsize::MIN)
    }

    /// Allocate new buffer of given size in bytes and alignment.
    ///
    /// # Panic
    /// If we cannot allocate the buffer, we panic.
    ///
    /// Alignment must be a power of 2.
    ///
    /// # Alignment Rust Type
    /// Once <https://github.com/rust-lang/rust/issues/102070> is closed and we have a proper stable `Alignment` type,
    /// we should use that. For now we only enforce "not zero" on the type level and the rest during runtime.
    #[expect(clippy::arc_with_non_send_sync)]
    pub fn with_alignment(len: usize, alignment: NonZeroUsize) -> Self {
        Self {
            data: SharedAllocation(Arc::new(UnsafeCell::new(Allocation::new(len, alignment)))),
            first_uninit_element: 0,
        }
    }

    /// Size of the entire buffer, including the initialized part and the uninitialized part.
    ///
    /// Also see [`space_left`](Self::space_left) and [`initialized_bytes`](Self::initialized_bytes).
    pub fn total_size(&self) -> usize {
        self.data.total_size()
    }

    /// How much space is left.
    ///
    /// This is identical to the length of the [`tail`](Self::tail), but does not require a mutable reference to obtain.
    pub fn space_left(&self) -> usize {
        self.total_size() - self.first_uninit_element
    }

    /// Number of initialized bytes.
    pub fn initialized_bytes(&self) -> usize {
        self.first_uninit_element
    }

    /// Checks if this is the only slice pointing to the underlying allocation.
    ///
    /// This requires a mutable reference so because only mutable references are unique.
    pub fn is_unique(&mut self) -> bool {
        self.data.is_unique()
    }

    /// The uninitialized part of the buffer.
    ///
    /// This can be used as a target for I/O operations. After writing data in, call [`bump`](Self::bump) to specify
    /// the amount of data written to the START of the tail.
    ///
    /// If you want to append data from an existing slice or a constant value, it is easier to use
    /// [`LinearBufferExtend`]. However, using this low-level interface might work better if you have I/O operations
    /// that can read into a pre-allocated buffer.
    ///
    /// # Example
    /// ```
    /// # use linear_buffer::LinearBuffer;
    /// let mut buffer = LinearBuffer::new(3);
    ///
    /// let tail = buffer.tail();
    /// tail[0].write(b'f');
    /// tail[1].write(b'o');
    /// tail[2].write(b'o');
    ///
    /// unsafe { buffer.bump(3) };
    ///
    /// assert_eq!(
    ///     buffer.slice_initialized_part(0..3).as_ref(),
    ///     b"foo",
    /// );
    /// ```
    ///
    ///
    /// [`LinearBufferExtend`]: crate::LinearBufferExtend
    pub fn tail(&mut self) -> &mut [MaybeUninit<u8>] {
        let data_ptr = self.data.0.get();

        // SAFETY: there can only be one caller that accesses the tail due to Rust's borrowing rules
        let partially_initialized_buffer = unsafe { &mut *data_ptr };

        // SAFETY: first_uninit_element is always in bounds because we reject "overshooting" in `bump`
        unsafe { partially_initialized_buffer.get_unchecked_mut(self.first_uninit_element..) }
    }

    /// Bump initialized part of the buffer by given amount of bytes (= delta).
    ///
    /// # Panic
    /// There must be enough space left in buffer.
    ///
    /// # Safety
    /// The caller must ensure that they initialized the respective portion of the buffer using [`tail`](Self::tail).
    pub unsafe fn bump(&mut self, initialized: usize) {
        let space_left = self.space_left();
        assert!(
            initialized <= space_left,
            "buffer only has {space_left} bytes left but initialized part should be bumped by {initialized} bytes",
        );

        self.first_uninit_element += initialized;
    }

    /// Get a slice of the initialized portion of the buffer.
    ///
    /// You may hold multiple overlapping slices to the same initialized memory.
    ///
    /// # Panic
    /// The range must be well-formed and within the range of the initialized part.
    #[track_caller]
    pub fn slice_initialized_part(&self, range: impl RangeBounds<usize>) -> Slice {
        let len = self.total_size();

        let begin = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n.checked_add(1).expect("out of range"),
            Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            Bound::Included(&n) => n.checked_add(1).expect("out of range"),
            Bound::Excluded(&n) => n,
            Bound::Unbounded => len,
        };

        assert!(
            begin <= end,
            "range start must not be greater than end: {begin} <= {end}",
        );
        assert!(
            end <= self.first_uninit_element,
            "range end out of bounds: {end} <= {}",
            self.first_uninit_element,
        );

        Slice {
            data: self.data.clone(),
            range: begin..end,
        }
    }
}

/// Wrapper around the half-initialized buffer.
#[derive(Debug, Clone)]
struct SharedAllocation(Arc<UnsafeCell<Allocation>>);

// SAFETY: We manually make sure that:
//         - the inner allocation never changes
//         - there is only at max one mutable reference to the tail part of the buffer
//         - there is NO mutable reference to the initialized part of the buffer
//         - slices (i.e. non-mut references) only exist to the initialized part of the buffer
unsafe impl Send for SharedAllocation {}
unsafe impl Sync for SharedAllocation {}

impl SharedAllocation {
    /// Size of the entire buffer, including the initialized part and the uninitialized part.
    fn total_size(&self) -> usize {
        let data_ptr = self.0.get();

        // SAFETY: we NEVER change the underlying allocation
        let allocation = unsafe { &*data_ptr };

        allocation.len()
    }

    /// Checks if this is the only slice pointing to the underlying allocation.
    ///
    /// This requires a mutable reference so because only mutable references are unique.
    fn is_unique(&mut self) -> bool {
        // do NOT use `Arc::strong_count` here because it is racy/relaxed, see https://github.com/rust-lang/rust/issues/117485
        Arc::get_mut(&mut self.0).is_some()
    }
}

/// A slice of initialized data from a [`LinearBuffer`].
#[derive(Clone)]
pub struct Slice {
    data: SharedAllocation,
    range: Range<usize>,
}

impl Slice {
    /// Size of the underlying allocation in bytes.
    pub fn allocation_size(&self) -> usize {
        self.data.total_size()
    }

    /// Checks if this is the only slice pointing to the underlying allocation.
    ///
    /// This requires a mutable reference so because only mutable references are unique.
    pub fn is_unique(&mut self) -> bool {
        self.data.is_unique()
    }
}

impl std::fmt::Debug for Slice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}

impl Deref for Slice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        let data_ptr = self.data.0.get();

        // SAFETY: the actual allocation is never changed
        let partially_initialized_buffer = unsafe { &*data_ptr };

        // SAFETY: we've check the bounds in LinearBuffer::slice_initialized_part
        let init_part = unsafe { partially_initialized_buffer.get_unchecked(self.range.clone()) };

        // SAFETY: this is only the initialized part
        unsafe {
            // Slice methods of "assume init" aren't stable yet, see
            // https://github.com/rust-lang/rust/issues/63569
            //
            // So we just use the code from the stdlib
            &*(init_part as *const [MaybeUninit<u8>] as *const [u8])
        }
    }
}

impl AsRef<[u8]> for Slice {
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}

#[cfg(test)]
mod test {
    use crate::LinearBufferExtend;

    use super::*;

    #[test]
    #[should_panic(
        expected = "buffer only has 2 bytes left but initialized part should be bumped by 3 bytes"
    )]
    fn panic_bump_too_much() {
        let mut buffer = LinearBuffer::new(3);

        buffer.tail()[0].write(1);
        unsafe { buffer.bump(1) };

        buffer.tail()[0].write(1);
        buffer.tail()[1].write(1);
        unsafe { buffer.bump(3) };
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn panic_slice_begin_usize_out_of_range() {
        let mut buffer = LinearBuffer::new(3);
        buffer.append(b"foo");

        buffer.slice_initialized_part((Bound::Excluded(usize::MAX), Bound::Unbounded));
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn panic_slice_end_usize_out_of_range() {
        let mut buffer = LinearBuffer::new(3);
        buffer.append(b"foo");

        buffer.slice_initialized_part(..=usize::MAX);
    }

    #[test]
    #[should_panic(expected = "range start must not be greater than end: 2 <= 1")]
    #[expect(clippy::reversed_empty_ranges)]
    fn panic_slice_begin_past_end() {
        let mut buffer = LinearBuffer::new(10);
        buffer.append(b"foo");

        buffer.slice_initialized_part(2..1);
    }

    #[test]
    #[should_panic(expected = "range end out of bounds: 4 <= 3")]
    fn panic_slice_end_past_init_part() {
        let mut buffer = LinearBuffer::new(10);
        buffer.append(b"foo");

        buffer.slice_initialized_part(..4);
    }

    #[test]
    fn empty_slice() {
        let mut buffer = LinearBuffer::new(3);
        buffer.append(b"foo");

        let bytes = buffer.slice_initialized_part(0..0);
        assert_eq!(bytes.as_ref(), b"");
    }

    #[test]
    fn slices_are_zero_copy() {
        let mut buffer = LinearBuffer::new(10);
        buffer.append(b"foo");

        let bytes_1 = buffer.slice_initialized_part(..3);
        let ptr_1 = bytes_1.as_ptr().expose_provenance();
        assert_eq!(bytes_1.as_ref(), b"foo".as_slice());

        buffer.append(b"bar");

        let bytes_2 = buffer.slice_initialized_part(..6);
        let ptr_2 = bytes_1.as_ptr().expose_provenance();
        assert_eq!(bytes_2.as_ref(), b"foobar".as_slice());
        assert_eq!(ptr_1, ptr_2);

        buffer.append(b"xxxx");

        let data = buffer.slice_initialized_part(..);
        let data_ptr = data.as_ptr().expose_provenance();
        assert_eq!(data_ptr, ptr_1);
        assert_eq!(data.as_ref(), b"foobarxxxx".as_slice());
    }

    #[test]
    fn can_read_slice_after_buffer_drop() {
        let mut buffer = LinearBuffer::new(3);
        buffer.append(b"foo");

        let bytes = buffer.slice_initialized_part(..);
        drop(buffer);

        assert_eq!(bytes.as_ref(), b"foo".as_slice());
    }

    #[test]
    fn slice_clone() {
        let mut buffer = LinearBuffer::new(3);
        buffer.append(b"foo");

        let mut bytes = buffer.slice_initialized_part(..);
        assert_eq!(bytes.as_ref(), b"foo".as_slice());
        assert!(!bytes.is_unique());

        drop(buffer);
        assert!(bytes.is_unique());

        let mut bytes2 = bytes.clone();
        assert_eq!(bytes2.as_ref(), b"foo".as_slice());
        assert!(!bytes.is_unique());
        assert!(!bytes2.is_unique());
        assert_eq!(
            bytes.as_ptr().expose_provenance(),
            bytes2.as_ptr().expose_provenance(),
            "slice cloning MUST NOT clone the actual data",
        );

        drop(bytes);
        assert!(bytes2.is_unique());
        assert_eq!(bytes2.as_ref(), b"foo".as_slice());
    }

    #[test]
    fn slice_debug() {
        let mut buffer = LinearBuffer::new(3);
        buffer.append(b"foo");

        let slice = buffer.slice_initialized_part(..);
        assert_eq!(format!("{slice:?}"), "[102, 111, 111]");
        assert_eq!(format!("{slice:x?}"), "[66, 6f, 6f]");
    }

    #[test]
    fn empty_buffer() {
        let buffer = LinearBuffer::new(0);
        let slice = buffer.slice_initialized_part(..);
        assert_eq!(slice.as_ref(), b"");
    }

    #[test]
    #[should_panic(expected = "size fits `isize`")]
    fn new_panics_larger_than_isize() {
        LinearBuffer::new(usize::MAX);
    }

    #[test]
    #[should_panic(expected = "cannot allocate 9223372036854775807 bytes with alignment 1")]
    #[cfg(not(miri))] // MIRI cannot handle this
    fn new_panics_out_of_memory() {
        LinearBuffer::new(isize::MAX as usize);
    }

    #[test]
    #[should_panic(expected = "valid alignment")]
    fn new_panics_if_alignment_is_not_power_of_two() {
        LinearBuffer::with_alignment(1, NonZeroUsize::new(3).unwrap());
    }

    #[test]
    fn alignment() {
        for size in [0, 13] {
            for shift in 0..13 {
                let alignment = NonZeroUsize::new(1 << shift).unwrap();
                println!("size={size} alignment={alignment}");

                let mut buffer = LinearBuffer::with_alignment(size, alignment);
                assert_eq!(buffer.total_size(), size);

                let slice = buffer.slice_initialized_part(0..0);
                assert_eq!(slice.as_ptr().align_offset(alignment.get()), 0);

                buffer.fill(0, size);
                let slice = buffer.slice_initialized_part(..size);
                assert_eq!(slice.as_ptr().align_offset(alignment.get()), 0);
            }
        }
    }

    #[test]
    fn is_unique() {
        let mut buffer = LinearBuffer::new(3);
        assert!(buffer.is_unique());

        let mut slice_1 = buffer.slice_initialized_part(..0);
        assert!(!buffer.is_unique());
        assert!(!slice_1.is_unique());

        let mut slice_2 = buffer.slice_initialized_part(..0);
        assert!(!buffer.is_unique());
        assert!(!slice_1.is_unique());
        assert!(!slice_2.is_unique());

        drop(slice_1);
        assert!(!buffer.is_unique());
        assert!(!slice_2.is_unique());

        drop(buffer);
        assert!(slice_2.is_unique());
    }

    #[test]
    #[ignore = "this is unsound, it just demonstrates that MIRI will find out about it"]
    fn miri_finds_it() {
        let mut buffer = LinearBuffer::new(3);

        buffer.tail()[0].write(1);

        // we lie about the amount of data written
        unsafe { buffer.bump(3) };

        let bytes = buffer.slice_initialized_part(..);
        assert_ne!(bytes.as_ref(), b"xxx".as_slice());
    }

    const fn assert_send<T: Send>() {}
    const fn assert_sync<T: Sync>() {}

    const _: () = assert_send::<LinearBuffer>();
    const _: () = assert_sync::<LinearBuffer>();
    const _: () = assert_send::<Slice>();
    const _: () = assert_sync::<Slice>();
}
