//! Allocation-related tools.

use std::{
    alloc::Layout,
    mem::MaybeUninit,
    num::NonZeroUsize,
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

/// An allocation of potentially uninitialized memory.
///
/// This is basically `Box<[MaybeUninit<u8>]>` but allows us to control the alignment as well.
pub(crate) struct Allocation {
    layout: Layout,
    ptr: NonNull<u8>,
}

impl Allocation {
    /// Create new allocation with given size and alignment.
    pub(crate) fn new(size: usize, alignment: NonZeroUsize) -> Self {
        let layout = Layout::array::<u8>(size)
            .expect("size fits `isize`")
            .align_to(alignment.get())
            .expect("valid alignment");

        let ptr = if size == 0 {
            // That's basically what the standard library does for empty `Vec`s. We are allowed to create an empty
            // slice based on this pointer.
            NonNull::<u8>::without_provenance(alignment)
        } else {
            // SAFETY: we made sure that the size is non-zero
            let ptr = unsafe { std::alloc::alloc(layout) };

            match NonNull::new(ptr) {
                Some(ptr) => ptr,
                None => {
                    panic!("cannot allocate {size} bytes with alignment {alignment}")
                }
            }
        };

        Self { layout, ptr }
    }

    /// Correctly typed pointer.
    fn ptr(&self) -> NonNull<MaybeUninit<u8>> {
        self.ptr.cast()
    }
}

impl Drop for Allocation {
    fn drop(&mut self) {
        let Self { layout, ptr } = self;

        if layout.size() != 0 {
            // SAFETY: this is a valid pointer and there are no dangling references
            unsafe { std::alloc::dealloc(ptr.as_ptr(), *layout) };
        }
    }
}

impl Deref for Allocation {
    type Target = [MaybeUninit<u8>];

    fn deref(&self) -> &Self::Target {
        // SAFETY: this is a valid pointer
        unsafe { std::slice::from_raw_parts(self.ptr().as_ptr(), self.layout.size()) }
    }
}

impl DerefMut for Allocation {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: this is a valid pointer
        unsafe { std::slice::from_raw_parts_mut(self.ptr().as_ptr(), self.layout.size()) }
    }
}
