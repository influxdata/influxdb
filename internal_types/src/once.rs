use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};

/// A non-zero value that can be set once
#[derive(Debug, Default)]
pub struct OnceNonZeroU32(AtomicU32);

impl OnceNonZeroU32 {
    pub fn new() -> Self {
        Self(AtomicU32::new(0))
    }

    pub fn get(&self) -> Option<NonZeroU32> {
        NonZeroU32::new(self.0.load(Ordering::Relaxed))
    }

    pub fn set(&self, value: NonZeroU32) -> Result<(), NonZeroU32> {
        match self
            .0
            .compare_exchange(0, value.get(), Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => Ok(()),
            Err(id) => Err(NonZeroU32::new(id).unwrap()), // Must be non-zero in order to fail
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_once_non_zero() {
        let a = OnceNonZeroU32::default();

        assert!(a.get().is_none());
        a.set(NonZeroU32::new(293).unwrap()).unwrap();
        assert_eq!(a.get().unwrap().get(), 293);
        assert_eq!(
            a.set(NonZeroU32::new(2334).unwrap()).unwrap_err().get(),
            293
        );
    }
}
