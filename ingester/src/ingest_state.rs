//! Propagate subsystem error state between other subsystems.
//!
//! A [`IngestState`] allows disparate subsystems to broadcast their health
//! state to other subsystems. Concretely, it is used to reject writes when the
//! ingester is unable to process them.

use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;
use thiserror::Error;

/// The set of error states for the ingest system.
///
/// Each error variant has a discriminant value that has exactly set one bit in
/// a usize.
#[derive(Debug, Error, Clone, Copy)]
pub(crate) enum IngestStateError {
    #[error("ingester overloaded - persisting backlog")]
    PersistSaturated = 1 << 0,

    #[error("ingester is shutting down")]
    GracefulStop = 1 << 1,

    #[error("ingester disk full - persisting write-ahead log")]
    DiskFull = 1 << 2,
}

impl IngestStateError {
    #[inline(always)]
    fn as_bits(self) -> usize {
        // Map the user-friendly enum to a u64 bitmap.
        let set = self as usize;
        debug_assert_eq!(set.count_ones(), 1); // A single bit
        debug_assert!(set < usize::BITS as usize); // No shifting out of 64 bits
        set
    }
}

/// A thread-safe, cheap-to-read "ingest error" state to propagate errors from
/// subsystems to the ingest code path. Many error states can be set at any one
/// time, but at most one error is returned by a call to
/// [`IngestState::read()`].
///
/// Calling [`IngestState::read()`] returns [`Ok`], or the error currently set
/// with [`IngestState::set()`].
///
/// Error states can be unset by calling [`IngestState::unset()`].
#[derive(Debug, Default)]
pub(crate) struct IngestState {
    /// The actual state value.
    ///
    /// The value of this variable is a bitmap covering the [`IngestStateError`]
    /// discriminants, each of which are a disjoint bit.
    ///
    /// This is cache padded due to the high read volume, preventing any
    /// unfortunate false-sharing of cache lines from impacting the hot-path
    /// reads.
    state: CachePadded<AtomicUsize>,
}

impl IngestState {
    /// Set the [`IngestState`] to return `error` when [`IngestState::read()`]
    /// is called.
    ///
    /// Returns true if this call set the error state to `error`, false if
    /// `error` was already set.
    pub(crate) fn set(&self, error: IngestStateError) -> bool {
        let set = error.as_bits();
        let mut current = self.state.load(Ordering::Relaxed);
        loop {
            if current & set != 0 {
                // The "set" state was already set, and therefore this thread
                // wasn't the first to set it.
                return false;
            }

            // Set the state bit for `set` in the current state.
            let new = current | set;

            // Try and set the new state
            match self.state.compare_exchange_weak(
                current,
                new,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(v) => {
                    core::hint::spin_loop();
                    current = v;
                }
            }
        }
    }

    /// Clear the error state of `error`.
    ///
    /// Any currently set [`IngestStateError`] state other than `error` persists
    /// after this call.
    ///
    /// Returns true if this call unset the `error` state, false if `error` was
    /// already unset.
    pub(crate) fn unset(&self, error: IngestStateError) -> bool {
        let unset = error.as_bits();
        let mut current = self.state.load(Ordering::Relaxed);
        loop {
            if current & unset == 0 {
                // The "unset" state is not set.
                return false;
            }

            // Unset the state bit for `unset` in the current state.
            let new = current & (!unset);

            // Try and set the new state
            match self.state.compare_exchange_weak(
                current,
                new,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(v) => {
                    core::hint::spin_loop();
                    current = v;
                }
            }
        }
    }

    /// Return the current [`IngestStateError`], if any.
    ///
    /// # Precedence
    ///
    /// If more than one error state it set, a single error is returned based on
    /// the following precedence (ordered by highest priority to lowest):
    ///
    ///   1. [`IngestStateError::GracefulStop`]
    ///   2. [`IngestStateError::PersistSaturated`].
    ///
    pub(crate) fn read(&self) -> Result<(), IngestStateError> {
        let current = self.state.load(Ordering::Relaxed);

        if current != 0 {
            // Map the non-healthy state to an error using a "cold" function,
            // asking LLVM to move the mapping logic out of the happy/hot path.
            return as_err(current);
        }

        Ok(())
    }
}

/// Map `state` to exactly one [`IngestStateError`].
///
/// Shutdown always takes precedence, ensuring that once set, this is the error
/// the user always sees (instead of potentially flip-flopping between "shutting
/// down" and "persist saturated").
#[cold]
fn as_err(state: usize) -> Result<(), IngestStateError> {
    if state & IngestStateError::GracefulStop.as_bits() != 0 {
        return Err(IngestStateError::GracefulStop);
    }

    if state & IngestStateError::DiskFull.as_bits() != 0 {
        return Err(IngestStateError::DiskFull);
    }

    if state & IngestStateError::PersistSaturated.as_bits() != 0 {
        return Err(IngestStateError::PersistSaturated);
    }

    unreachable!()
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn test_default_state() {
        let state = IngestState::default();
        assert!(state.read().is_ok());
    }

    #[test]
    fn test_error_state_precedence() {
        let state = IngestState::default();

        state.set(IngestStateError::PersistSaturated);
        assert_matches!(state.read(), Err(IngestStateError::PersistSaturated));

        state.set(IngestStateError::DiskFull);
        assert_matches!(state.read(), Err(IngestStateError::DiskFull));

        state.set(IngestStateError::GracefulStop);
        assert_matches!(state.read(), Err(IngestStateError::GracefulStop));

        // The persist state does not affect the shutdown error.
        state.unset(IngestStateError::PersistSaturated);
        assert_matches!(state.read(), Err(IngestStateError::GracefulStop));
        state.set(IngestStateError::PersistSaturated);
        assert_matches!(state.read(), Err(IngestStateError::GracefulStop));

        // And neither does the disk full state.
        state.unset(IngestStateError::DiskFull);
        assert_matches!(state.read(), Err(IngestStateError::GracefulStop));
        state.set(IngestStateError::DiskFull);
        assert_matches!(state.read(), Err(IngestStateError::GracefulStop));

        // Un-setting the shutdown state shows the disk full state.
        state.unset(IngestStateError::GracefulStop);
        assert_matches!(state.read(), Err(IngestStateError::DiskFull));

        // Un-setting the disk full state then shows the persist saturated state.
        state.unset(IngestStateError::DiskFull);
        assert_matches!(state.read(), Err(IngestStateError::PersistSaturated));
    }

    /// A hand-rolled strategy to enumerate [`IngestStateError`] variants.
    /// New variants MUST be added here.
    fn ingest_state_errors() -> impl Strategy<Value = IngestStateError> {
        prop_oneof![
            Just(IngestStateError::PersistSaturated),
            Just(IngestStateError::GracefulStop),
            Just(IngestStateError::DiskFull)
        ]
    }

    proptest! {
        // This test throws a compiler error if a new state is added and not
        // covered by the test strategy generation for [`IngestStateError`].
        #[test]
        fn test_ingest_state_error_strategy(error in ingest_state_errors()) {
            match error {
                IngestStateError::PersistSaturated => {}
                IngestStateError::GracefulStop => {}
                IngestStateError::DiskFull => {}
            }
        }

        /// For every [`IngestStateError`] pair, ensure they do not overflow
        /// a `usize`, contain a single bit and (if different variants), do
        /// not share the bit placement.
        #[test]
        fn test_disjoint_discriminant_bits_prop(error_a in ingest_state_errors(), error_b in ingest_state_errors()) {
            prop_assert!(error_a.as_bits() < usize::BITS as usize);
            prop_assert_eq!(error_a.as_bits().count_ones(), 1);

            prop_assert!(error_b.as_bits() < usize::BITS as usize);
            prop_assert_eq!(error_b.as_bits().count_ones(), 1);

            if std::mem::discriminant(&error_a) != std::mem::discriminant(&error_b) {
                prop_assert_ne!(error_a.as_bits(), error_b.as_bits());
            }
        }

        /// For every [`IngestStateError`], setup a new [`IngestState`], ensure
        /// it writes are proceedable for it, then toggle setting the state error
        /// on and off.
        #[test]
        fn test_set_unset(error in ingest_state_errors()) {
            let state = IngestState::default();

            assert_matches!(state.read(), Ok(()));

            prop_assert!(state.set(error));
            assert_matches!(state.read(), Err(inner) => {
                prop_assert_eq!(std::mem::discriminant(&inner), std::mem::discriminant(&error));
            });

            prop_assert!(state.unset(error));
            assert_matches!(state.read(), Ok(()));
        }

        /// For every [`IngestStateError`] pair, this test checks that setting
        /// and unsetting `error_a` reports correctly whether it is the first to
        /// unset or set the state.
        ///
        /// This test also ensures that for disjoint `error_a` and `error_b`
        /// setting/unsetting one does not reset or affect the other.
        #[test]
        fn test_set_first_caller(error_a in ingest_state_errors(), error_b in ingest_state_errors()) {
            let state = IngestState::default();

            // First call is always told it is the first to set the state.
            prop_assert!(state.set(error_a));
            // Second call does not see "first == true"
            prop_assert!(!state.set(error_a));

            // Unsetting, then resetting reports first == true again, as well
            // as being informed it is the first to unset the state.
            prop_assert!(state.unset(error_a));
            prop_assert!(!state.unset(error_a));
            prop_assert!(state.set(error_a));

            // Remaining checks only apply if error_b is a different error state.
            if std::mem::discriminant(&error_a) != std::mem::discriminant(&error_b) {
                // First call for a different state is told it is the first to
                // set that state.
                prop_assert!(state.set(error_b));

                // Unsetting one state must not reset the other.
                prop_assert!(state.unset(error_b));
                prop_assert!(!state.set(error_a));
            }
        }
    }
}
