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
pub enum IngestStateError {
    /// Set when the ingester has exceeded its capacity for concurrent active
    /// persist jobs.
    #[error("ingester overloaded - persisting backlog")]
    PersistSaturated = 1 << 0,

    /// Set for the duration of the ingester's graceful shutdown procedure.
    #[error("ingester is shutting down")]
    GracefulStop = 1 << 1,

    /// Indicates the ingester's disk is too full to safely accept further
    /// writes.
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
pub struct IngestState {
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
    pub fn set(&self, error: IngestStateError) -> bool {
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
    pub fn unset(&self, error: IngestStateError) -> bool {
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
    /// If more than one error state is set, a single error is returned based on
    /// the following precedence (ordered by highest priority to lowest):
    ///
    ///   1. [`IngestStateError::GracefulStop`]
    ///   2. [`IngestStateError::DiskFull`]
    ///   3. [`IngestStateError::PersistSaturated`].
    ///
    pub fn read(&self) -> Result<(), IngestStateError> {
        let current = self.state.load(Ordering::Relaxed);

        if current != 0 {
            // Map the non-healthy state to an error using a "cold" function,
            // asking LLVM to move the mapping logic out of the happy/hot path.
            return as_err(current);
        }

        Ok(())
    }

    /// Return the current [`IngestStateError`], if any, filtering out any
    /// contained within the given list of `exceptions`.
    ///
    /// # Precedence
    ///
    /// If more than one error state is set, this follows the same precedence
    /// rules as [`IngestState::read()`].
    pub fn read_with_exceptions<const N: usize>(
        &self,
        exceptions: [IngestStateError; N],
    ) -> Result<(), IngestStateError> {
        let exception_mask = exceptions
            .into_iter()
            .map(IngestStateError::as_bits)
            .fold(0, std::ops::BitOr::bitor);
        let current = self.state.load(Ordering::Relaxed);

        let masked = current & !exception_mask;
        if masked != 0 {
            return as_err(masked);
        }

        Ok(())
    }
}

/// Map `state` to exactly one [`IngestStateError`].
///
/// Shutdown always takes precedence, ensuring that once set, this is the error
/// the user always sees (instead of potentially flip-flopping between "shutting
/// down", "persist saturated" & "disk full").
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
    use std::mem::discriminant;

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
        assert_matches!(
            state.read_with_exceptions([]),
            Err(IngestStateError::PersistSaturated)
        );

        state.set(IngestStateError::DiskFull);
        assert_matches!(state.read(), Err(IngestStateError::DiskFull));
        assert_matches!(
            state.read_with_exceptions([]),
            Err(IngestStateError::DiskFull)
        );

        state.set(IngestStateError::GracefulStop);
        assert_matches!(state.read(), Err(IngestStateError::GracefulStop));
        assert_matches!(
            state.read_with_exceptions([]),
            Err(IngestStateError::GracefulStop)
        );

        // The persist state does not affect the shutdown error.
        state.unset(IngestStateError::PersistSaturated);
        assert_matches!(state.read(), Err(IngestStateError::GracefulStop));
        assert_matches!(
            state.read_with_exceptions([]),
            Err(IngestStateError::GracefulStop)
        );
        state.set(IngestStateError::PersistSaturated);
        assert_matches!(state.read(), Err(IngestStateError::GracefulStop));
        assert_matches!(
            state.read_with_exceptions([]),
            Err(IngestStateError::GracefulStop)
        );

        // And neither does the disk full state.
        state.unset(IngestStateError::DiskFull);
        assert_matches!(state.read(), Err(IngestStateError::GracefulStop));
        assert_matches!(
            state.read_with_exceptions([]),
            Err(IngestStateError::GracefulStop)
        );
        state.set(IngestStateError::DiskFull);
        assert_matches!(state.read(), Err(IngestStateError::GracefulStop));
        assert_matches!(
            state.read_with_exceptions([]),
            Err(IngestStateError::GracefulStop)
        );

        // Un-setting the shutdown state shows the disk full state.
        state.unset(IngestStateError::GracefulStop);
        assert_matches!(state.read(), Err(IngestStateError::DiskFull));
        assert_matches!(
            state.read_with_exceptions([]),
            Err(IngestStateError::DiskFull)
        );

        // Un-setting the disk full state then shows the persist saturated state.
        state.unset(IngestStateError::DiskFull);
        assert_matches!(state.read(), Err(IngestStateError::PersistSaturated));
        assert_matches!(
            state.read_with_exceptions([]),
            Err(IngestStateError::PersistSaturated)
        );
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

    fn disjoint_set(not: &[IngestStateError]) -> Vec<IngestStateError> {
        [
            IngestStateError::PersistSaturated,
            IngestStateError::GracefulStop,
            IngestStateError::DiskFull,
        ]
        .into_iter()
        .filter(|v| !not.iter().any(|w| discriminant(v) == discriminant(w)))
        .collect()
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

            if discriminant(&error_a) != discriminant(&error_b) {
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
                prop_assert_eq!(discriminant(&inner), discriminant(&error));
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
            if discriminant(&error_a) != discriminant(&error_b) {
                // First call for a different state is told it is the first to
                // set that state.
                prop_assert!(state.set(error_b));

                // Unsetting one state must not reset the other.
                prop_assert!(state.unset(error_b));
                prop_assert!(!state.set(error_a));
            }
        }

        /// For every [`IngestStateError`], check that reading an [`IngestState`]
        /// while specifying it as an exception returns `Ok()` when:
        ///
        ///  * No error is set
        ///  * The error marked as an exception is set
        ///  * Multiple errors are set, but are a subset of the exceptions
        ///
        /// It also ensures that an error variant other than the exception is
        /// returned as expected.
        #[test]
        fn test_read_with_exceptions(set_error in ingest_state_errors()) {
            let state = IngestState::default();

            // Assert reading a state with no error will always succeed.
            assert_matches!(state.read_with_exceptions([set_error]), Ok(()));
            for v in disjoint_set(&[set_error]) {
                assert_matches!(state.read_with_exceptions([v]), Ok(()));
            }

            // Set the error, then ensure that it is correctly excepted, but
            // still returned for other reads.
            assert!(state.set(set_error));

            assert_matches!(state.read_with_exceptions([]), Err(_));
            for v in disjoint_set(&[set_error]) {
                assert_matches!(state.read_with_exceptions([v]), Err(got_error) => {
                    assert_eq!(discriminant(&got_error), discriminant(&set_error));
                });

                // Temporarily set the additional error, include it in the
                // exceptions and ensure the other error is still returned
                assert!(state.set(v));
                assert_matches!(state.read_with_exceptions([v, set_error]), Ok(()));

                assert_matches!(state.read_with_exceptions([
                    set_error,
                    disjoint_set(&[v, set_error])
                    .pop()
                    .expect("this check cannot be made for less than 3 possible error states")
                ]), Err(got_error) => {
                    assert_eq!(discriminant(&got_error), discriminant(&v));
                });
                assert!(state.unset(v));
            }
            assert_matches!(state.read_with_exceptions([set_error]), Ok(()));
        }
    }
}
