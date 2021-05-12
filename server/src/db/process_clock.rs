//! Process clock used in establishing a partial ordering of operations via a Lamport Clock.

use chrono::Utc;
use entry::ClockValue;
use std::{
    convert::{TryFrom, TryInto},
    sync::atomic::{AtomicU64, Ordering},
};

#[derive(Debug)]
pub struct ProcessClock {
    inner: AtomicU64,
}

impl ProcessClock {
    /// Create a new process clock value initialized to the current system time.
    pub fn new() -> Self {
        Self {
            inner: AtomicU64::new(system_clock_now()),
        }
    }

    /// Returns the next process clock value, which will be the maximum of the system time in
    /// nanoseconds or the previous process clock value plus 1. Every operation that needs a
    /// process clock value should be incrementing it as well, so there should never be a read of
    /// the process clock without an accompanying increment of at least 1 nanosecond.
    ///
    /// We expect that updates to the process clock are not so frequent and the system is slow
    /// enough that the returned value will be incremented by at least 1.
    pub fn next(&self) -> ClockValue {
        let next = loop {
            if let Ok(next) = self.try_update() {
                break next;
            }
        };

        ClockValue::try_from(next).expect("process clock should not be 0")
    }

    fn try_update(&self) -> Result<u64, u64> {
        let now = system_clock_now();
        let current_process_clock = self.inner.load(Ordering::SeqCst);
        let next_candidate = current_process_clock + 1;

        let next = now.max(next_candidate);

        self.inner
            .compare_exchange(
                current_process_clock,
                next,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map(|_| next)
    }
}

// Convenience function for getting the current time in a `u64` represented as nanoseconds since
// the epoch
//
// While this might jump backwards, the logic above that takes the maximum of the current process
// clock and the value returned from this function should ensure that the process clock is
// strictly increasing.
fn system_clock_now() -> u64 {
    Utc::now()
        .timestamp_nanos()
        .try_into()
        .expect("current time since the epoch should be positive")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_tests::utils::TestDb;
    use entry::test_helpers::lp_to_entry;
    use std::{sync::Arc, thread, time::Duration};

    #[test]
    fn process_clock_defaults_to_current_time_in_ns() {
        let before = system_clock_now();

        let db = Arc::new(TestDb::builder().build().db);
        let db_process_clock = db.process_clock.inner.load(Ordering::SeqCst);

        let after = system_clock_now();

        assert!(
            before < db_process_clock,
            "expected {} to be less than {}",
            before,
            db_process_clock
        );
        assert!(
            db_process_clock < after,
            "expected {} to be less than {}",
            db_process_clock,
            after
        );
    }

    #[test]
    fn process_clock_incremented_and_set_on_sequenced_entry() {
        let before = system_clock_now();
        let before = ClockValue::try_from(before).unwrap();

        let db = Arc::new(TestDb::builder().write_buffer(true).build().db);

        let entry = lp_to_entry("cpu bar=1 10");
        db.store_entry(entry).unwrap();

        let between = system_clock_now();
        let between = ClockValue::try_from(between).unwrap();

        let entry = lp_to_entry("cpu foo=2 10");
        db.store_entry(entry).unwrap();

        let after = system_clock_now();
        let after = ClockValue::try_from(after).unwrap();

        let sequenced_entries = db
            .write_buffer
            .as_ref()
            .unwrap()
            .lock()
            .writes_since(before);
        assert_eq!(sequenced_entries.len(), 2);

        assert!(
            sequenced_entries[0].clock_value() < between,
            "expected {:?} to be before {:?}",
            sequenced_entries[0].clock_value(),
            between
        );

        assert!(
            between < sequenced_entries[1].clock_value(),
            "expected {:?} to be before {:?}",
            between,
            sequenced_entries[1].clock_value(),
        );

        assert!(
            sequenced_entries[1].clock_value() < after,
            "expected {:?} to be before {:?}",
            sequenced_entries[1].clock_value(),
            after
        );
    }

    #[test]
    fn next_process_clock_always_increments() {
        // Process clock defaults to the current time
        let db = Arc::new(TestDb::builder().write_buffer(true).build().db);

        // Set the process clock value to a time in the future, so that when compared to the
        // current time, the process clock value will be greater
        let later: u64 = (Utc::now() + chrono::Duration::weeks(4))
            .timestamp_nanos()
            .try_into()
            .unwrap();

        db.process_clock.inner.store(later, Ordering::SeqCst);

        // Every call to next_process_clock should increment at least 1, even in this case
        // where the system time will be less than the process clock
        assert_eq!(
            db.process_clock.next(),
            ClockValue::try_from(later + 1).unwrap()
        );
        assert_eq!(
            db.process_clock.next(),
            ClockValue::try_from(later + 2).unwrap()
        );
    }

    #[test]
    fn process_clock_multithreaded_access_always_increments() {
        let pc = Arc::new(ProcessClock::new());

        let handles: Vec<_> = (0..10)
            .map(|thread_num| {
                let pc = Arc::clone(&pc);
                thread::spawn(move || {
                    let mut pc_val_before = pc.next();
                    for iteration in 0..10 {
                        let pc_val_after = pc.next();

                        // This might be useful for debugging if this test fails
                        println!(
                            "thread {} in iteration {} testing {:?} < {:?}",
                            thread_num, iteration, pc_val_before, pc_val_after
                        );

                        // Process clock should always increase
                        assert!(
                            pc_val_before < pc_val_after,
                            "expected {:?} to be less than {:?}",
                            pc_val_before,
                            pc_val_after
                        );

                        pc_val_before = pc_val_after;

                        // encourage yielding
                        thread::sleep(Duration::from_millis(1));
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }
}
