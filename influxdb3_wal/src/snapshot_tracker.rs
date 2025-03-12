//! This module contains the code and logic for tracking how and when to snapshot the WAL. This
//! can be used to tell the `SnapshotHandler` what is set to persist. After that WAL files
//! can be deleted from object store. The tracker is useful even for setups without a WAL
//! configured as it can be used to ensure that data in the write buffer is persisted in blocks
//! that are not too large and unlikely to overlap.

use crate::{Gen1Duration, SnapshotDetails, SnapshotSequenceNumber, WalFileSequenceNumber};
use data_types::Timestamp;
use observability_deps::tracing::{debug, info};

/// A struct that tracks the WAL periods (files if using object store) and decides when to snapshot the WAL.
#[derive(Debug)]
pub(crate) struct SnapshotTracker {
    last_snapshot_sequence_number: SnapshotSequenceNumber,
    last_wal_sequence_number: WalFileSequenceNumber,
    wal_periods: Vec<WalPeriod>,
    snapshot_size: usize,
    gen1_duration: Gen1Duration,
}

impl SnapshotTracker {
    /// Create a new `SnapshotTracker` with the given snapshot size and gen1 duration. The
    /// gen1 duration is the size of chunks in the write buffer that will be persisted as
    /// parquet files.
    pub(crate) fn new(
        snapshot_size: usize,
        gen1_duration: Gen1Duration,
        last_snapshot_sequence_number: Option<SnapshotSequenceNumber>,
    ) -> Self {
        Self {
            last_snapshot_sequence_number: last_snapshot_sequence_number.unwrap_or_default(),
            last_wal_sequence_number: WalFileSequenceNumber::default(),
            wal_periods: Vec::new(),
            snapshot_size,
            gen1_duration,
        }
    }

    /// Add a wal period to the tracker. This should be called when a new wal file is created.
    ///
    /// # Panics
    /// This will panic if the new period overlaps with the last period.
    pub(crate) fn add_wal_period(&mut self, wal_period: WalPeriod) {
        if let Some(last_period) = self.wal_periods.last() {
            assert!(last_period.wal_file_number < wal_period.wal_file_number);
        }

        self.last_wal_sequence_number = wal_period.wal_file_number;
        self.wal_periods.push(wal_period);
    }

    /// Returns the max_time and the last_wal_file_sequence_number that can be snapshot along with
    /// the wal periods. The `SnapshotDetails` get handed over to the file notifier, which will
    /// persist any chunks with data < max_time. The WAL will keep track of the returned periods so
    /// that when the snapshot is done, it can delete the associated files.
    ///
    /// In the case of data coming in for future times, we will be unable to snapshot older data.
    /// Over time this will back up the WAL. To guard against this, if the number of WAL periods
    /// is >= 3x the snapshot size, snapshot everything up to the last period.
    pub(crate) fn snapshot(&mut self, force_snapshot: bool) -> Option<SnapshotDetails> {
        debug!(
            wal_periods_len = ?self.wal_periods.len(),
            num_snapshots_after = ?self.number_of_periods_to_snapshot_after(),
            "wal periods and snapshots"
        );

        if !self.should_run_snapshot(force_snapshot) {
            return None;
        }

        // if the number of wal periods is >= 3x the snapshot size, snapshot everything
        let wal_periods_3_times_snapshot_size = self.wal_periods.len() >= 3 * self.snapshot_size;
        if force_snapshot || wal_periods_3_times_snapshot_size {
            info!(
                ?force_snapshot,
                ?wal_periods_3_times_snapshot_size,
                "snapshotting all"
            );
            return self.snapshot_all();
        }

        info!(
            ?force_snapshot,
            ?wal_periods_3_times_snapshot_size,
            "snapshotting all before last wal period (using last wal period time)"
        );
        // uses the last wal period's time to leave behind "some" of the wal periods
        // for default config (gen1 duration is 10m / flush interval 1s), it leaves
        // behind 300 wal periods.
        self.snapshot_in_order_wal_periods()
    }

    fn should_run_snapshot(&mut self, force_snapshot: bool) -> bool {
        // When force_snapshot is set the wal_periods won't be empty, as call site always adds a
        // no-op when wal buffer is empty and adds the wal period
        if self.wal_periods.is_empty() {
            if force_snapshot {
                info!("cannot force a snapshot when wal periods are empty");
            }
            return false;
        }

        // if force_snapshot is set, we don't need to check wal periods len or num periods we
        // snapshot immediately
        if !force_snapshot && self.wal_periods.len() < self.number_of_periods_to_snapshot_after() {
            return false;
        }

        true
    }

    pub(crate) fn snapshot_in_order_wal_periods(&mut self) -> Option<SnapshotDetails> {
        let t = self.wal_periods.last()?.max_time;
        // round the last timestamp down to the gen1_duration
        let t = t - (t.get() % self.gen1_duration.as_nanos());
        debug!(timestamp_ns = ?t, gen1_duration_ns = ?self.gen1_duration.as_nanos(), "last timestamp");

        // any wal period that has data before this time can be snapshot
        let periods_to_snapshot = self
            .wal_periods
            .iter()
            .take_while(|period| period.max_time < t)
            .cloned()
            .collect::<Vec<_>>();
        debug!(?periods_to_snapshot, "periods to snapshot");
        let first_wal_file_number = periods_to_snapshot
            .iter()
            .peekable()
            .peek()
            .map(|period| period.wal_file_number)?;

        let last_wal_file_number = periods_to_snapshot
            .iter()
            .last()
            .map(|period| period.wal_file_number)?;

        // remove the wal periods and return the snapshot details
        self.wal_periods
            .retain(|p| p.wal_file_number > last_wal_file_number);

        Some(SnapshotDetails {
            snapshot_sequence_number: self.increment_snapshot_sequence_number(),
            end_time_marker: t.get(),
            first_wal_sequence_number: first_wal_file_number,
            last_wal_sequence_number: last_wal_file_number,
            forced: false,
        })
    }

    fn snapshot_all(&mut self) -> Option<SnapshotDetails> {
        let wal_periods: Vec<WalPeriod> = self.wal_periods.drain(..).collect();
        let max_time = wal_periods.iter().map(|period| period.max_time).max()?;
        let t = max_time - (max_time.get() % self.gen1_duration.as_nanos())
            + self.gen1_duration.as_nanos();
        let first_wal_sequence_number = wal_periods.first()?.wal_file_number;
        let last_wal_sequence_number = wal_periods.last()?.wal_file_number;
        let snapshot_details = SnapshotDetails {
            snapshot_sequence_number: self.increment_snapshot_sequence_number(),
            end_time_marker: t.get(),
            first_wal_sequence_number,
            last_wal_sequence_number,
            forced: true,
        };

        Some(snapshot_details)
    }

    /// The number of wal periods we need to see before we attempt a snapshot. This is to ensure that we
    /// don't snapshot before we've buffered up enough data to fill a gen1 chunk.
    fn number_of_periods_to_snapshot_after(&self) -> usize {
        self.snapshot_size + self.snapshot_size / 2
    }

    /// Returns the last [`WalFileSequenceNumber`] that was added to the tracker.
    pub(crate) fn last_wal_sequence_number(&self) -> WalFileSequenceNumber {
        self.last_wal_sequence_number
    }

    /// Returns the last [`SnapshotSequenceNumber`] that was added to the tracker.
    pub(crate) fn last_snapshot_sequence_number(&self) -> SnapshotSequenceNumber {
        self.last_snapshot_sequence_number
    }

    fn increment_snapshot_sequence_number(&mut self) -> SnapshotSequenceNumber {
        self.last_snapshot_sequence_number = self.last_snapshot_sequence_number.next();
        self.last_snapshot_sequence_number
    }

    #[cfg(test)]
    pub(crate) fn num_wal_periods(&self) -> usize {
        self.wal_periods.len()
    }
}

/// A struct that represents a period of time in the WAL. This is used to track the data timestamps
/// and sequence numbers for each period of the WAL (which will be a file in object store, if enabled).
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct WalPeriod {
    pub(crate) wal_file_number: WalFileSequenceNumber,
    pub(crate) min_time: Timestamp,
    pub(crate) max_time: Timestamp,
}

impl WalPeriod {
    pub(crate) fn new(
        wal_file_number: WalFileSequenceNumber,
        min_time: Timestamp,
        max_time: Timestamp,
    ) -> Self {
        info!(
            ?min_time,
            ?max_time,
            ?wal_file_number,
            "timestamps passed in and wal file num"
        );
        assert!(min_time <= max_time);

        Self {
            wal_file_number,
            min_time,
            max_time,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot() {
        let mut tracker = SnapshotTracker::new(2, Gen1Duration::new_1m(), None);
        let p1 = WalPeriod::new(
            WalFileSequenceNumber::new(1),
            Timestamp::new(0),
            Timestamp::new(60_000000000),
        );
        let p2 = WalPeriod::new(
            WalFileSequenceNumber::new(2),
            Timestamp::new(800000001),
            Timestamp::new(119_900000000),
        );
        let p3 = WalPeriod::new(
            WalFileSequenceNumber::new(3),
            Timestamp::new(60_600000000),
            Timestamp::new(120_900000000),
        );
        let p4 = WalPeriod::new(
            WalFileSequenceNumber::new(4),
            Timestamp::new(120_800000001),
            Timestamp::new(240_000000000),
        );
        let p5 = WalPeriod::new(
            WalFileSequenceNumber::new(5),
            Timestamp::new(180_800000001),
            Timestamp::new(299_000000000),
        );
        let p6 = WalPeriod::new(
            WalFileSequenceNumber::new(6),
            Timestamp::new(240_800000001),
            Timestamp::new(360_100000000),
        );

        assert!(tracker.snapshot(false).is_none());
        tracker.add_wal_period(p1.clone());
        assert!(tracker.snapshot(false).is_none());
        tracker.add_wal_period(p2.clone());
        assert!(tracker.snapshot(false).is_none());
        tracker.add_wal_period(p3.clone());
        assert_eq!(
            tracker.snapshot(false),
            Some(SnapshotDetails {
                snapshot_sequence_number: SnapshotSequenceNumber::new(1),
                end_time_marker: 120_000000000,
                first_wal_sequence_number: WalFileSequenceNumber::new(1),
                last_wal_sequence_number: WalFileSequenceNumber::new(2),
                forced: false,
            })
        );
        tracker.add_wal_period(p4.clone());
        assert_eq!(tracker.snapshot(false), None);
        tracker.add_wal_period(p5.clone());
        assert_eq!(
            tracker.snapshot(false),
            Some(SnapshotDetails {
                snapshot_sequence_number: SnapshotSequenceNumber::new(2),
                end_time_marker: 240_000000000,
                first_wal_sequence_number: WalFileSequenceNumber::new(3),
                last_wal_sequence_number: WalFileSequenceNumber::new(3),
                forced: false,
            })
        );

        assert_eq!(tracker.wal_periods, vec![p4.clone(), p5.clone()]);

        tracker.add_wal_period(p6.clone());
        assert_eq!(
            tracker.snapshot(false),
            Some(SnapshotDetails {
                snapshot_sequence_number: SnapshotSequenceNumber::new(3),
                end_time_marker: 360_000000000,
                first_wal_sequence_number: WalFileSequenceNumber::new(4),
                last_wal_sequence_number: WalFileSequenceNumber::new(5),
                forced: false,
            })
        );

        assert!(tracker.snapshot(false).is_none());
    }

    #[test]
    fn snapshot_future_data_forces_snapshot() {
        let mut tracker = SnapshotTracker::new(2, Gen1Duration::new_1m(), None);
        let p1 = WalPeriod::new(
            WalFileSequenceNumber::new(1),
            Timestamp::new(0),
            Timestamp::new(300_100000000),
        );
        let p2 = WalPeriod::new(
            WalFileSequenceNumber::new(2),
            Timestamp::new(30_000000000),
            Timestamp::new(59_900000000),
        );
        let p3 = WalPeriod::new(
            WalFileSequenceNumber::new(3),
            Timestamp::new(60_000000000),
            Timestamp::new(60_900000000),
        );
        let p4 = WalPeriod::new(
            WalFileSequenceNumber::new(4),
            Timestamp::new(90_000000000),
            Timestamp::new(120_000000000),
        );
        let p5 = WalPeriod::new(
            WalFileSequenceNumber::new(5),
            Timestamp::new(120_000000000),
            Timestamp::new(150_000000000),
        );
        let p6 = WalPeriod::new(
            WalFileSequenceNumber::new(6),
            Timestamp::new(150_000000000),
            Timestamp::new(180_100000000),
        );

        tracker.add_wal_period(p1.clone());
        tracker.add_wal_period(p2.clone());
        tracker.add_wal_period(p3.clone());
        assert!(tracker.snapshot(false).is_none());
        tracker.add_wal_period(p4.clone());
        assert!(tracker.snapshot(false).is_none());
        tracker.add_wal_period(p5.clone());
        assert!(tracker.snapshot(false).is_none());
        tracker.add_wal_period(p6.clone());

        assert_eq!(
            tracker.snapshot(false),
            Some(SnapshotDetails {
                snapshot_sequence_number: SnapshotSequenceNumber::new(1),
                end_time_marker: 360000000000,
                first_wal_sequence_number: WalFileSequenceNumber::new(1),
                last_wal_sequence_number: WalFileSequenceNumber::new(6),
                forced: true,
            })
        );
    }
}
