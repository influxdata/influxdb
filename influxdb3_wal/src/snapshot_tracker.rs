//! This module contains the code and logic for tracking how and when to snapshot the WAL. This
//! can be used to tell the `SnapshotHandler` what is set to persist. After that WAL files
//! can be deleted from object store. The tracker is useful even for setups without a WAL
//! configured as it can be used to ensure that data in the write buffer is persisted in blocks
//! that are not too large and unlikely to overlap.

use crate::{Level0Duration, SnapshotDetails, SnapshotSequenceNumber, WalFileSequenceNumber};
use data_types::Timestamp;

/// A struct that tracks the WAL periods (files if using object store) and decides when to snapshot the WAL.
#[derive(Debug)]
pub(crate) struct SnapshotTracker {
    last_snapshot_sequence_number: SnapshotSequenceNumber,
    last_wal_sequence_number: WalFileSequenceNumber,
    wal_periods: Vec<WalPeriod>,
    snapshot_size: usize,
    level_0_duration: Level0Duration,
}

impl SnapshotTracker {
    /// Create a new `SnapshotTracker` with the given snapshot size and level 0 duration. The
    /// level 0 duration is the size of chunks in the write buffer that will be persisted as
    /// parquet files.
    pub(crate) fn new(
        snapshot_size: usize,
        level_0_duration: Level0Duration,
        last_snapshot_sequence_number: Option<SnapshotSequenceNumber>,
    ) -> Self {
        Self {
            last_snapshot_sequence_number: last_snapshot_sequence_number.unwrap_or_default(),
            last_wal_sequence_number: WalFileSequenceNumber::default(),
            wal_periods: Vec::new(),
            snapshot_size,
            level_0_duration,
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
    pub(crate) fn snapshot(&mut self) -> Option<SnapshotInfo> {
        if self.wal_periods.is_empty()
            || self.wal_periods.len() < self.number_of_periods_to_snapshot_after()
        {
            return None;
        }

        // if the number of wal periods is >= 3x the snapshot size, snapshot everything up to, but
        // not including, the last period:
        if self.wal_periods.len() >= 3 * self.snapshot_size {
            let n_periods_to_take = self.wal_periods.len() - 1;
            let wal_periods: Vec<WalPeriod> =
                self.wal_periods.drain(0..n_periods_to_take).collect();
            let max_time = wal_periods
                .iter()
                .map(|period| period.max_time)
                .max()
                .unwrap();
            let t = max_time - (max_time.get() % self.level_0_duration.as_nanos())
                + self.level_0_duration.as_nanos();
            let last_wal_sequence_number = wal_periods.last().unwrap().wal_file_number;

            let snapshot_details = SnapshotDetails {
                snapshot_sequence_number: self.increment_snapshot_sequence_number(),
                end_time_marker: t.get(),
                last_wal_sequence_number,
            };

            return Some(SnapshotInfo {
                snapshot_details,
                wal_periods,
            });
        }

        let t = self.wal_periods.last().unwrap().max_time;
        // round the last timestamp down to the level_0_duration
        let t = t - (t.get() % self.level_0_duration.as_nanos());

        // any wal period that has data before this time can be snapshot
        let periods_to_snapshot = self
            .wal_periods
            .iter()
            .take_while(|period| period.max_time < t)
            .cloned()
            .collect::<Vec<_>>();

        periods_to_snapshot.last().cloned().map(|period| {
            // remove the wal periods and return the snapshot details
            self.wal_periods
                .retain(|p| p.wal_file_number > period.wal_file_number);

            SnapshotInfo {
                snapshot_details: SnapshotDetails {
                    snapshot_sequence_number: self.increment_snapshot_sequence_number(),
                    end_time_marker: t.get(),
                    last_wal_sequence_number: period.wal_file_number,
                },
                wal_periods: periods_to_snapshot,
            }
        })
    }

    /// The number of wal periods we need to see before we attempt a snapshot. This is to ensure that we
    /// don't snapshot before we've buffered up enough data to fill a level 0 chunk.
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
}

#[derive(Debug, Eq, PartialEq)]
pub struct SnapshotInfo {
    pub(crate) snapshot_details: SnapshotDetails,
    pub(crate) wal_periods: Vec<WalPeriod>,
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
        let mut tracker = SnapshotTracker::new(2, Level0Duration::new_1m(), None);
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

        assert!(tracker.snapshot().is_none());
        tracker.add_wal_period(p1.clone());
        assert!(tracker.snapshot().is_none());
        tracker.add_wal_period(p2.clone());
        assert!(tracker.snapshot().is_none());
        tracker.add_wal_period(p3.clone());
        assert_eq!(
            tracker.snapshot(),
            Some(SnapshotInfo {
                snapshot_details: SnapshotDetails {
                    snapshot_sequence_number: SnapshotSequenceNumber::new(1),
                    end_time_marker: 120_000000000,
                    last_wal_sequence_number: WalFileSequenceNumber::new(2)
                },
                wal_periods: vec![p1, p2],
            })
        );
        tracker.add_wal_period(p4.clone());
        assert_eq!(tracker.snapshot(), None);
        tracker.add_wal_period(p5.clone());
        assert_eq!(
            tracker.snapshot(),
            Some(SnapshotInfo {
                snapshot_details: SnapshotDetails {
                    snapshot_sequence_number: SnapshotSequenceNumber::new(2),
                    end_time_marker: 240_000000000,
                    last_wal_sequence_number: WalFileSequenceNumber::new(3)
                },
                wal_periods: vec![p3]
            })
        );

        assert_eq!(tracker.wal_periods, vec![p4.clone(), p5.clone()]);

        tracker.add_wal_period(p6.clone());
        assert_eq!(
            tracker.snapshot(),
            Some(SnapshotInfo {
                snapshot_details: SnapshotDetails {
                    snapshot_sequence_number: SnapshotSequenceNumber::new(3),
                    end_time_marker: 360_000000000,
                    last_wal_sequence_number: WalFileSequenceNumber::new(5)
                },
                wal_periods: vec![p4, p5]
            })
        );

        assert!(tracker.snapshot().is_none());
    }

    #[test]
    fn snapshot_future_data_forces_snapshot() {
        let mut tracker = SnapshotTracker::new(2, Level0Duration::new_1m(), None);
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
        assert!(tracker.snapshot().is_none());
        tracker.add_wal_period(p4.clone());
        assert!(tracker.snapshot().is_none());
        tracker.add_wal_period(p5.clone());
        assert!(tracker.snapshot().is_none());
        tracker.add_wal_period(p6.clone());

        assert_eq!(
            tracker.snapshot(),
            Some(SnapshotInfo {
                snapshot_details: SnapshotDetails {
                    snapshot_sequence_number: SnapshotSequenceNumber::new(1),
                    end_time_marker: 360000000000,
                    last_wal_sequence_number: WalFileSequenceNumber::new(5)
                },
                wal_periods: vec![p1, p2, p3, p4, p5]
            })
        );
    }
}
