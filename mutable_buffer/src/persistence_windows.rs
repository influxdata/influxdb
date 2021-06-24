//!  In memory structures for tracking data ingest and when persistence can or should occur.
use entry::Sequence;

use std::{
    collections::{BTreeMap, VecDeque},
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use snafu::Snafu;

#[derive(Debug, Copy, Clone, Snafu)]
pub enum Error {
    #[snafu(display(
        "Late arrival window {:#?} too short. Minimum value should be >= {:#?}",
        value,
        CLOSED_WINDOW_PERIOD
    ))]
    ArrivalWindowTooShort { value: Duration },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

const CLOSED_WINDOW_PERIOD: Duration = Duration::from_secs(30);

/// PersistenceWindows keep track of ingested data within a partition to determine when it
/// can be persisted. This allows IOx to receive out of order writes (in their timestamps) while
/// persisting mostly in non-time overlapping Parquet files.
///
/// The sequencer_id in the code below will map to a Kafka partition. The sequence_number maps
/// to a Kafka offset. Because IOx will run without Kafka, we use the more generic terms rather
/// than the Kafka terminology.
#[derive(Debug, Clone)]
pub struct PersistenceWindows {
    persistable: Option<Window>,
    closed: VecDeque<Window>,
    open: Option<Window>,
    late_arrival_period: Duration,
}

impl PersistenceWindows {
    pub fn new(late_arrival_period: Duration) -> Result<Self> {
        let late_arrival_seconds = late_arrival_period.as_secs();
        let closed_window_seconds = CLOSED_WINDOW_PERIOD.as_secs();

        if late_arrival_seconds < closed_window_seconds {
            return ArrivalWindowTooShort {
                value: late_arrival_period,
            }
            .fail();
        }

        let closed_window_count = late_arrival_seconds / closed_window_seconds;

        Ok(Self {
            persistable: None,
            closed: VecDeque::with_capacity(closed_window_count as usize),
            open: None,
            late_arrival_period,
        })
    }

    /// Updates the windows with the information from a batch of rows from a single sequencer
    /// to the same partition. The min and max times are the times on the row data. The `received_at`
    /// Instant is when the data was received. Taking it in this function is really just about
    /// dependency injection for testing purposes. Otherwise, this function wouldn't take that
    /// parameter and just use `Instant::now()`.
    ///
    /// The `received_at` is used by the lifecycle manager to determine how long the data in a
    /// persistence window has been sitting in memory. If it is over the configured threshold
    /// the data should be persisted.
    ///
    /// The times passed in are used to determine where to split the in-memory data when persistence
    /// is triggered (either by crossing a row count threshold or time).
    pub fn add_range(
        &mut self,
        sequence: &Sequence,
        row_count: usize,
        min_time: DateTime<Utc>,
        max_time: DateTime<Utc>,
        received_at: Instant,
    ) {
        self.rotate(received_at);

        match self.open.as_mut() {
            Some(w) => w.add_range(sequence, row_count, min_time, max_time),
            None => {
                self.open = Some(Window::new(
                    received_at,
                    sequence,
                    row_count,
                    min_time,
                    max_time,
                ))
            }
        };
    }

    /// Returns the number of rows that are persistable. These rows could be duplicates and there
    /// are other rows that may fall in closed and open that would be pulled into a persistence
    /// operation. This number is used to determine if persistence should be triggered, not as
    /// an exact number.
    pub fn persistable_row_count(&self) -> usize {
        self.persistable.as_ref().map(|w| w.row_count).unwrap_or(0)
    }

    /// Returns the instant of the oldest persistable data. This is used by the lifecycle manager
    /// to determine if persistence should be triggered because data has been sitting in memory
    /// too long. This limit should only be hit if the throughput in the partition hasn't crossed
    /// the row count threshold or if the partition has gone cold for writes.
    pub fn persistable_age(&self) -> Option<Instant> {
        self.persistable.as_ref().map(|w| w.created_at)
    }

    /// Returns the max timestamp of data in the persistable window. Any unpersisted data with a
    /// timestamp <= than this value can be persisted.
    pub fn max_persistable_timestamp(&self) -> Option<DateTime<Utc>> {
        self.persistable.as_ref().map(|w| w.max_time)
    }

    /// rotates open window to closed if past time and any closed windows to persistable. The lifecycle manager
    /// should clone all persistence windows and call this method before checking on persistable_age
    /// to see if the time threshold has been crossed.
    pub fn rotate(&mut self, now: Instant) {
        let rotate = self
            .open
            .as_ref()
            .map(|w| now.duration_since(w.created_at) >= CLOSED_WINDOW_PERIOD)
            .unwrap_or(false);

        if rotate {
            self.closed.push_back(self.open.take().unwrap())
        }

        while let Some(w) = self.closed.pop_front() {
            if now.duration_since(w.created_at) >= self.late_arrival_period {
                match self.persistable.as_mut() {
                    Some(persistable_window) => persistable_window.add_window(w),
                    None => self.persistable = Some(w),
                }
            } else {
                self.closed.push_front(w);
                break;
            }
        }
    }

    /// Clears out the persistable window and sets the min time of any closed and open windows
    /// to the greater of either their min time or the end time of the persistable window (known
    /// as the max_persistable_timestamp value).
    pub fn flush(&mut self) {
        if let Some(t) = self.max_persistable_timestamp() {
            for w in &mut self.closed {
                if w.min_time < t {
                    w.min_time = t;
                }
            }

            if let Some(w) = self.open.as_mut() {
                if w.min_time < t {
                    w.min_time = t;
                }
            };
        }

        self.persistable = None;
    }

    /// Returns the unpersisted sequencer numbers that represent the min
    pub fn minimum_unpersisted_sequence(&self) -> Option<BTreeMap<u32, MinMaxSequence>> {
        if let Some(w) = self.persistable.as_ref() {
            return Some(w.sequencer_numbers.clone());
        }

        if let Some(w) = self.closed.get(0) {
            return Some(w.sequencer_numbers.clone());
        }

        if let Some(w) = self.open.as_ref() {
            return Some(w.sequencer_numbers.clone());
        }

        None
    }
}

#[derive(Debug, Clone)]
struct Window {
    /// The server time when this window was created. Used to determine how long data in this
    /// window has been sitting in memory.
    created_at: Instant,
    row_count: usize,
    min_time: DateTime<Utc>, // min time value for data in the window
    max_time: DateTime<Utc>, // max time value for data in the window
    /// maps sequencer_id to the minimum and maximum sequence numbers seen
    sequencer_numbers: BTreeMap<u32, MinMaxSequence>,
}

/// The minimum and maximum sequence numbers seen for a given sequencer
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct MinMaxSequence {
    pub min: u64,
    pub max: u64,
}

impl Window {
    fn new(
        created_at: Instant,
        sequence: &Sequence,
        row_count: usize,
        min_time: DateTime<Utc>,
        max_time: DateTime<Utc>,
    ) -> Self {
        let mut sequencer_numbers = BTreeMap::new();
        sequencer_numbers.insert(
            sequence.id,
            MinMaxSequence {
                min: sequence.number,
                max: sequence.number,
            },
        );

        Self {
            created_at,
            row_count,
            min_time,
            max_time,
            sequencer_numbers,
        }
    }

    /// Updates the window with the passed in range. This function assumes that sequence numbers
    /// are always increasing.
    fn add_range(
        &mut self,
        sequence: &Sequence,
        row_count: usize,
        min_time: DateTime<Utc>,
        max_time: DateTime<Utc>,
    ) {
        self.row_count += row_count;
        if self.min_time > min_time {
            self.min_time = min_time;
        }
        if self.max_time < max_time {
            self.max_time = max_time;
        }
        match self.sequencer_numbers.get_mut(&sequence.id) {
            Some(n) => {
                assert!(sequence.number > n.max);
                n.max = sequence.number;
            }
            None => {
                self.sequencer_numbers.insert(
                    sequence.id,
                    MinMaxSequence {
                        min: sequence.number,
                        max: sequence.number,
                    },
                );
            }
        }
    }

    /// Add one window to another. Used to collapse closed windows into persisted.
    fn add_window(&mut self, other: Self) {
        self.row_count += other.row_count;
        if self.min_time > other.min_time {
            self.min_time = other.min_time;
        }
        if self.max_time < other.max_time {
            self.max_time = other.max_time;
        }
        for (sequencer_id, other_n) in other.sequencer_numbers {
            match self.sequencer_numbers.get_mut(&sequencer_id) {
                Some(n) => {
                    assert!(other_n.max > n.max);
                    n.max = other_n.max;
                }
                None => {
                    self.sequencer_numbers.insert(sequencer_id, other_n);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_open_window() {
        let mut w = PersistenceWindows::new(Duration::from_secs(60)).unwrap();

        let i = Instant::now();
        let start_time = Utc::now();

        w.add_range(&Sequence { id: 1, number: 2 }, 1, start_time, Utc::now(), i);
        w.add_range(
            &Sequence { id: 1, number: 4 },
            2,
            Utc::now(),
            Utc::now(),
            Instant::now(),
        );
        w.add_range(
            &Sequence { id: 1, number: 10 },
            1,
            Utc::now(),
            Utc::now(),
            Instant::now(),
        );
        let last_time = Utc::now();
        w.add_range(
            &Sequence { id: 2, number: 23 },
            10,
            Utc::now(),
            last_time,
            Instant::now(),
        );

        assert_eq!(w.persistable_row_count(), 0);
        assert!(w.closed.is_empty());
        assert!(w.persistable.is_none());
        let open = w.open.unwrap();

        assert_eq!(open.min_time, start_time);
        assert_eq!(open.max_time, last_time);
        assert_eq!(open.row_count, 14);
        assert_eq!(
            open.sequencer_numbers.get(&1).unwrap(),
            &MinMaxSequence { min: 2, max: 10 }
        );
        assert_eq!(
            open.sequencer_numbers.get(&2).unwrap(),
            &MinMaxSequence { min: 23, max: 23 }
        );
    }

    #[test]
    fn closes_open_window() {
        let mut w = PersistenceWindows::new(Duration::from_secs(60)).unwrap();
        let created_at = Instant::now();
        let start_time = Utc::now();
        let last_time = Utc::now();

        w.add_range(
            &Sequence { id: 1, number: 2 },
            1,
            start_time,
            start_time,
            created_at,
        );
        w.add_range(
            &Sequence { id: 1, number: 3 },
            1,
            last_time,
            last_time,
            Instant::now(),
        );
        let after_close_threshold = created_at.checked_add(CLOSED_WINDOW_PERIOD).unwrap();
        let open_time = Utc::now();
        w.add_range(
            &Sequence { id: 1, number: 6 },
            2,
            last_time,
            open_time,
            after_close_threshold,
        );

        assert_eq!(w.persistable_row_count(), 0);

        let closed = w.closed.get(0).unwrap();
        assert_eq!(
            closed.sequencer_numbers.get(&1).unwrap(),
            &MinMaxSequence { min: 2, max: 3 }
        );
        assert_eq!(closed.row_count, 2);
        assert_eq!(closed.min_time, start_time);
        assert_eq!(closed.max_time, last_time);

        let open = w.open.unwrap();
        assert_eq!(open.row_count, 2);
        assert_eq!(open.min_time, last_time);
        assert_eq!(open.max_time, open_time);
        assert_eq!(
            open.sequencer_numbers.get(&1).unwrap(),
            &MinMaxSequence { min: 6, max: 6 }
        )
    }

    #[test]
    fn moves_to_persistable() {
        let mut w = PersistenceWindows::new(Duration::from_secs(120)).unwrap();
        let created_at = Instant::now();
        let start_time = Utc::now();

        let first_end = Utc::now();
        w.add_range(
            &Sequence { id: 1, number: 2 },
            2,
            start_time,
            first_end,
            created_at,
        );

        let second_created_at = created_at.checked_add(CLOSED_WINDOW_PERIOD).unwrap();
        let second_end = Utc::now();
        w.add_range(
            &Sequence { id: 1, number: 3 },
            3,
            first_end,
            second_end,
            second_created_at,
        );

        let third_created_at = second_created_at.checked_add(CLOSED_WINDOW_PERIOD).unwrap();
        let third_end = Utc::now();
        w.add_range(
            &Sequence { id: 1, number: 4 },
            4,
            second_end,
            third_end,
            third_created_at,
        );

        assert_eq!(w.persistable_row_count(), 0);
        // confirm the two on closed and third on open
        let c = w.closed.get(0).cloned().unwrap();
        assert_eq!(c.created_at, created_at);
        assert_eq!(c.row_count, 2);
        assert_eq!(c.min_time, start_time);
        assert_eq!(c.max_time, first_end);

        let c = w.closed.get(1).cloned().unwrap();
        assert_eq!(c.created_at, second_created_at);
        assert_eq!(c.row_count, 3);
        assert_eq!(c.min_time, first_end);
        assert_eq!(c.max_time, second_end);

        let c = w.open.clone().unwrap();
        assert_eq!(c.created_at, third_created_at);
        assert_eq!(c.row_count, 4);
        assert_eq!(c.min_time, second_end);
        assert_eq!(c.max_time, third_end);

        let fourth_created_at = third_created_at
            .checked_add(CLOSED_WINDOW_PERIOD * 3)
            .unwrap();
        let fourth_end = Utc::now();
        w.add_range(
            &Sequence { id: 1, number: 5 },
            1,
            fourth_end,
            fourth_end,
            fourth_created_at,
        );

        assert_eq!(w.persistable_row_count(), 5);
        assert_eq!(w.persistable_age(), Some(created_at));

        // confirm persistable has first and second
        let c = w.persistable.as_ref().unwrap();
        assert_eq!(c.created_at, created_at);
        assert_eq!(c.row_count, 5);
        assert_eq!(c.min_time, start_time);
        assert_eq!(c.max_time, second_end);

        // and the third window moved to closed
        let c = w.closed.get(0).cloned().unwrap();
        assert_eq!(c.created_at, third_created_at);
        assert_eq!(c.row_count, 4);
        assert_eq!(c.min_time, second_end);
        assert_eq!(c.max_time, third_end);

        let fifth_created_at = fourth_created_at
            .checked_add(CLOSED_WINDOW_PERIOD * 100)
            .unwrap();
        w.add_range(
            &Sequence { id: 1, number: 9 },
            2,
            Utc::now(),
            Utc::now(),
            fifth_created_at,
        );

        assert_eq!(w.persistable_row_count(), 10);
        assert_eq!(w.persistable_age(), Some(created_at));

        let c = w.persistable.as_ref().unwrap();
        assert_eq!(c.created_at, created_at);
        assert_eq!(c.row_count, 10);
        assert_eq!(c.min_time, start_time);
        assert_eq!(c.max_time, fourth_end);
    }

    #[test]
    fn flush_persistable_keeps_open_and_closed() {
        let mut w = PersistenceWindows::new(Duration::from_secs(120)).unwrap();

        // these instants represent when the server received the data. Here we have a window that
        // should be in the persistable group, a closed window, and an open window that is closed on flush.
        let created_at = Instant::now();
        let second_created_at = created_at.checked_add(CLOSED_WINDOW_PERIOD * 2).unwrap();
        let third_created_at = second_created_at.checked_add(CLOSED_WINDOW_PERIOD).unwrap();
        let end_at = third_created_at.checked_add(CLOSED_WINDOW_PERIOD).unwrap();

        // these times represent the value of the time column for the rows of data. Here we have
        // non-overlapping windows.
        let start_time = Utc::now();
        let first_end = start_time + chrono::Duration::seconds(1);
        let second_start = first_end + chrono::Duration::seconds(1);
        let second_end = second_start + chrono::Duration::seconds(1);
        let third_start = second_end + chrono::Duration::seconds(1);
        let third_end = third_start + chrono::Duration::seconds(1);

        w.add_range(
            &Sequence { id: 1, number: 2 },
            2,
            start_time,
            first_end,
            created_at,
        );
        w.add_range(
            &Sequence { id: 1, number: 3 },
            3,
            second_start,
            second_end,
            second_created_at,
        );
        w.add_range(
            &Sequence { id: 1, number: 5 },
            2,
            third_start,
            third_end,
            third_created_at,
        );

        w.rotate(end_at);

        let max_time = w.max_persistable_timestamp().unwrap();
        assert_eq!(max_time, first_end);
        let c = w.persistable.as_ref().unwrap();
        assert_eq!(c.created_at, created_at);
        assert_eq!(c.row_count, 2);
        assert_eq!(c.min_time, start_time);
        assert_eq!(c.max_time, first_end);

        let mins = w.persistable.as_ref().unwrap().sequencer_numbers.clone();
        assert_eq!(mins, w.minimum_unpersisted_sequence().unwrap());
        w.flush();
        assert!(w.persistable.is_none());
        let mins = w.closed[0].sequencer_numbers.clone();
        assert_eq!(mins, w.minimum_unpersisted_sequence().unwrap());

        let c = &w.closed[0];
        assert_eq!(c.row_count, 3);
        assert_eq!(c.min_time, second_start);
        assert_eq!(c.max_time, second_end);
        assert_eq!(c.created_at, second_created_at);

        let c = &w.closed[1];
        assert_eq!(c.row_count, 2);
        assert_eq!(c.min_time, third_start);
        assert_eq!(c.max_time, third_end);
        assert_eq!(c.created_at, third_created_at);
    }

    #[test]
    fn flush_persistable_overlaps_closed() {
        let mut w = PersistenceWindows::new(Duration::from_secs(120)).unwrap();

        // these instants represent when data is received by the server. Here we have a persistable
        // window followed by two closed windows.
        let created_at = Instant::now();
        let second_created_at = created_at.checked_add(CLOSED_WINDOW_PERIOD * 2).unwrap();
        let third_created_at = second_created_at.checked_add(CLOSED_WINDOW_PERIOD).unwrap();
        let end_at = third_created_at.checked_add(CLOSED_WINDOW_PERIOD).unwrap();

        // the times of the rows of data. this will create overlapping windows where persistable
        // overlaps with the oldest closed window.
        let start_time = Utc::now();
        let second_start = start_time + chrono::Duration::seconds(1);
        let first_end = second_start + chrono::Duration::seconds(1);
        let second_end = first_end + chrono::Duration::seconds(1);
        let third_start = first_end + chrono::Duration::seconds(1);
        let third_end = third_start + chrono::Duration::seconds(1);

        w.add_range(
            &Sequence { id: 1, number: 2 },
            2,
            start_time,
            first_end,
            created_at,
        );
        w.add_range(
            &Sequence { id: 1, number: 3 },
            3,
            second_start,
            second_end,
            second_created_at,
        );
        w.add_range(
            &Sequence { id: 1, number: 5 },
            2,
            third_start,
            third_end,
            third_created_at,
        );

        w.rotate(end_at);

        let max_time = w.max_persistable_timestamp().unwrap();
        assert_eq!(max_time, first_end);
        let c = w.persistable.as_ref().unwrap();
        assert_eq!(c.created_at, created_at);
        assert_eq!(c.row_count, 2);
        assert_eq!(c.min_time, start_time);
        assert_eq!(c.max_time, first_end);

        let mins = w.persistable.as_ref().unwrap().sequencer_numbers.clone();
        assert_eq!(mins, w.minimum_unpersisted_sequence().unwrap());
        w.flush();
        assert!(w.persistable.is_none());
        let mins = w.closed[0].sequencer_numbers.clone();
        assert_eq!(mins, w.minimum_unpersisted_sequence().unwrap());

        // the first closed window should have a min time equal to the flush
        let c = &w.closed[0];
        assert_eq!(c.row_count, 3);
        assert_eq!(c.min_time, max_time);
        assert_eq!(c.max_time, second_end);
        assert_eq!(c.created_at, second_created_at);

        let c = &w.closed[1];
        assert_eq!(c.row_count, 2);
        assert_eq!(c.min_time, third_start);
        assert_eq!(c.max_time, third_end);
        assert_eq!(c.created_at, third_created_at);
    }

    #[test]
    fn flush_persistable_overlaps_open() {
        let mut w = PersistenceWindows::new(Duration::from_secs(120)).unwrap();

        // these instants represent when data is received by the server. Here we have a persistable
        // window followed by two closed windows.
        let created_at = Instant::now();
        let second_created_at = created_at.checked_add(CLOSED_WINDOW_PERIOD * 3).unwrap();
        let third_created_at = second_created_at.checked_add(CLOSED_WINDOW_PERIOD).unwrap();
        let end_at = third_created_at.checked_add(Duration::new(1, 0)).unwrap();

        // the times of the rows of data. this will create overlapping windows where persistable
        // overlaps with the newest open window (but not the closed one).
        let start_time = Utc::now();
        let third_start = start_time + chrono::Duration::seconds(1);
        let first_end = third_start + chrono::Duration::seconds(1);
        let second_end = first_end + chrono::Duration::seconds(1);
        let third_end = second_end + chrono::Duration::seconds(1);

        w.add_range(
            &Sequence { id: 1, number: 2 },
            2,
            start_time,
            first_end,
            created_at,
        );
        w.add_range(
            &Sequence { id: 1, number: 3 },
            3,
            first_end,
            second_end,
            second_created_at,
        );
        w.add_range(
            &Sequence { id: 1, number: 5 },
            2,
            third_start,
            third_end,
            third_created_at,
        );

        w.rotate(end_at);

        let max_time = w.max_persistable_timestamp().unwrap();
        assert_eq!(max_time, first_end);
        let c = w.persistable.as_ref().unwrap();
        assert_eq!(c.created_at, created_at);
        assert_eq!(c.row_count, 2);
        assert_eq!(c.min_time, start_time);
        assert_eq!(c.max_time, first_end);

        let mins = w.persistable.as_ref().unwrap().sequencer_numbers.clone();
        assert_eq!(mins, w.minimum_unpersisted_sequence().unwrap());
        w.flush();
        assert!(w.persistable.is_none());
        let mins = w.closed[0].sequencer_numbers.clone();
        assert_eq!(mins, w.minimum_unpersisted_sequence().unwrap());

        // the closed window should have a min time equal to the flush
        let c = &w.closed[0];
        assert_eq!(c.row_count, 3);
        assert_eq!(c.min_time, first_end);
        assert_eq!(c.max_time, second_end);
        assert_eq!(c.created_at, second_created_at);

        // the open window should have a min time equal to max_time
        let c = w.open.as_ref().unwrap();
        assert_eq!(c.row_count, 2);
        assert_eq!(c.min_time, max_time);
        assert_eq!(c.max_time, third_end);
        assert_eq!(c.created_at, third_created_at);
    }

    #[test]
    fn flush_persistable_overlaps_open_and_closed() {
        let mut w = PersistenceWindows::new(Duration::from_secs(120)).unwrap();

        // these instants represent when data is received by the server. Here we have a persistable
        // window followed by two closed windows.
        let created_at = Instant::now();
        let second_created_at = created_at.checked_add(CLOSED_WINDOW_PERIOD * 3).unwrap();
        let third_created_at = second_created_at.checked_add(CLOSED_WINDOW_PERIOD).unwrap();
        let end_at = third_created_at.checked_add(Duration::new(1, 0)).unwrap();

        // the times of the rows of data. this will create overlapping windows where persistable
        // overlaps with the closed window and the open one.
        let start_time = Utc::now();
        let second_start = start_time + chrono::Duration::seconds(1);
        let third_start = second_start + chrono::Duration::seconds(1);
        let first_end = third_start + chrono::Duration::seconds(1);
        let second_end = first_end + chrono::Duration::seconds(1);
        let third_end = second_end + chrono::Duration::seconds(1);

        w.add_range(
            &Sequence { id: 1, number: 2 },
            2,
            start_time,
            first_end,
            created_at,
        );
        w.add_range(
            &Sequence { id: 1, number: 3 },
            3,
            second_start,
            second_end,
            second_created_at,
        );
        w.add_range(
            &Sequence { id: 1, number: 5 },
            2,
            third_start,
            third_end,
            third_created_at,
        );

        // this should rotate the first window into persistable
        w.rotate(end_at);

        let max_time = w.max_persistable_timestamp().unwrap();
        assert_eq!(max_time, first_end);
        let c = w.persistable.as_ref().unwrap();
        assert_eq!(c.created_at, created_at);
        assert_eq!(c.row_count, 2);
        assert_eq!(c.min_time, start_time);
        assert_eq!(c.max_time, first_end);

        let mins = w.persistable.as_ref().unwrap().sequencer_numbers.clone();
        assert_eq!(mins, w.minimum_unpersisted_sequence().unwrap());

        // after flush we should see no more persistable window and the closed and open windows
        // should have min timestamps equal to the previous flush end.
        w.flush();
        assert!(w.persistable.is_none());
        let mins = w.closed[0].sequencer_numbers.clone();
        assert_eq!(mins, w.minimum_unpersisted_sequence().unwrap());

        // the closed window should have a min time equal to the flush
        let c = &w.closed[0];
        assert_eq!(c.row_count, 3);
        assert_eq!(c.min_time, max_time);
        assert_eq!(c.max_time, second_end);
        assert_eq!(c.created_at, second_created_at);

        // the open window should have a min time equal to max_time
        let c = w.open.as_ref().unwrap();
        assert_eq!(c.row_count, 2);
        assert_eq!(c.min_time, max_time);
        assert_eq!(c.max_time, third_end);
        assert_eq!(c.created_at, third_created_at);
    }
}
