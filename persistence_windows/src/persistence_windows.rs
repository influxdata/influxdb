//!  In memory structures for tracking data ingest and when persistence can or should occur.
use std::{
    collections::{btree_map::Entry, BTreeMap, VecDeque},
    time::{Duration, Instant},
};

use chrono::{DateTime, TimeZone, Utc};

use entry::Sequence;
use internal_types::guard::{ReadGuard, ReadLock};

use crate::min_max_sequence::MinMaxSequence;

const DEFAULT_CLOSED_WINDOW_PERIOD: Duration = Duration::from_secs(30);

/// PersistenceWindows keep track of ingested data within a partition to determine when it
/// can be persisted. This allows IOx to receive out of order writes (in their timestamps) while
/// persisting mostly in non-time overlapping Parquet files.
///
/// The sequencer_id in the code below will map to a Kafka partition id. The sequence_number maps
/// to a Kafka offset. Because IOx will run without Kafka, we use the more generic terms rather
/// than the Kafka terminology.
///
/// The `PersistenceWindows` operate on two different types of time
///
/// * row timestamps - these are `DateTime<Utc>` and are the row's value for the `time` column
/// * Wall timestamps - these are `Instant` and are the Wall clock of the system used to determine
///   the "age" of a set of writes within a PersistenceWindow
///
/// To aid testing Wall timestamps are passed to many methods instead of directly using `Instant::now`
///
/// The PersistenceWindows answer the question: - "What is the maximum row timestamp in the writes
/// that arrived more than late_arrival_period seconds ago, as determined by wall clock time"
#[derive(Debug)]
pub struct PersistenceWindows {
    persistable: ReadLock<Option<Window>>,
    closed: VecDeque<Window>,
    open: Option<Window>,
    late_arrival_period: Duration,
    closed_window_period: Duration,

    /// The last instant passed to PersistenceWindows::add_range
    last_instant: Instant,

    /// maps sequencer_id to the maximum sequence passed to PersistenceWindows::add_range
    sequencer_numbers: BTreeMap<u32, u64>,
}

/// A handle for flushing data from the `PersistenceWindows`
///
/// When a `FlushHandle` is created it computes the row timestamp that should be persisted up to
///
/// It then allows flushing the corresponding writes from the `PersistenceWindows` that were
/// present at the time the `FlushHandle` was created. Even if later writes have been recorded
/// in the `PersistenceWindows` in the intervening time
///
#[derive(Debug)]
pub struct FlushHandle {
    guard: ReadGuard<Option<Window>>,
    /// The number of closed windows at the time of the handle's creation
    ///
    /// This identifies the windows that can have their
    /// minimum timestamps truncated on flush
    closed_count: usize,

    /// The timestamp to flush
    timestamp: DateTime<Utc>,
}

impl FlushHandle {
    /// Should flush all rows with a timestamp less than or equal to this
    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }
}

impl PersistenceWindows {
    pub fn new(late_arrival_period: Duration) -> Self {
        let closed_window_period = late_arrival_period.min(DEFAULT_CLOSED_WINDOW_PERIOD);

        let late_arrival_seconds = late_arrival_period.as_secs();
        let closed_window_seconds = closed_window_period.as_secs();

        let closed_window_count = late_arrival_seconds / closed_window_seconds;

        Self {
            persistable: ReadLock::new(None),
            closed: VecDeque::with_capacity(closed_window_count as usize),
            open: None,
            late_arrival_period,
            closed_window_period,
            last_instant: Instant::now(),
            sequencer_numbers: Default::default(),
        }
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
        sequence: Option<&Sequence>,
        row_count: usize,
        min_time: DateTime<Utc>,
        max_time: DateTime<Utc>,
        received_at: Instant,
    ) {
        assert!(
            received_at >= self.last_instant,
            "PersistenceWindows::add_range called out of order"
        );
        self.last_instant = received_at;

        if let Some(sequence) = sequence {
            match self.sequencer_numbers.entry(sequence.id) {
                Entry::Occupied(mut occupied) => {
                    assert!(
                        *occupied.get() < sequence.number,
                        "sequence number {} for sequencer {} was not greater than previous {}",
                        sequence.number,
                        sequence.id,
                        *occupied.get()
                    );
                    *occupied.get_mut() = sequence.number;
                }
                Entry::Vacant(vacant) => {
                    vacant.insert(sequence.number);
                }
            }
        }

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

    /// rotates open window to closed if past time and any closed windows to persistable.
    ///
    /// `now` is the Wall clock time of the server to use for determining how "old" a given
    /// persistence window is, or in other words, how long since the writes it contains the
    /// metrics for were written to this partition
    fn rotate(&mut self, now: Instant) {
        let rotate = self
            .open
            .as_ref()
            .map(|w| w.is_closeable(now, self.closed_window_period))
            .unwrap_or(false);

        if rotate {
            self.closed.push_back(self.open.take().unwrap())
        }

        let late_arrival_period = self.late_arrival_period;

        // if there is no ongoing persistence operation, try and
        // add closed windows to the `persistable` window
        if let Some(persistable) = self.persistable.get_mut() {
            while self
                .closed
                .front()
                .map(|w| w.is_persistable(now, late_arrival_period))
                .unwrap_or(false)
            {
                let w = self.closed.pop_front().unwrap();
                match persistable.as_mut() {
                    Some(persistable_window) => persistable_window.add_window(w),
                    None => *persistable = Some(w),
                }
            }
        }
    }

    /// Acquire a handle that prevents mutation of the persistable window until dropped
    ///
    /// Returns `None` if there is an outstanding handle
    pub fn flush_handle(&mut self, now: Instant) -> Option<FlushHandle> {
        // Verify no active flush handles
        self.persistable.get_mut()?;

        // Rotate into persistable window
        self.rotate(now);

        Some(FlushHandle {
            guard: self.persistable.lock(),
            closed_count: self.closed.len(),
            timestamp: self.persistable.as_ref()?.max_time,
        })
    }

    /// Clears out the persistable window
    pub fn flush(&mut self, handle: FlushHandle) {
        let closed_count = handle.closed_count;
        let timestamp = handle.timestamp;
        std::mem::drop(handle);

        assert!(
            self.closed.len() >= closed_count,
            "windows dropped from closed whilst locked"
        );

        let persistable = self
            .persistable
            .get_mut()
            .expect("expected no active locks")
            .take()
            .expect("expected persistable window");

        assert_eq!(
            persistable.max_time, timestamp,
            "persistable max time doesn't match handle"
        );

        // Everything up to and including persistable max time will have been persisted
        let new_min = Utc.timestamp_nanos(persistable.max_time.timestamp_nanos() + 1);
        for w in self.closed.iter_mut().take(closed_count) {
            if w.min_time < new_min {
                w.min_time = new_min;
                if w.max_time < new_min {
                    w.max_time = new_min;
                    w.row_count = 0;
                }
            }
        }
    }

    /// Returns an iterator over the windows starting with the oldest
    fn windows(&self) -> impl Iterator<Item = &Window> {
        self.persistable
            .as_ref()
            .into_iter()
            .chain(self.closed.iter())
            .chain(self.open.as_ref().into_iter())
    }

    /// Returns the minimum window
    fn minimum_window(&self) -> Option<&Window> {
        self.windows().next()
    }

    /// Returns the unpersisted sequencer numbers that represent the min
    pub fn minimum_unpersisted_sequence(&self) -> Option<BTreeMap<u32, MinMaxSequence>> {
        self.minimum_window().map(|x| x.sequencer_numbers.clone())
    }

    /// Returns the minimum unpersisted age
    pub fn minimum_unpersisted_age(&self) -> Option<Instant> {
        self.minimum_window().map(|x| x.created_at)
    }

    /// Returns the minimum unpersisted timestamp
    pub fn minimum_unpersisted_timestamp(&self) -> Option<DateTime<Utc>> {
        self.windows().map(|x| x.min_time).min()
    }

    /// Returns the maximum unpersisted timestamp
    pub fn maximum_unpersisted_timestamp(&self) -> Option<DateTime<Utc>> {
        self.windows().map(|x| x.max_time).max()
    }

    /// Returns the number of persistable rows
    pub fn persistable_row_count(&self, now: Instant) -> usize {
        self.windows()
            .take_while(|window| window.is_persistable(now, self.late_arrival_period))
            .map(|window| window.row_count)
            .sum()
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

impl Window {
    fn new(
        created_at: Instant,
        sequence: Option<&Sequence>,
        row_count: usize,
        min_time: DateTime<Utc>,
        max_time: DateTime<Utc>,
    ) -> Self {
        let mut sequencer_numbers = BTreeMap::new();
        if let Some(sequence) = sequence {
            sequencer_numbers.insert(
                sequence.id,
                MinMaxSequence::new(sequence.number, sequence.number),
            );
        }

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
        sequence: Option<&Sequence>,
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
        if let Some(sequence) = sequence {
            match self.sequencer_numbers.get_mut(&sequence.id) {
                Some(n) => {
                    assert!(sequence.number > n.max());
                    *n = MinMaxSequence::new(n.min(), sequence.number);
                }
                None => {
                    self.sequencer_numbers.insert(
                        sequence.id,
                        MinMaxSequence::new(sequence.number, sequence.number),
                    );
                }
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
                    assert!(other_n.max() > n.max());
                    *n = MinMaxSequence::new(n.min(), other_n.max());
                }
                None => {
                    self.sequencer_numbers.insert(sequencer_id, other_n);
                }
            }
        }
    }

    /// If this window can be closed
    fn is_closeable(&self, now: Instant, closed_window_period: Duration) -> bool {
        now.checked_duration_since(self.created_at)
            .map(|x| x >= closed_window_period)
            .unwrap_or(false)
    }

    /// If this window is persistable
    fn is_persistable(&self, now: Instant, late_arrival_period: Duration) -> bool {
        now.checked_duration_since(self.created_at)
            .map(|x| x >= late_arrival_period)
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_open_window() {
        let mut w = PersistenceWindows::new(Duration::from_secs(60));

        let i = Instant::now();
        let start_time = Utc::now();

        w.add_range(
            Some(&Sequence { id: 1, number: 2 }),
            1,
            start_time,
            Utc::now(),
            i,
        );
        w.add_range(
            Some(&Sequence { id: 1, number: 4 }),
            2,
            Utc::now(),
            Utc::now(),
            Instant::now(),
        );
        w.add_range(
            Some(&Sequence { id: 1, number: 10 }),
            1,
            Utc::now(),
            Utc::now(),
            Instant::now(),
        );
        let last_time = Utc::now();
        w.add_range(
            Some(&Sequence { id: 2, number: 23 }),
            10,
            Utc::now(),
            last_time,
            Instant::now(),
        );

        assert!(w.closed.is_empty());
        assert!(w.persistable.is_none());
        let open = w.open.unwrap();

        assert_eq!(open.min_time, start_time);
        assert_eq!(open.max_time, last_time);
        assert_eq!(open.row_count, 14);
        assert_eq!(
            open.sequencer_numbers.get(&1).unwrap(),
            &MinMaxSequence::new(2, 10)
        );
        assert_eq!(
            open.sequencer_numbers.get(&2).unwrap(),
            &MinMaxSequence::new(23, 23)
        );
    }

    #[test]
    fn closes_open_window() {
        let mut w = PersistenceWindows::new(Duration::from_secs(60));
        let created_at = Instant::now();
        let start_time = Utc::now();
        let last_time = Utc::now();

        w.add_range(
            Some(&Sequence { id: 1, number: 2 }),
            1,
            start_time,
            start_time,
            created_at,
        );
        w.add_range(
            Some(&Sequence { id: 1, number: 3 }),
            1,
            last_time,
            last_time,
            Instant::now(),
        );
        let after_close_threshold = created_at
            .checked_add(DEFAULT_CLOSED_WINDOW_PERIOD)
            .unwrap();
        let open_time = Utc::now();
        w.add_range(
            Some(&Sequence { id: 1, number: 6 }),
            2,
            last_time,
            open_time,
            after_close_threshold,
        );

        assert!(w.persistable.is_none());

        let closed = w.closed.get(0).unwrap();
        assert_eq!(
            closed.sequencer_numbers.get(&1).unwrap(),
            &MinMaxSequence::new(2, 3)
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
            &MinMaxSequence::new(6, 6)
        )
    }

    #[test]
    fn moves_to_persistable() {
        let mut w = PersistenceWindows::new(Duration::from_secs(120));
        let created_at = Instant::now();
        let start_time = Utc::now();

        let first_end = Utc::now();
        w.add_range(
            Some(&Sequence { id: 1, number: 2 }),
            2,
            start_time,
            first_end,
            created_at,
        );

        let second_created_at = created_at
            .checked_add(DEFAULT_CLOSED_WINDOW_PERIOD)
            .unwrap();
        let second_end = Utc::now();
        w.add_range(
            Some(&Sequence { id: 1, number: 3 }),
            3,
            first_end,
            second_end,
            second_created_at,
        );

        let third_created_at = second_created_at
            .checked_add(DEFAULT_CLOSED_WINDOW_PERIOD)
            .unwrap();
        let third_end = Utc::now();
        w.add_range(
            Some(&Sequence { id: 1, number: 4 }),
            4,
            second_end,
            third_end,
            third_created_at,
        );

        assert!(w.persistable.is_none());
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
            .checked_add(DEFAULT_CLOSED_WINDOW_PERIOD * 3)
            .unwrap();
        let fourth_end = Utc::now();
        w.add_range(
            Some(&Sequence { id: 1, number: 5 }),
            1,
            fourth_end,
            fourth_end,
            fourth_created_at,
        );

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
            .checked_add(DEFAULT_CLOSED_WINDOW_PERIOD * 100)
            .unwrap();
        w.add_range(
            Some(&Sequence { id: 1, number: 9 }),
            2,
            Utc::now(),
            Utc::now(),
            fifth_created_at,
        );

        let c = w.persistable.as_ref().unwrap();
        assert_eq!(c.created_at, created_at);
        assert_eq!(c.row_count, 10);
        assert_eq!(c.min_time, start_time);
        assert_eq!(c.max_time, fourth_end);
    }

    #[test]
    fn flush_persistable_keeps_open_and_closed() {
        let mut w = PersistenceWindows::new(Duration::from_secs(120));

        // these instants represent when the server received the data. Here we have a window that
        // should be in the persistable group, a closed window, and an open window that is closed on flush.
        let created_at = Instant::now();
        let second_created_at = created_at
            .checked_add(DEFAULT_CLOSED_WINDOW_PERIOD * 2)
            .unwrap();
        let third_created_at = second_created_at
            .checked_add(DEFAULT_CLOSED_WINDOW_PERIOD)
            .unwrap();
        let end_at = third_created_at
            .checked_add(DEFAULT_CLOSED_WINDOW_PERIOD)
            .unwrap();

        // these times represent the value of the time column for the rows of data. Here we have
        // non-overlapping windows.
        let start_time = Utc::now();
        let first_end = start_time + chrono::Duration::seconds(1);
        let second_start = first_end + chrono::Duration::seconds(1);
        let second_end = second_start + chrono::Duration::seconds(1);
        let third_start = second_end + chrono::Duration::seconds(1);
        let third_end = third_start + chrono::Duration::seconds(1);

        w.add_range(
            Some(&Sequence { id: 1, number: 2 }),
            2,
            start_time,
            first_end,
            created_at,
        );
        w.add_range(
            Some(&Sequence { id: 1, number: 3 }),
            3,
            second_start,
            second_end,
            second_created_at,
        );
        w.add_range(
            Some(&Sequence { id: 1, number: 5 }),
            2,
            third_start,
            third_end,
            third_created_at,
        );

        w.rotate(end_at);

        let c = w.persistable.as_ref().unwrap();
        assert_eq!(c.created_at, created_at);
        assert_eq!(c.row_count, 2);
        assert_eq!(c.min_time, start_time);
        assert_eq!(c.max_time, first_end);

        let mins = w.persistable.as_ref().unwrap().sequencer_numbers.clone();
        assert_eq!(mins, w.minimum_unpersisted_sequence().unwrap());

        let handle = w.flush_handle(end_at).unwrap();
        w.flush(handle);

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
        let mut w = PersistenceWindows::new(Duration::from_secs(120));

        // these instants represent when data is received by the server. Here we have a persistable
        // window followed by two closed windows.
        let created_at = Instant::now();
        let second_created_at = created_at
            .checked_add(DEFAULT_CLOSED_WINDOW_PERIOD * 2)
            .unwrap();
        let third_created_at = second_created_at
            .checked_add(DEFAULT_CLOSED_WINDOW_PERIOD)
            .unwrap();
        let end_at = third_created_at
            .checked_add(DEFAULT_CLOSED_WINDOW_PERIOD)
            .unwrap();

        // the times of the rows of data. this will create overlapping windows where persistable
        // overlaps with the oldest closed window.
        let start_time = Utc::now();
        let second_start = start_time + chrono::Duration::seconds(1);
        let first_end = second_start + chrono::Duration::seconds(1);
        let second_end = first_end + chrono::Duration::seconds(1);
        let third_start = first_end + chrono::Duration::seconds(1);
        let third_end = third_start + chrono::Duration::seconds(1);

        w.add_range(
            Some(&Sequence { id: 1, number: 2 }),
            2,
            start_time,
            first_end,
            created_at,
        );
        w.add_range(
            Some(&Sequence { id: 1, number: 3 }),
            3,
            second_start,
            second_end,
            second_created_at,
        );
        w.add_range(
            Some(&Sequence { id: 1, number: 5 }),
            2,
            third_start,
            third_end,
            third_created_at,
        );

        w.rotate(end_at);

        let c = w.persistable.as_ref().unwrap();
        assert_eq!(c.created_at, created_at);
        assert_eq!(c.row_count, 2);
        assert_eq!(c.min_time, start_time);
        assert_eq!(c.max_time, first_end);

        let mins = w.persistable.as_ref().unwrap().sequencer_numbers.clone();
        assert_eq!(mins, w.minimum_unpersisted_sequence().unwrap());
        let flush = w.flush_handle(end_at).unwrap();

        assert_eq!(flush.timestamp(), first_end);
        let truncated_time = flush.timestamp() + chrono::Duration::nanoseconds(1);

        w.flush(flush);
        assert!(w.persistable.is_none());
        let mins = w.closed[0].sequencer_numbers.clone();
        assert_eq!(mins, w.minimum_unpersisted_sequence().unwrap());

        // the first closed window should have a min time truncated by the flush
        let c = &w.closed[0];
        assert_eq!(c.row_count, 3);
        assert_eq!(c.min_time, truncated_time);
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
        let mut w = PersistenceWindows::new(Duration::from_secs(120));

        // these instants represent when data is received by the server. Here we have a persistable
        // window followed by two closed windows.
        let created_at = Instant::now();
        let second_created_at = created_at
            .checked_add(DEFAULT_CLOSED_WINDOW_PERIOD * 3)
            .unwrap();
        let third_created_at = second_created_at
            .checked_add(DEFAULT_CLOSED_WINDOW_PERIOD)
            .unwrap();
        let end_at = third_created_at.checked_add(Duration::new(1, 0)).unwrap();

        // the times of the rows of data. this will create overlapping windows where persistable
        // overlaps with the newest open window (but not the closed one).
        let start_time = Utc::now();
        let third_start = start_time + chrono::Duration::seconds(1);
        let first_end = third_start + chrono::Duration::seconds(1);
        let second_end = first_end + chrono::Duration::seconds(1);
        let third_end = second_end + chrono::Duration::seconds(1);

        w.add_range(
            Some(&Sequence { id: 1, number: 2 }),
            2,
            start_time,
            first_end,
            created_at,
        );
        w.add_range(
            Some(&Sequence { id: 1, number: 3 }),
            3,
            first_end,
            second_end,
            second_created_at,
        );
        w.add_range(
            Some(&Sequence { id: 1, number: 5 }),
            2,
            third_start,
            third_end,
            third_created_at,
        );

        w.rotate(end_at);

        let c = w.persistable.as_ref().unwrap();
        assert_eq!(c.created_at, created_at);
        assert_eq!(c.row_count, 2);
        assert_eq!(c.min_time, start_time);
        assert_eq!(c.max_time, first_end);

        let mins = w.persistable.as_ref().unwrap().sequencer_numbers.clone();
        assert_eq!(mins, w.minimum_unpersisted_sequence().unwrap());

        let flush = w.flush_handle(end_at).unwrap();
        assert_eq!(flush.timestamp(), first_end);
        let flushed_time = flush.timestamp() + chrono::Duration::nanoseconds(1);

        w.flush(flush);
        assert!(w.persistable.is_none());
        let mins = w.closed[0].sequencer_numbers.clone();
        assert_eq!(mins, w.minimum_unpersisted_sequence().unwrap());

        // the closed window should have a min time equal to the flush
        let c = &w.closed[0];
        assert_eq!(c.row_count, 3);
        assert_eq!(c.min_time, flushed_time);
        assert_eq!(c.max_time, second_end);
        assert_eq!(c.created_at, second_created_at);

        // the open window should not have been modified by the flush
        let c = w.open.as_ref().unwrap();
        assert_eq!(c.row_count, 2);
        assert_eq!(c.min_time, third_start);
        assert_eq!(c.max_time, third_end);
        assert_eq!(c.created_at, third_created_at);
    }

    #[test]
    fn flush_persistable_overlaps_open_and_closed() {
        let mut w = PersistenceWindows::new(Duration::from_secs(120));

        // these instants represent when data is received by the server. Here we have a persistable
        // window followed by two closed windows.
        let created_at = Instant::now();
        let second_created_at = created_at
            .checked_add(DEFAULT_CLOSED_WINDOW_PERIOD * 3)
            .unwrap();
        let third_created_at = second_created_at
            .checked_add(DEFAULT_CLOSED_WINDOW_PERIOD)
            .unwrap();
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
            Some(&Sequence { id: 1, number: 2 }),
            2,
            start_time,
            first_end,
            created_at,
        );
        w.add_range(
            Some(&Sequence { id: 1, number: 3 }),
            3,
            second_start,
            second_end,
            second_created_at,
        );
        w.add_range(
            Some(&Sequence { id: 1, number: 5 }),
            2,
            third_start,
            third_end,
            third_created_at,
        );

        let c = w.persistable.as_ref().unwrap();
        assert_eq!(c.created_at, created_at);
        assert_eq!(c.row_count, 2);
        assert_eq!(c.min_time, start_time);
        assert_eq!(c.max_time, first_end);

        let mins = w.persistable.as_ref().unwrap().sequencer_numbers.clone();
        assert_eq!(mins, w.minimum_unpersisted_sequence().unwrap());

        // this should rotate the first window into persistable
        // after flush we should see no more persistable window and the closed windows
        // should have min timestamps equal to the previous flush end.
        let flush = w.flush_handle(end_at).unwrap();

        assert_eq!(flush.timestamp(), first_end);
        let flushed_time = flush.timestamp() + chrono::Duration::nanoseconds(1);

        w.flush(flush);
        assert!(w.persistable.is_none());
        let mins = w.closed[0].sequencer_numbers.clone();
        assert_eq!(mins, w.minimum_unpersisted_sequence().unwrap());

        // the closed window should have a min time equal to the flush
        let c = &w.closed[0];
        assert_eq!(c.row_count, 3);
        assert_eq!(c.min_time, flushed_time);
        assert_eq!(c.max_time, second_end);
        assert_eq!(c.created_at, second_created_at);

        // the open window should not have been modified by the flush
        let c = w.open.as_ref().unwrap();
        assert_eq!(c.row_count, 2);
        assert_eq!(c.min_time, third_start);
        assert_eq!(c.max_time, third_end);
        assert_eq!(c.created_at, third_created_at);
    }

    #[test]
    fn test_flush_guard() {
        let mut w = PersistenceWindows::new(Duration::from_secs(120));

        let instant = Instant::now();
        let start = Utc::now();

        w.add_range(
            Some(&Sequence { id: 1, number: 2 }),
            2,
            start,
            start + chrono::Duration::seconds(2),
            instant,
        );

        w.rotate(instant + Duration::from_secs(120));
        assert!(w.persistable.is_some());
        assert_eq!(w.persistable.as_ref().unwrap().row_count, 2);
        assert_eq!(
            w.persistable.as_ref().unwrap().max_time,
            start + chrono::Duration::seconds(2)
        );

        w.add_range(
            Some(&Sequence { id: 1, number: 4 }),
            5,
            start,
            start + chrono::Duration::seconds(4),
            instant + Duration::from_secs(120),
        );

        // Should rotate into closed
        w.rotate(instant + Duration::from_secs(120) + DEFAULT_CLOSED_WINDOW_PERIOD);
        assert_eq!(w.closed.len(), 1);

        let guard = w
            .flush_handle(instant + Duration::from_secs(120) + DEFAULT_CLOSED_WINDOW_PERIOD)
            .unwrap();

        // Should only allow one at once
        assert!(w.flush_handle(instant).is_none());

        // This should not rotate into persistable as active flush guard
        w.rotate(instant + Duration::from_secs(240));
        assert_eq!(w.persistable.as_ref().unwrap().row_count, 2);

        let flush_t = guard.timestamp();
        assert_eq!(flush_t, start + chrono::Duration::seconds(2));

        // Flush persistable window
        w.flush(guard);
        assert!(w.persistable.is_none());

        // This should rotate into persistable
        w.rotate(instant + Duration::from_secs(240));
        assert_eq!(w.persistable.as_ref().unwrap().row_count, 5);

        // Min time should have been truncated by persist operation to be
        // 1 nanosecond more than was persisted
        let truncated_time = flush_t + chrono::Duration::nanoseconds(1);
        assert_eq!(w.persistable.as_ref().unwrap().min_time, truncated_time);

        let guard = w.flush_handle(instant + Duration::from_secs(240)).unwrap();

        w.add_range(
            Some(&Sequence { id: 1, number: 9 }),
            9,
            start,
            start + chrono::Duration::seconds(2),
            instant + Duration::from_secs(240),
        );

        // Should rotate into closed
        w.rotate(instant + Duration::from_secs(240) + DEFAULT_CLOSED_WINDOW_PERIOD);
        assert_eq!(w.closed.len(), 1);

        // This should not rotate into persistable as active flush guard
        w.rotate(instant + Duration::from_secs(360));
        assert_eq!(w.persistable.as_ref().unwrap().row_count, 5);

        std::mem::drop(guard);
        // This should rotate into persistable
        w.rotate(instant + Duration::from_secs(360));
        assert_eq!(w.persistable.as_ref().unwrap().row_count, 5 + 9);
        assert_eq!(w.persistable.as_ref().unwrap().min_time, start);
    }

    #[test]
    fn test_flush_guard_multiple_closed() {
        let mut w = PersistenceWindows::new(DEFAULT_CLOSED_WINDOW_PERIOD * 3);

        let instant = Instant::now();
        let start = Utc::now();

        w.add_range(
            Some(&Sequence { id: 1, number: 2 }),
            2,
            start,
            start + chrono::Duration::seconds(2),
            instant,
        );

        w.add_range(
            Some(&Sequence { id: 1, number: 6 }),
            5,
            start,
            start + chrono::Duration::seconds(4),
            instant + DEFAULT_CLOSED_WINDOW_PERIOD,
        );

        w.add_range(
            Some(&Sequence { id: 1, number: 9 }),
            9,
            start,
            start + chrono::Duration::seconds(2),
            instant + DEFAULT_CLOSED_WINDOW_PERIOD * 2,
        );

        w.add_range(
            Some(&Sequence { id: 1, number: 10 }),
            17,
            start,
            start + chrono::Duration::seconds(2),
            instant + DEFAULT_CLOSED_WINDOW_PERIOD * 3,
        );

        assert_eq!(w.closed.len(), 2);
        assert_eq!(w.closed[0].row_count, 5);
        assert_eq!(w.closed[1].row_count, 9);
        assert_eq!(w.open.as_ref().unwrap().row_count, 17);

        let flush = w
            .flush_handle(instant + DEFAULT_CLOSED_WINDOW_PERIOD * 3)
            .unwrap();

        let flush_t = flush.timestamp();

        assert_eq!(flush.closed_count, 2);
        assert_eq!(flush_t, start + chrono::Duration::seconds(2));
        let truncated_time = flush_t + chrono::Duration::nanoseconds(1);

        assert_eq!(w.persistable.as_ref().unwrap().row_count, 2);

        w.add_range(
            Some(&Sequence { id: 1, number: 14 }),
            11,
            start,
            start + chrono::Duration::seconds(2),
            instant + DEFAULT_CLOSED_WINDOW_PERIOD * 4,
        );

        w.rotate(instant + DEFAULT_CLOSED_WINDOW_PERIOD * 5);

        // Despite time passing persistable window shouldn't have changed due to flush guard
        assert_eq!(w.persistable.as_ref().unwrap().row_count, 2);
        assert_eq!(w.closed.len(), 4);

        w.flush(flush);

        assert!(w.persistable.is_none());
        assert_eq!(w.closed.len(), 4);

        assert_eq!(w.closed[0].min_time, truncated_time);
        assert_eq!(w.closed[0].max_time, start + chrono::Duration::seconds(4));
        assert_eq!(w.closed[0].row_count, 5);

        assert_eq!(w.closed[1].min_time, truncated_time);
        assert_eq!(w.closed[1].max_time, truncated_time);
        assert_eq!(w.closed[1].row_count, 0); // Entirely flushed window

        // Window closed after flush handle - should be left alone
        assert_eq!(w.closed[2].min_time, start);
        assert_eq!(w.closed[2].max_time, start + chrono::Duration::seconds(2));
        assert_eq!(w.closed[2].row_count, 17); // Entirely flushed window

        // Window created after flush handle - should be left alone
        assert_eq!(w.closed[3].min_time, start);
        assert_eq!(w.closed[3].max_time, start + chrono::Duration::seconds(2));
        assert_eq!(w.closed[3].row_count, 11);
    }
}
