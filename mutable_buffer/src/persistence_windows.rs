//!  In memory structures for tracking data ingest and when persistence can or should occur.
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};

const CLOSED_WINDOW_SECONDS: u8 = 30;
const SECONDS_IN_MINUTE: u8 = 60;

/// PersistenceWindows keep track of ingested data within a partition to determine when it
/// can be persisted. This allows IOx to receive out of order writes (in their timestamps) while
/// persisting mostly in non-time overlapping Parquet files.
#[derive(Debug, Clone)]
pub struct PersistenceWindows {
    persistable: Option<Window>,
    closed: Vec<Window>,
    open: Option<Window>,
    late_arrival_period: Duration,
}

impl PersistenceWindows {
    pub fn new(late_arrival_minutes: u8) -> Self {
        let late_arrival_seconds = late_arrival_minutes * SECONDS_IN_MINUTE;
        let closed_window_count = late_arrival_seconds / CLOSED_WINDOW_SECONDS;

        Self {
            persistable: None,
            closed: Vec::with_capacity(closed_window_count as usize),
            open: None,
            late_arrival_period: Duration::new(late_arrival_seconds as u64, 0),
        }
    }

    /// Updates the windows with the information from a batch of rows to the same partition.
    pub fn add_range(
        &mut self,
        sequencer_id: u32,
        sequence_number: u64,
        row_count: usize,
        min_time: DateTime<Utc>,
        max_time: DateTime<Utc>,
        received_at: Instant,
    ) {
        self.rotate(received_at);

        match self.open.as_mut() {
            Some(w) => w.add_range(sequencer_id, sequence_number, row_count, min_time, max_time),
            None => {
                self.open = Some(Window::new(
                    received_at,
                    sequencer_id,
                    sequence_number,
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
    pub fn t_flush(&self) -> Option<DateTime<Utc>> {
        self.persistable.as_ref().map(|w| w.max_time)
    }

    /// rotates open window to closed if past time and any closed windows to persistable. The lifecycle manager
    /// should clone all persistence windows and call this method before checking on persistable_age
    /// to see if the time threshold has been crossed.
    pub fn rotate(&mut self, now: Instant) {
        let rotate = self
            .open
            .as_ref()
            .map(|w| now.duration_since(w.created_at).as_secs() >= CLOSED_WINDOW_SECONDS as u64)
            .unwrap_or(false);

        if rotate {
            self.closed.push(self.open.take().unwrap())
        }

        let mut i = 0;
        while i != self.closed.len() {
            if now.duration_since(self.closed[i].created_at) >= self.late_arrival_period {
                let closed = self.closed.remove(i);
                match self.persistable.as_mut() {
                    Some(w) => w.add_window(closed),
                    None => self.persistable = Some(closed),
                }
            } else {
                i += 1;
            }
        }
    }

    /// Clears out the persistable window and sets the min time of any closed and open windows
    /// to the greater of either their min time or the end time of the persistable window (known
    /// as the t_flush value).
    pub fn flush(&mut self) {
        if let Some(t) = self.t_flush() {
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
    pub fn minimum_unpersisted_sequence(&self) -> Option<BTreeMap<u32, (u64, u64)>> {
        if let Some(w) = self.persistable.as_ref() {
            return Some(w.sequencer_numbers.clone());
        }

        if let Some(w) = self.closed.first() {
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
    created_at: Instant,
    row_count: usize,
    min_time: DateTime<Utc>,
    max_time: DateTime<Utc>,
    sequencer_numbers: BTreeMap<u32, (u64, u64)>,
}

impl Window {
    fn new(
        created_at: Instant,
        sequencer_id: u32,
        sequence_number: u64,
        row_count: usize,
        min_time: DateTime<Utc>,
        max_time: DateTime<Utc>,
    ) -> Self {
        let mut sequencer_numbers = BTreeMap::new();
        sequencer_numbers.insert(sequencer_id, (sequence_number, sequence_number));

        Self {
            created_at,
            row_count,
            min_time,
            max_time,
            sequencer_numbers,
        }
    }

    // Updates the window the the passed in range. This function assumes that sequence numbers
    // are always increasing.
    fn add_range(
        &mut self,
        sequencer_id: u32,
        sequence_number: u64,
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
        match self.sequencer_numbers.get_mut(&sequencer_id) {
            Some(n) => n.1 = sequence_number,
            None => {
                self.sequencer_numbers
                    .insert(sequencer_id, (sequence_number, sequence_number));
            }
        }
    }

    // Add one window to another. Used to collapse closed windows into persisted.
    fn add_window(&mut self, other: Self) {
        self.row_count += other.row_count;
        if self.min_time > other.min_time {
            self.min_time = other.min_time;
        }
        if self.max_time < other.max_time {
            self.max_time = other.max_time;
        }
        for (sequencer_id, (min, max)) in other.sequencer_numbers {
            match self.sequencer_numbers.get_mut(&sequencer_id) {
                Some(n) => n.1 = max,
                None => {
                    self.sequencer_numbers.insert(sequencer_id, (min, max));
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
        let mut w = PersistenceWindows::new(1);
        assert_eq!(w.closed.capacity(), 2);

        let i = Instant::now();
        let start_time = Utc::now();

        w.add_range(1, 2, 1, start_time, Utc::now(), i);
        w.add_range(1, 4, 2, Utc::now(), Utc::now(), Instant::now());
        w.add_range(1, 10, 1, Utc::now(), Utc::now(), Instant::now());
        let last_time = Utc::now();
        w.add_range(2, 23, 10, Utc::now(), last_time, Instant::now());

        assert_eq!(w.persistable_row_count(), 0);
        assert!(w.closed.is_empty());
        assert!(w.persistable.is_none());
        let open = w.open.unwrap();

        assert_eq!(open.min_time, start_time);
        assert_eq!(open.max_time, last_time);
        assert_eq!(open.row_count, 14);
        assert_eq!(open.sequencer_numbers.get(&1).unwrap(), &(2, 10));
        assert_eq!(open.sequencer_numbers.get(&2).unwrap(), &(23, 23));
    }

    #[test]
    fn closes_open_window() {
        let mut w = PersistenceWindows::new(1);
        let created_at = Instant::now();
        let start_time = Utc::now();
        let last_time = Utc::now();

        w.add_range(1, 2, 1, start_time, start_time, created_at);
        w.add_range(1, 3, 1, last_time, last_time, Instant::now());
        let after_close_threshold = created_at
            .checked_add(Duration::new(CLOSED_WINDOW_SECONDS as u64, 0))
            .unwrap();
        let open_time = Utc::now();
        w.add_range(1, 6, 2, last_time, open_time, after_close_threshold);

        assert_eq!(w.persistable_row_count(), 0);

        let closed = w.closed.first().unwrap();
        assert_eq!(closed.sequencer_numbers.get(&1).unwrap(), &(2, 3));
        assert_eq!(closed.row_count, 2);
        assert_eq!(closed.min_time, start_time);
        assert_eq!(closed.max_time, last_time);

        let open = w.open.unwrap();
        assert_eq!(open.row_count, 2);
        assert_eq!(open.min_time, last_time);
        assert_eq!(open.max_time, open_time);
        assert_eq!(open.sequencer_numbers.get(&1).unwrap(), &(6, 6))
    }

    #[test]
    fn moves_to_persistable() {
        let mut w = PersistenceWindows::new(2);
        let created_at = Instant::now();
        let start_time = Utc::now();

        let first_end = Utc::now();
        w.add_range(1, 2, 2, start_time, first_end, created_at);

        let second_created_at = created_at
            .checked_add(Duration::new(CLOSED_WINDOW_SECONDS as u64, 0))
            .unwrap();
        let second_end = Utc::now();
        w.add_range(1, 3, 3, first_end, second_end, second_created_at);

        let third_created_at = second_created_at
            .checked_add(Duration::new(CLOSED_WINDOW_SECONDS as u64, 0))
            .unwrap();
        let third_end = Utc::now();
        w.add_range(1, 4, 4, second_end, third_end, third_created_at);

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
            .checked_add(Duration::new(CLOSED_WINDOW_SECONDS as u64 * 3, 0))
            .unwrap();
        let fourth_end = Utc::now();
        w.add_range(1, 5, 1, fourth_end, fourth_end, fourth_created_at);

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
            .checked_add(Duration::new(CLOSED_WINDOW_SECONDS as u64 * 100, 0))
            .unwrap();
        w.add_range(1, 9, 2, Utc::now(), Utc::now(), fifth_created_at);

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
        let mut w = PersistenceWindows::new(2);
        let created_at = Instant::now();
        let start_time = Utc::now();

        let first_end = Utc::now();
        w.add_range(1, 2, 2, start_time, first_end, created_at);

        let second_start = Utc::now();
        let second_created_at = created_at
            .checked_add(Duration::new(CLOSED_WINDOW_SECONDS as u64 * 2, 0))
            .unwrap();
        let second_end = Utc::now();
        w.add_range(1, 3, 3, second_start, second_end, second_created_at);

        let third_start = Utc::now();
        let third_created_at = second_created_at
            .checked_add(Duration::new(CLOSED_WINDOW_SECONDS as u64, 0))
            .unwrap();
        let third_end = Utc::now();
        w.add_range(1, 5, 2, third_start, third_end, third_created_at);

        let end_at = third_created_at
            .checked_add(Duration::new(CLOSED_WINDOW_SECONDS as u64, 0))
            .unwrap();
        w.rotate(end_at);

        let t_flush = w.t_flush().unwrap();
        assert_eq!(t_flush, first_end);
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
        let mut w = PersistenceWindows::new(2);
        let created_at = Instant::now();
        let start_time = Utc::now();
        std::thread::sleep(Duration::new(0, 1_000));
        let second_start = Utc::now();
        std::thread::sleep(Duration::new(0, 1_000));

        let first_end = Utc::now();
        w.add_range(1, 2, 2, start_time, first_end, created_at);

        let second_created_at = created_at
            .checked_add(Duration::new(CLOSED_WINDOW_SECONDS as u64 * 2, 0))
            .unwrap();
        let third_start = Utc::now();
        std::thread::sleep(Duration::new(0, 1_000));
        let second_end = Utc::now();
        w.add_range(1, 3, 3, second_start, second_end, second_created_at);

        let third_created_at = second_created_at
            .checked_add(Duration::new(CLOSED_WINDOW_SECONDS as u64, 0))
            .unwrap();
        let third_end = Utc::now();
        w.add_range(1, 5, 2, third_start, third_end, third_created_at);

        let end_at = third_created_at
            .checked_add(Duration::new(CLOSED_WINDOW_SECONDS as u64, 0))
            .unwrap();
        w.rotate(end_at);

        let t_flush = w.t_flush().unwrap();
        assert_eq!(t_flush, first_end);
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
        assert_eq!(c.min_time, t_flush);
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
        let mut w = PersistenceWindows::new(2);
        let created_at = Instant::now();
        let start_time = Utc::now();
        std::thread::sleep(Duration::new(0, 1_000));
        let third_start = Utc::now();
        std::thread::sleep(Duration::new(0, 1_000));

        let first_end = Utc::now();
        w.add_range(1, 2, 2, start_time, first_end, created_at);

        std::thread::sleep(Duration::new(0, 1_000));
        let second_start = Utc::now();
        let second_created_at = created_at
            .checked_add(Duration::new(CLOSED_WINDOW_SECONDS as u64 * 3, 0))
            .unwrap();
        std::thread::sleep(Duration::new(0, 1_000));
        let second_end = Utc::now();
        w.add_range(1, 3, 3, second_start, second_end, second_created_at);

        let third_created_at = second_created_at
            .checked_add(Duration::new(CLOSED_WINDOW_SECONDS as u64, 0))
            .unwrap();
        let third_end = Utc::now();
        w.add_range(1, 5, 2, third_start, third_end, third_created_at);

        let end_at = third_created_at.checked_add(Duration::new(1, 0)).unwrap();
        w.rotate(end_at);

        let t_flush = w.t_flush().unwrap();
        assert_eq!(t_flush, first_end);
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
        assert_eq!(c.min_time, second_start);
        assert_eq!(c.max_time, second_end);
        assert_eq!(c.created_at, second_created_at);

        // the open window should have a min time equal to t_flush
        let c = w.open.as_ref().unwrap();
        assert_eq!(c.row_count, 2);
        assert_eq!(c.min_time, t_flush);
        assert_eq!(c.max_time, third_end);
        assert_eq!(c.created_at, third_created_at);
    }

    #[test]
    fn flush_persistable_overlaps_open_and_closed() {
        let mut w = PersistenceWindows::new(2);
        let created_at = Instant::now();
        let start_time = Utc::now();
        std::thread::sleep(Duration::new(0, 1_000));
        let second_start = Utc::now();
        std::thread::sleep(Duration::new(0, 1_000));
        let third_start = Utc::now();
        std::thread::sleep(Duration::new(0, 1_000));

        let first_end = Utc::now();
        w.add_range(1, 2, 2, start_time, first_end, created_at);

        let second_created_at = created_at
            .checked_add(Duration::new(CLOSED_WINDOW_SECONDS as u64 * 3, 0))
            .unwrap();
        std::thread::sleep(Duration::new(0, 1_000));
        let second_end = Utc::now();
        w.add_range(1, 3, 3, second_start, second_end, second_created_at);

        let third_created_at = second_created_at
            .checked_add(Duration::new(CLOSED_WINDOW_SECONDS as u64, 0))
            .unwrap();
        let third_end = Utc::now();
        w.add_range(1, 5, 2, third_start, third_end, third_created_at);

        let end_at = third_created_at.checked_add(Duration::new(1, 0)).unwrap();
        w.rotate(end_at);

        let t_flush = w.t_flush().unwrap();
        assert_eq!(t_flush, first_end);
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
        assert_eq!(c.min_time, t_flush);
        assert_eq!(c.max_time, second_end);
        assert_eq!(c.created_at, second_created_at);

        // the open window should have a min time equal to t_flush
        let c = w.open.as_ref().unwrap();
        assert_eq!(c.row_count, 2);
        assert_eq!(c.min_time, t_flush);
        assert_eq!(c.max_time, third_end);
        assert_eq!(c.created_at, third_created_at);
    }
}
