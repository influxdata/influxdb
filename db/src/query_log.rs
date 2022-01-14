//! Ring buffer of queries that have been run with some brief information

use std::{
    collections::VecDeque,
    sync::{atomic, Arc},
    time::Duration,
};

use parking_lot::Mutex;
use time::{Time, TimeProvider};

// The query duration used for queries still running.
const UNCOMPLETED_DURATION: i64 = -1;

/// Information about a single query that was executed
#[derive(Debug)]
pub struct QueryLogEntry {
    /// The type of query
    pub query_type: String,

    /// The text of the query (SQL for sql queries, pbjson for storage rpc queries)
    pub query_text: String,

    /// Time at which the query was run
    pub issue_time: Time,

    /// Duration in nanoseconds query took to complete (-1 is a sentinel value
    /// indicating query not completed).
    query_completed_duration: atomic::AtomicI64,
}

impl QueryLogEntry {
    /// Creates a new QueryLogEntry -- use `QueryLog::push` to add new entries to the log
    fn new(query_type: String, query_text: String, issue_time: Time) -> Self {
        Self {
            query_type,
            query_text,
            issue_time,
            query_completed_duration: UNCOMPLETED_DURATION.into(),
        }
    }

    pub fn query_completed_duration(&self) -> Option<Duration> {
        match self
            .query_completed_duration
            .load(atomic::Ordering::Relaxed)
        {
            UNCOMPLETED_DURATION => None,
            d => Some(Duration::from_nanos(d as u64)),
        }
    }

    pub fn set_completed(&self, now: Time) {
        let dur = now - self.issue_time;
        self.query_completed_duration
            .store(dur.as_nanos() as i64, atomic::Ordering::Relaxed);
    }
}

/// Stores a fixed number `QueryExecutions` -- handles locking
/// internally so can be shared across multiple
#[derive(Debug)]
pub struct QueryLog {
    log: Mutex<VecDeque<Arc<QueryLogEntry>>>,
    max_size: usize,
    time_provider: Arc<dyn TimeProvider>,
}

impl QueryLog {
    /// Create a new QueryLog that can hold at most `size` items.
    /// When the `size+1` item is added, item `0` is evicted.
    pub fn new(max_size: usize, time_provider: Arc<dyn TimeProvider>) -> Self {
        Self {
            log: Mutex::new(VecDeque::with_capacity(max_size)),
            max_size,
            time_provider,
        }
    }

    pub fn push(
        &self,
        query_type: impl Into<String>,
        query_text: impl Into<String>,
    ) -> Arc<QueryLogEntry> {
        let entry = Arc::new(QueryLogEntry::new(
            query_type.into(),
            query_text.into(),
            self.time_provider.now(),
        ));

        if self.max_size == 0 {
            return entry;
        }

        let mut log = self.log.lock();

        // enforce limit
        if log.len() == self.max_size {
            log.pop_front();
        }

        log.push_back(Arc::clone(&entry));
        entry
    }

    pub fn entries(&self) -> VecDeque<Arc<QueryLogEntry>> {
        let log = self.log.lock();
        log.clone()
    }

    /// Marks the provided query entry as completed using the current time.
    pub fn set_completed(&self, entry: Arc<QueryLogEntry>) {
        entry.set_completed(self.time_provider.now())
    }
}

#[cfg(test)]
mod test_super {
    use time::MockProvider;

    use super::*;

    #[test]
    fn test_query_log_entry_completed() {
        let time_provider = MockProvider::new(Time::from_timestamp_millis(100));

        let entry = Arc::new(QueryLogEntry::new(
            "sql".into(),
            "SELECT 1".into(),
            time_provider.now(),
        ));
        // query has not completed
        assert_eq!(entry.query_completed_duration(), None);

        // when the query completes at the same time it's issued
        entry.set_completed(time_provider.now());
        assert_eq!(
            entry.query_completed_duration(),
            Some(Duration::from_millis(0))
        );

        // when the query completes some time in the future.
        time_provider.set(Time::from_timestamp_millis(300));
        entry.set_completed(time_provider.now());
        assert_eq!(
            entry.query_completed_duration(),
            Some(Duration::from_millis(200))
        );
    }
}
