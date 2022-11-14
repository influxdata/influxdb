//! Ring buffer of queries that have been run with some brief information

use data_types::NamespaceId;
use iox_query::QueryText;
use iox_time::{Time, TimeProvider};
use parking_lot::Mutex;
use std::{
    collections::VecDeque,
    sync::{atomic, Arc},
    time::Duration,
};
use trace::ctx::TraceId;

// The query duration used for queries still running.
const UNCOMPLETED_DURATION: i64 = -1;

/// Information about a single query that was executed
pub struct QueryLogEntry {
    /// Namespace ID.
    pub namespace_id: NamespaceId,

    /// The type of query
    pub query_type: String,

    /// The text of the query (SQL for sql queries, pbjson for storage rpc queries)
    pub query_text: QueryText,

    /// The trace ID if any
    pub trace_id: Option<TraceId>,

    /// Time at which the query was run
    pub issue_time: Time,

    /// Duration in nanoseconds query took to complete (-1 is a sentinel value
    /// indicating query not completed).
    query_completed_duration: atomic::AtomicI64,

    /// If the query completed successfully
    pub success: atomic::AtomicBool,
}

impl std::fmt::Debug for QueryLogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryLogEntry")
            .field("query_type", &self.query_type)
            .field("query_text", &self.query_text.to_string())
            .field("issue_time", &self.issue_time)
            .field("query_completed_duration", &self.query_completed_duration)
            .field("success", &self.success)
            .finish()
    }
}

impl QueryLogEntry {
    /// Creates a new QueryLogEntry -- use `QueryLog::push` to add new entries to the log
    fn new(
        namespace_id: NamespaceId,
        query_type: String,
        query_text: QueryText,
        trace_id: Option<TraceId>,
        issue_time: Time,
    ) -> Self {
        Self {
            namespace_id,
            query_type,
            query_text,
            trace_id,
            issue_time,
            query_completed_duration: UNCOMPLETED_DURATION.into(),
            success: atomic::AtomicBool::new(false),
        }
    }

    /// If this query is completed, returns `Some(duration)` of how
    /// long it took
    pub fn query_completed_duration(&self) -> Option<Duration> {
        match self
            .query_completed_duration
            .load(atomic::Ordering::Relaxed)
        {
            UNCOMPLETED_DURATION => None,
            d => Some(Duration::from_nanos(d as u64)),
        }
    }

    /// Returns true if `set_completed` was called with `success=true`
    pub fn success(&self) -> bool {
        self.success.load(atomic::Ordering::SeqCst)
    }

    /// Mark this entry complete as of `now`. `success` records if the
    /// entry is successful or not.
    pub fn set_completed(&self, now: Time, success: bool) {
        let dur = now - self.issue_time;
        self.query_completed_duration
            .store(dur.as_nanos() as i64, atomic::Ordering::Relaxed);
        self.success.store(success, atomic::Ordering::SeqCst);
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
        namespace_id: NamespaceId,
        query_type: impl Into<String>,
        query_text: QueryText,
        trace_id: Option<TraceId>,
    ) -> Arc<QueryLogEntry> {
        let entry = Arc::new(QueryLogEntry::new(
            namespace_id,
            query_type.into(),
            query_text,
            trace_id,
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
    /// `success` specifies the query ran successfully
    pub fn set_completed(&self, entry: Arc<QueryLogEntry>, success: bool) {
        entry.set_completed(self.time_provider.now(), success)
    }
}

#[cfg(test)]
mod test_super {
    use iox_time::MockProvider;

    use super::*;

    #[test]
    fn test_query_log_entry_completed() {
        let time_provider = MockProvider::new(Time::from_timestamp_millis(100).unwrap());

        let entry = Arc::new(QueryLogEntry::new(
            NamespaceId::new(1),
            "sql".into(),
            Box::new("SELECT 1"),
            None,
            time_provider.now(),
        ));
        // query has not completed
        assert_eq!(entry.query_completed_duration(), None);
        assert!(!entry.success());

        // when the query completes at the same time it's issued
        entry.set_completed(time_provider.now(), true);
        assert_eq!(
            entry.query_completed_duration(),
            Some(Duration::from_millis(0))
        );
        assert!(entry.success());

        // when the query completes some time in the future.
        time_provider.set(Time::from_timestamp_millis(300).unwrap());
        entry.set_completed(time_provider.now(), false);
        assert_eq!(
            entry.query_completed_duration(),
            Some(Duration::from_millis(200))
        );
        assert!(!entry.success());
    }
}
