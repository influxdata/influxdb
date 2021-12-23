//! Ring buffer of queries that have been run with some brief information

use std::{collections::VecDeque, sync::Arc};

use parking_lot::Mutex;
use time::{Time, TimeProvider};

/// Information about a single query that was executed
#[derive(Debug)]
pub struct QueryLogEntry {
    /// The type of query
    pub query_type: String,

    /// The text of the query (SQL for sql queries, pbjson for storage rpc queries)
    pub query_text: String,

    /// Time at which the query was run
    pub issue_time: Time,
}

impl QueryLogEntry {
    /// Creates a new QueryLogEntry -- use `QueryLog::push` to add new entries to the log
    fn new(query_type: String, query_text: String, issue_time: Time) -> Self {
        Self {
            query_type,
            query_text,
            issue_time,
        }
    }
}

/// Stores a fixed number `QueryExcutions` -- handles locking
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

    pub fn push(&self, query_type: impl Into<String>, query_text: impl Into<String>) {
        if self.max_size == 0 {
            return;
        }

        let entry = Arc::new(QueryLogEntry::new(
            query_type.into(),
            query_text.into(),
            self.time_provider.now(),
        ));

        let mut log = self.log.lock();

        // enforce limit
        if log.len() == self.max_size {
            log.pop_front();
        }

        log.push_back(entry);
    }

    pub fn entries(&self) -> VecDeque<Arc<QueryLogEntry>> {
        let log = self.log.lock();
        log.clone()
    }
}
