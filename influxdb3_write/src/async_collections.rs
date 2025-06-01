use iox_time::{Time, TimeProvider};
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tokio::time::sleep;

/// A priority queue that allows items to be pushed with a specific time and popped when their time
/// is reached.
///
/// This queue is designed to be used in asynchronous contexts, where tasks can wait for
/// items to become available based on their scheduled time.
///
/// It is cheap and safe to clone, so that a separate task can be used to push items into the queue.
#[derive(Clone)]
pub(crate) struct PriorityQueue<T> {
    heap: Arc<Mutex<BinaryHeap<Reverse<Item<T>>>>>,
    notify: Arc<Notify>,
    time_provider: Arc<dyn TimeProvider>,
}

impl<T> PriorityQueue<T>
where
    T: Send + 'static,
{
    /// Creates a new empty `PriorityQueue`.
    pub(crate) fn new(time_provider: Arc<dyn TimeProvider>) -> Self {
        Self {
            heap: Arc::new(Mutex::new(BinaryHeap::new())),
            notify: Arc::new(Notify::new()),
            time_provider,
        }
    }

    /// Push an item into the queue with a specific time.
    pub(crate) fn push(&self, time: Time, item: T) {
        let mut heap = self.heap.lock().unwrap();
        heap.push(Reverse(Item::new(time, item)));
        self.notify.notify_one(); // Wake up the waiting task
    }

    /// Pop the next item from the queue that is ready to be processed.
    pub(crate) async fn pop(&self) -> T {
        loop {
            let notify = Arc::clone(&self.notify);
            let next_time = {
                let mut heap = self.heap.lock().unwrap();
                if let Some(Reverse(Item { time, .. })) = heap.peek() {
                    if *time <= self.time_provider.now() {
                        return heap
                            .pop()
                            .map(|Reverse(Item { item, .. })| item)
                            // Expect is safe because we already checked the time via the peek.
                            .expect("item");
                    }
                    Some(*time)
                } else {
                    None
                }
            };

            if let Some(time) = next_time {
                let now = self.time_provider.now();
                if time > now {
                    if let Some(delay) = time.checked_duration_since(now) {
                        tokio::select! {
                            _ = sleep(delay) => {},
                            _ = notify.notified() => continue, // Re-evaluate if woken up
                        }
                    } else {
                        // If duration calculation fails, just yield
                        tokio::task::yield_now().await;
                    }
                }
            } else {
                notify.notified().await;
            }
        }
    }
}

/// A wrapper around an item with its scheduled time, used for ordering in the priority queue.
struct Item<T> {
    /// The time when the item should be dequeued.
    time: Time,
    item: T,
}

impl<T> Item<T> {
    fn new(time: Time, item: T) -> Self {
        Self { time, item }
    }
}

impl<T> Eq for Item<T> {}

impl<T> PartialEq<Self> for Item<T> {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time
    }
}

impl<T> PartialOrd<Self> for Item<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Item<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.time.cmp(&other.time)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use iox_time::MockProvider;
    use std::sync::Arc;
    use test_helpers::timeout::FutureTimeout;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_priority_queue() {
        let mock = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let queue = PriorityQueue::new(Arc::clone(&mock) as _);

        queue.push(mock.now() + Duration::from_millis(20), "Task 1");
        queue.push(mock.now() + Duration::from_millis(10), "Task 2");

        // Verifies that the tasks are popped in the order of their scheduled time.
        // Time hasn't progressed yet, so popping should yield nothing.
        assert!(queue.pop().now_or_never().is_none());
        mock.inc(Duration::from_millis(15));
        // Now the first task should be ready to pop.
        assert_eq!(queue.pop().await, "Task 2");
        mock.inc(Duration::from_millis(15));
        assert_eq!(queue.pop().await, "Task 1");

        // Verifies that popping from an empty queue waits until an item is pushed.
        queue
            .pop()
            .with_timeout(Duration::from_millis(10))
            .await
            .expect_err("should fail");

        // Push another item and verify it can be popped.
        queue.push(mock.now() + Duration::from_millis(10), "Task 3");
        mock.inc(Duration::from_millis(10));
        assert_eq!(queue.pop().await, "Task 3");

        // An item in the past will be popped immediately.
        queue.push(mock.now() - Duration::from_millis(5), "Task 4");
        assert_eq!(queue.pop().await, "Task 4");
    }
}
