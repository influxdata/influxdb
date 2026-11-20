use iox_time::{Time, TimeProvider};
use parking_lot::Mutex;
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::sync::Arc;
use tokio::sync::Notify;

/// A priority queue that allows items to be pushed with a specific time and popped when their time
/// is reached.
///
/// This queue is designed to be used in asynchronous contexts, where tasks can wait for
/// items to become available based on their scheduled time.
///
/// It is cheap and safe to clone, so that a separate task can be used to push items into the queue.
#[derive(Clone)]
pub(crate) struct PriorityQueue<T> {
    queue: Arc<Mutex<Queue<T>>>,
    notify: Arc<Notify>,
    time_provider: Arc<dyn TimeProvider>,
}

/// The queued items and the source of the sequence numbers that order items
/// sharing a time. They are updated together, so one lock covers both.
struct Queue<T> {
    heap: BinaryHeap<Reverse<Item<T>>>,
    next_sequence: u64,
}

impl<T> Default for Queue<T> {
    fn default() -> Self {
        Self {
            heap: BinaryHeap::new(),
            next_sequence: 0,
        }
    }
}

impl<T> PriorityQueue<T>
where
    T: Send + 'static,
{
    /// Creates a new empty `PriorityQueue`.
    pub(crate) fn new(time_provider: Arc<dyn TimeProvider>) -> Self {
        Self {
            queue: Arc::new(Mutex::new(Queue::default())),
            notify: Arc::new(Notify::new()),
            time_provider,
        }
    }

    /// Push an item into the queue with a specific time.
    pub(crate) fn push(&self, time: Time, item: T) {
        let mut queue = self.queue.lock();
        let sequence = queue.next_sequence;
        queue.next_sequence += 1;
        queue.heap.push(Reverse(Item::new(time, sequence, item)));
        self.notify.notify_one(); // Wake up the waiting task
    }

    /// Pop the next item from the queue that is ready to be processed.
    pub(crate) async fn pop(&self) -> T {
        loop {
            let notify = Arc::clone(&self.notify);
            let next_time = {
                let mut queue = self.queue.lock();
                if let Some(Reverse(Item { time, .. })) = queue.heap.peek() {
                    if *time <= self.time_provider.now() {
                        return queue
                            .heap
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
                    tokio::select! {
                        _ = self.time_provider.sleep_until(time) => {},
                        _ = notify.notified() => continue, // Re-evaluate if woken up
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
    /// Tie-breaker for items sharing a `time`, so they dequeue in push order.
    sequence: u64,
    item: T,
}

impl<T> Item<T> {
    fn new(time: Time, sequence: u64, item: T) -> Self {
        Self {
            time,
            sequence,
            item,
        }
    }
}

impl<T> Eq for Item<T> {}

impl<T> PartialEq<Self> for Item<T> {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time && self.sequence == other.sequence
    }
}

impl<T> PartialOrd<Self> for Item<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Item<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.time
            .cmp(&other.time)
            .then_with(|| self.sequence.cmp(&other.sequence))
    }
}

#[cfg(test)]
mod tests;
