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
