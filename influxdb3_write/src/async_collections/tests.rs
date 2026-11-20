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

/// `pop` must wake from advancing the *mock* clock, not from real time elapsing.
///
/// The scheduled delay is far longer than any test should wait in real time, so
/// this only passes if `pop` waits via the injected `TimeProvider`.
#[tokio::test]
async fn test_pop_wakes_on_mock_time_advance() {
    let mock = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let queue = PriorityQueue::new(Arc::clone(&mock) as _);

    queue.push(mock.now() + Duration::from_secs(3600), "Task 1");

    let popped = tokio::spawn({
        let queue = queue.clone();
        async move { queue.pop().await }
    });

    // Let the popping task reach its wait before the clock moves.
    tokio::task::yield_now().await;
    mock.inc(Duration::from_secs(3600));

    assert_eq!(
        popped
            .with_timeout(Duration::from_secs(5))
            .await
            .expect("pop should wake when mock time advances")
            .unwrap(),
        "Task 1"
    );
}

/// Items scheduled at the same time pop in the order they were pushed.
#[tokio::test]
async fn test_pop_breaks_time_ties_by_push_order() {
    let mock = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let queue = PriorityQueue::new(Arc::clone(&mock) as _);

    let due = mock.now() + Duration::from_millis(10);
    let pushed: Vec<usize> = (0..16).collect();
    for i in &pushed {
        queue.push(due, *i);
    }

    mock.inc(Duration::from_millis(10));

    let mut popped = Vec::new();
    for _ in &pushed {
        popped.push(queue.pop().await);
    }
    assert_eq!(popped, pushed);
}
