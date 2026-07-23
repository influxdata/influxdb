use super::local::run_until_cancelled;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// When the token is not cancelled, `run_until_cancelled` yields the run's
/// result unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn run_until_cancelled_returns_result_when_not_cancelled() {
    let cancel = CancellationToken::new();
    let join = tokio::task::spawn_blocking(|| 42);
    let result = run_until_cancelled(join, &cancel).await;
    assert!(matches!(result, Some(Ok(42))));
}

/// The core of the disable/delete-hang fix: when the per-trigger token is
/// cancelled while a blocking run is in flight, `run_until_cancelled` returns
/// promptly (abandoning the run) instead of waiting for it to finish. If the
/// race were removed this would block on the 30s task and the elapsed-time
/// assertion would fail.
#[tokio::test(flavor = "multi_thread")]
async fn run_until_cancelled_abandons_blocking_run_on_cancel() {
    let cancel = CancellationToken::new();
    // A long-running task standing in for an in-flight plugin run. The local
    // worker uses spawn_blocking; `run_until_cancelled` is generic over the
    // JoinHandle, so an async task exercises the same race while leaving nothing
    // to join at teardown.
    let join = tokio::spawn(async {
        tokio::time::sleep(Duration::from_secs(30)).await;
        42
    });

    // Cancel shortly after the run starts.
    let canceller = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        canceller.cancel();
    });

    let start = std::time::Instant::now();
    let result = run_until_cancelled(join, &cancel).await;
    assert!(
        result.is_none(),
        "a cancelled run must be abandoned, not awaited to completion"
    );
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "must return promptly on cancel, not wait out the blocking run"
    );
}
