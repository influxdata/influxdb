use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use futures::FutureExt;
use iox_time::{MockProvider, Time, TimeProvider};
use observability_deps::tracing::{error, info};
use tokio_util::sync::CancellationToken;

use crate::{AbortableTaskRunner, AbortableTaskRunnerError, ShutdownManager};

#[tokio::test]
async fn test_shutdown_order() {
    let frontend_token = CancellationToken::new();
    let shutdown_manager = ShutdownManager::new(frontend_token.clone());

    static CLEAN: AtomicBool = AtomicBool::new(false);

    let token = shutdown_manager.register("test_component");
    tokio::spawn(async move {
        loop {
            futures::select! {
                _ = token.wait_for_shutdown().fuse() => {
                    CLEAN.store(true, Ordering::SeqCst);
                    break;
                }
                _ = tokio::time::sleep(Duration::from_millis(10)).fuse() => {
                    // sleeping... 😴
                }
            }
        }
    });

    shutdown_manager.shutdown();
    shutdown_manager.join().await;
    assert!(
        CLEAN.load(Ordering::SeqCst),
        "backend shutdown did not compelte"
    );
    assert!(
        frontend_token.is_cancelled(),
        "frontend shutdown was not triggered"
    );
}

#[test_log::test(tokio::test)]
async fn test_abortable_background_task_runner() {
    let timer = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let worker_timer = Arc::clone(&timer);
    let cancel_timer = Arc::clone(&timer);
    let fut = async move {
        let mut count = 0;
        loop {
            count += 1;
            if count > 1_000_000_000 {
                break;
            }
            worker_timer.sleep(Duration::from_millis(10)).await;
        }
        count
    };

    let token = CancellationToken::new();
    let token_clone = token.clone();

    let runner = AbortableTaskRunner::new(
        fut,
        Arc::clone(&timer) as _,
        Duration::from_millis(1),
        token,
    );

    tokio::spawn(async move {
        // wait for a millisecond and cancel
        cancel_timer.sleep(Duration::from_millis(1)).await;
        token_clone.cancel();
    });

    tokio::spawn(async move {
        loop {
            timer.inc(Duration::from_millis(10));
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    let res = runner.run().await.unwrap_err();
    error!(?res, "error after abort");
    assert!(matches!(res, AbortableTaskRunnerError::Aborted));
}

#[test_log::test(tokio::test)]
async fn test_abortable_background_task_runner_runs_forever_should_abort() {
    let timer = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let worker_timer = Arc::clone(&timer);
    let cancel_timer = Arc::clone(&timer);
    let atomic_counter = Arc::new(AtomicU64::new(0));
    let cloned = Arc::clone(&atomic_counter);
    let fut = async move {
        loop {
            let current = atomic_counter.fetch_add(1, Ordering::SeqCst);
            if current > 1_000_000_000 {
                break;
            }
            worker_timer.sleep(Duration::from_millis(10)).await;
        }
        atomic_counter.load(Ordering::SeqCst)
    };

    let token = CancellationToken::new();
    let token_clone = token.clone();

    let runner = AbortableTaskRunner::new(
        fut,
        Arc::clone(&timer) as _,
        Duration::from_millis(1),
        token,
    );

    tokio::spawn(async move {
        // wait for a millisecond and cancel
        cancel_timer.sleep(Duration::from_millis(1)).await;
        token_clone.cancel();
    });

    tokio::spawn(async move {
        loop {
            timer.inc(Duration::from_millis(10));
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    // although it's set to run forever because of cancelling the token this should stop
    // running forever and break
    runner.run_in_background();

    tokio::time::sleep(Duration::from_millis(500)).await;
    let current_1 = cloned.load(Ordering::SeqCst);
    info!(current_1, "current val");
    assert_ne!(current_1, 1_000_000_000);

    tokio::time::sleep(Duration::from_millis(500)).await;
    let current_2 = cloned.load(Ordering::SeqCst);
    info!(current_2, "current val");
    assert_ne!(current_2, 1_000_000_000);
    assert_eq!(current_1, current_2);
}

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn test_abortable_background_task_runner_with_waits_in_loop() {
    let timer = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let worker_timer = Arc::clone(&timer);
    let control_timer = Arc::clone(&timer);
    let cancel_timer = Arc::clone(&timer);
    let atomic_counter = Arc::new(AtomicU64::new(0));
    let cloned = Arc::clone(&atomic_counter);

    let token = CancellationToken::new();
    let token_clone = token.clone();

    tokio::spawn(async move {
        // wait for a millisecond and cancel
        info!("sleeping inside cancel loop");
        cancel_timer.sleep(Duration::from_millis(1)).await;
        info!("sending cancel signal");
        token_clone.cancel();
    });

    tokio::spawn(async move {
        loop {
            info!("incrementing time");
            control_timer.inc(Duration::from_millis(10));
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    let fut = async move {
        loop {
            worker_timer.sleep(Duration::from_millis(50)).await;
            let current = atomic_counter.fetch_add(1, Ordering::SeqCst);
            if current > 1_000_000_000 {
                break;
            }
        }
    };

    let runner = AbortableTaskRunner::new(
        fut,
        Arc::clone(&timer) as _,
        Duration::from_millis(1),
        token,
    );

    info!("running worker loop");
    let res = runner.run().await.unwrap_err();
    error!(?res, "error after abort");
    assert!(matches!(res, AbortableTaskRunnerError::Aborted));
    let current_count = cloned.load(Ordering::SeqCst);
    // because we slept for 50ms task will be aborted before incrementing the counter so
    // this should always be 0
    assert_eq!(current_count, 0);
}

#[tokio::test]
async fn test_named_shutdown_tracking() {
    let frontend_token = CancellationToken::new();
    let shutdown_manager = ShutdownManager::new(frontend_token.clone());

    let token_a = shutdown_manager.register("component_a");
    let token_b = shutdown_manager.register("component_b");

    assert_eq!(shutdown_manager.pending_components().len(), 2);
    assert!(
        shutdown_manager
            .pending_components()
            .contains(&"component_a")
    );
    assert!(
        shutdown_manager
            .pending_components()
            .contains(&"component_b")
    );

    // Complete one component
    token_a.complete();
    assert_eq!(shutdown_manager.pending_components().len(), 1);
    assert!(
        shutdown_manager
            .pending_components()
            .contains(&"component_b")
    );

    // Drop the other (should also remove from pending)
    drop(token_b);
    assert!(shutdown_manager.pending_components().is_empty());
}
