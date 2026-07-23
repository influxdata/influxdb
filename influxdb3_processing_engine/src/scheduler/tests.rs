use super::*;
use crate::scheduler_worker_protocol::TriggerExecutionError;
use influxdb3_catalog::catalog::{
    ErrorBehavior, NodeSpec, TriggerSettings, TriggerSpecificationDefinition,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

fn protocol_node(value: &str) -> Arc<str> {
    Arc::from(value)
}

#[derive(Debug)]
struct FakeWorker {
    failures_before_success: AtomicUsize,
    attempts: AtomicUsize,
    active: AtomicUsize,
    max_active: AtomicUsize,
    started: Notify,
    release: Notify,
    block: bool,
}

impl FakeWorker {
    fn new(failures_before_success: usize) -> Arc<Self> {
        Arc::new(Self {
            failures_before_success: AtomicUsize::new(failures_before_success),
            attempts: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            started: Notify::new(),
            release: Notify::new(),
            block: false,
        })
    }

    fn blocking() -> Arc<Self> {
        Arc::new(Self {
            failures_before_success: AtomicUsize::new(0),
            attempts: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            started: Notify::new(),
            release: Notify::new(),
            block: true,
        })
    }

    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }

    fn max_active(&self) -> usize {
        self.max_active.load(Ordering::SeqCst)
    }

    fn factory(
        self: &Arc<Self>,
    ) -> impl FnOnce(Arc<dyn TriggerScheduler>) -> Vec<Arc<dyn TriggerWorker>> {
        let state = Arc::clone(self);
        move |completion| vec![Arc::new(FakeWorkerHandle { state, completion })]
    }

    async fn execute_once(
        &self,
        work: TriggerWork,
    ) -> Result<TriggerWorkOutput, TriggerExecutionError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        self.started.notify_waiters();
        if self.block {
            self.release.notified().await;
        }
        self.active.fetch_sub(1, Ordering::SeqCst);
        if self
            .failures_before_success
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_sub(1)
            })
            .is_ok()
        {
            Err(TriggerExecutionError::new("boom"))
        } else {
            match work.payload {
                TriggerWorkPayload::Request(_) => {
                    Ok(TriggerWorkOutput::RequestResponse(TriggerResponse {
                        status_code: StatusCode::OK.as_u16(),
                        headers: Default::default(),
                        body: "ok".to_string(),
                    }))
                }
                TriggerWorkPayload::Wal { .. } | TriggerWorkPayload::Schedule { .. } => {
                    Ok(TriggerWorkOutput::Complete)
                }
            }
        }
    }
}

struct FakeWorkerHandle {
    state: Arc<FakeWorker>,
    completion: Arc<dyn TriggerScheduler>,
}

impl TriggerWorker for FakeWorkerHandle {
    fn node_id(&self) -> Arc<str> {
        protocol_node("worker")
    }

    fn submit_work(self: Arc<Self>, _scheduler_node_id: Arc<str>, work: TriggerWork) {
        tokio::spawn(async move {
            let work_id = work.id;
            let completion = Arc::clone(&self.completion);
            completion.work_progressed(self.node_id(), work_id);
            let result = self.state.execute_once(work).await;
            completion.work_finished(self.node_id(), TriggerWorkResult { work_id, result });
        });
    }

    fn cancel_work(self: Arc<Self>, _scheduler_node_id: Arc<str>, _work_id: TriggerWorkId) {
        self.state.release.notify_waiters();
    }
}

struct ManualCallbackWorker {
    node_id: Arc<str>,
    submissions: Mutex<Vec<(Arc<str>, TriggerWorkId)>>,
    cancellations: Mutex<Vec<(Arc<str>, TriggerWorkId)>>,
    submitted: Notify,
    cancelled: Notify,
    completion: Mutex<Option<Arc<dyn TriggerScheduler>>>,
}

impl ManualCallbackWorker {
    fn new() -> Arc<Self> {
        Self::with_node_id(protocol_node("worker"))
    }

    fn with_node_id(node_id: Arc<str>) -> Arc<Self> {
        Arc::new(Self {
            node_id,
            submissions: Mutex::new(Vec::new()),
            cancellations: Mutex::new(Vec::new()),
            submitted: Notify::new(),
            cancelled: Notify::new(),
            completion: Mutex::new(None),
        })
    }

    fn submission_count(&self) -> usize {
        self.submissions.lock().len()
    }

    fn submission(&self, idx: usize) -> ((Arc<str>, TriggerWorkId), Arc<dyn TriggerScheduler>) {
        let submissions = self.submissions.lock();
        let completion = Arc::clone(
            self.completion
                .lock()
                .as_ref()
                .expect("manual callback worker should have a scheduler completion handle"),
        );
        (submissions[idx].clone(), completion)
    }

    fn cancellation_count(&self) -> usize {
        self.cancellations.lock().len()
    }

    fn factory(
        self: &Arc<Self>,
    ) -> impl FnOnce(Arc<dyn TriggerScheduler>) -> Vec<Arc<dyn TriggerWorker>> {
        let state = Arc::clone(self);
        move |completion| vec![state.handle(completion)]
    }

    fn handle(self: &Arc<Self>, completion: Arc<dyn TriggerScheduler>) -> Arc<dyn TriggerWorker> {
        *self.completion.lock() = Some(completion);
        Arc::new(ManualCallbackWorkerHandle {
            state: Arc::clone(self),
        })
    }
}

struct ManualCallbackWorkerHandle {
    state: Arc<ManualCallbackWorker>,
}

impl TriggerWorker for ManualCallbackWorkerHandle {
    fn node_id(&self) -> Arc<str> {
        Arc::clone(&self.state.node_id)
    }

    fn submit_work(self: Arc<Self>, scheduler_node_id: Arc<str>, work: TriggerWork) {
        self.state
            .submissions
            .lock()
            .push((scheduler_node_id, work.id));
        self.state.submitted.notify_waiters();
    }

    fn cancel_work(self: Arc<Self>, scheduler_node_id: Arc<str>, work_id: TriggerWorkId) {
        self.state
            .cancellations
            .lock()
            .push((scheduler_node_id, work_id));
        self.state.cancelled.notify_waiters();
    }
}

fn trigger(error_behavior: ErrorBehavior, run_async: bool) -> Arc<TriggerDefinition> {
    Arc::new(TriggerDefinition {
        trigger_id: TriggerId::new(2),
        trigger_name: Arc::from("trig"),
        plugin_filename: "plugin.py".to_string(),
        database_name: Arc::from("db"),
        node_spec: NodeSpec::All,
        trigger: TriggerSpecificationDefinition::AllTablesWalWrite,
        trigger_settings: TriggerSettings {
            run_async,
            error_behavior,
        },
        trigger_arguments: None,
        disabled: false,
    })
}

fn payload() -> TriggerPayload {
    TriggerPayload::Schedule {
        scheduled_at: Utc::now(),
    }
}

fn request_payload() -> (TriggerPayload, oneshot::Receiver<Response>) {
    let (tx, rx) = oneshot::channel();
    (
        TriggerPayload::Request(RequestPayload::new(
            HashMap::new(),
            HashMap::new(),
            Bytes::new(),
            tx,
        )),
        rx,
    )
}

#[tokio::test]
async fn request_response_drops_invalid_headers() {
    let (response_tx, response_rx) = oneshot::channel();
    let payload = RequestPayload::new(HashMap::new(), HashMap::new(), Bytes::new(), response_tx);
    let mut headers = HashMap::new();
    headers.insert("x-valid".to_string(), "valid".to_string());
    headers.insert("x-invalid-value".to_string(), "invalid\nvalue".to_string());
    headers.insert("invalid header name".to_string(), "value".to_string());

    payload.send_work_response(TriggerResponse {
        status_code: StatusCode::OK.as_u16(),
        headers,
        body: "ok".to_string(),
    });

    let response = response_rx
        .await
        .expect("request should receive a response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-valid").unwrap(), "valid");
    assert!(response.headers().get("x-invalid-value").is_none());
    assert!(response.headers().get("invalid header name").is_none());
}

fn counting_auto_disable(auto_disable_count: Arc<AtomicUsize>, result: bool) -> AutoDisable {
    Arc::new(move || {
        let auto_disable_count = Arc::clone(&auto_disable_count);
        Box::pin(async move {
            auto_disable_count.fetch_add(1, Ordering::SeqCst);
            result
        }) as AutoDisableFuture
    })
}

fn noop_auto_disable() -> AutoDisable {
    Arc::new(|| Box::pin(async { true }) as AutoDisableFuture)
}

async fn spawn_test_scheduler(
    trigger_definition: Arc<TriggerDefinition>,
    make_workers: impl FnOnce(Arc<dyn TriggerScheduler>) -> Vec<Arc<dyn TriggerWorker>>,
    cancel: CancellationToken,
    auto_disable_count: Arc<AtomicUsize>,
) -> (Scheduler, TriggerKey) {
    let key = TriggerKey {
        db_id: DbId::new(1),
        trigger_id: TriggerId::new(2),
    };
    let scheduler = Scheduler::new(protocol_node("scheduler"), make_workers);
    scheduler
        .register_trigger(TriggerRegistration {
            key,
            trigger_definition: Arc::clone(&trigger_definition),
            cancel,
            config: SchedulerConfig::new(
                16,
                trigger_definition.trigger_settings.run_async,
                NonZeroUsize::MAX,
            ),
            auto_disable: counting_auto_disable(auto_disable_count, true),
        })
        .await;
    (scheduler, key)
}

#[tokio::test(start_paused = true)]
async fn retry_policy_uses_five_total_attempts_and_backoff() {
    let worker = FakeWorker::new(usize::MAX);
    let disable_count = Arc::new(AtomicUsize::new(0));
    let (scheduler, key) = spawn_test_scheduler(
        trigger(ErrorBehavior::Retry, false),
        worker.factory(),
        CancellationToken::new(),
        disable_count,
    )
    .await;

    scheduler
        .enqueue(TriggerInvocation::new(key, payload()))
        .await
        .unwrap();
    while worker.attempts() < 1 {
        worker.started.notified().await;
    }
    for (idx, delay) in [100, 200, 400, 800].into_iter().enumerate() {
        tokio::time::advance(std::time::Duration::from_millis(delay)).await;
        while worker.attempts() < idx + 2 {
            worker.started.notified().await;
        }
    }

    assert_eq!(worker.attempts(), 5);
    scheduler.shutdown_trigger(key).await;
}

#[tokio::test(start_paused = true)]
async fn request_retry_returns_success_after_transient_failure() {
    let worker = FakeWorker::new(2);
    let disable_count = Arc::new(AtomicUsize::new(0));
    let (scheduler, key) = spawn_test_scheduler(
        trigger(ErrorBehavior::Retry, false),
        worker.factory(),
        CancellationToken::new(),
        disable_count,
    )
    .await;
    let (payload, response_rx) = request_payload();

    scheduler
        .enqueue(TriggerInvocation::new(key, payload))
        .await
        .unwrap();
    while worker.attempts() < 1 {
        worker.started.notified().await;
    }
    for (idx, delay) in [100, 200].into_iter().enumerate() {
        tokio::time::advance(std::time::Duration::from_millis(delay)).await;
        while worker.attempts() < idx + 2 {
            worker.started.notified().await;
        }
    }

    let response = response_rx
        .await
        .expect("request should receive a response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(worker.attempts(), 3);
    scheduler.shutdown_trigger(key).await;
}

#[tokio::test(start_paused = true)]
async fn request_retry_exhaustion_returns_500_once() {
    let worker = FakeWorker::new(usize::MAX);
    let disable_count = Arc::new(AtomicUsize::new(0));
    let (scheduler, key) = spawn_test_scheduler(
        trigger(ErrorBehavior::Retry, false),
        worker.factory(),
        CancellationToken::new(),
        disable_count,
    )
    .await;
    let (payload, response_rx) = request_payload();

    scheduler
        .enqueue(TriggerInvocation::new(key, payload))
        .await
        .unwrap();
    while worker.attempts() < 1 {
        worker.started.notified().await;
    }
    for (idx, delay) in [100, 200, 400, 800].into_iter().enumerate() {
        tokio::time::advance(std::time::Duration::from_millis(delay)).await;
        while worker.attempts() < idx + 2 {
            worker.started.notified().await;
        }
    }

    let response = response_rx
        .await
        .expect("request should receive a response");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(worker.attempts(), 5);
    scheduler.shutdown_trigger(key).await;
}

#[tokio::test]
async fn cancelled_worker_result_returns_503_without_retrying() {
    let worker = ManualCallbackWorker::new();
    let disable_count = Arc::new(AtomicUsize::new(0));
    let (scheduler, key) = spawn_test_scheduler(
        trigger(ErrorBehavior::Retry, false),
        worker.factory(),
        CancellationToken::new(),
        disable_count,
    )
    .await;
    let (payload, response_rx) = request_payload();

    scheduler
        .enqueue(TriggerInvocation::new(key, payload))
        .await
        .unwrap();
    while worker.submission_count() < 1 {
        worker.submitted.notified().await;
    }
    let ((_scheduler_node_id, work_id), completion) = worker.submission(0);
    completion.work_finished(
        protocol_node("worker"),
        TriggerWorkResult {
            work_id,
            result: Err(TriggerExecutionError::cancelled()),
        },
    );

    let response = response_rx
        .await
        .expect("cancelled request should receive a response");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    tokio::task::yield_now().await;
    assert_eq!(worker.submission_count(), 1);
    scheduler.shutdown_trigger(key).await;
}

#[tokio::test(start_paused = true)]
async fn retry_backoff_is_cancelled_promptly() {
    let worker = FakeWorker::new(usize::MAX);
    let cancel = CancellationToken::new();
    let disable_count = Arc::new(AtomicUsize::new(0));
    let (scheduler, key) = spawn_test_scheduler(
        trigger(ErrorBehavior::Retry, false),
        worker.factory(),
        cancel.clone(),
        disable_count,
    )
    .await;

    scheduler
        .enqueue(TriggerInvocation::new(key, payload()))
        .await
        .unwrap();
    worker.started.notified().await;
    cancel.cancel();
    scheduler.shutdown_trigger(key).await;

    assert_eq!(worker.attempts(), 1);
}

#[tokio::test]
async fn disable_policy_calls_auto_disable_and_stops_runtime() {
    let worker = FakeWorker::new(usize::MAX);
    let cancel = CancellationToken::new();
    let disable_count = Arc::new(AtomicUsize::new(0));
    let (scheduler, key) = spawn_test_scheduler(
        trigger(ErrorBehavior::Disable, false),
        worker.factory(),
        cancel.clone(),
        Arc::clone(&disable_count),
    )
    .await;

    scheduler
        .enqueue(TriggerInvocation::new(key, payload()))
        .await
        .unwrap();
    while disable_count.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    scheduler.shutdown_trigger(key).await;

    assert_eq!(worker.attempts(), 1);
    assert_eq!(disable_count.load(Ordering::SeqCst), 1);
    assert!(cancel.is_cancelled());
}

#[tokio::test]
async fn disable_policy_keeps_runtime_when_auto_disable_fails() {
    let worker = FakeWorker::new(usize::MAX);
    let cancel = CancellationToken::new();
    let disable_count = Arc::new(AtomicUsize::new(0));
    let trigger_definition = trigger(ErrorBehavior::Disable, false);
    let key = TriggerKey {
        db_id: DbId::new(1),
        trigger_id: TriggerId::new(2),
    };
    let scheduler = Scheduler::new(protocol_node("scheduler"), worker.factory());
    scheduler
        .register_trigger(TriggerRegistration {
            key,
            trigger_definition,
            cancel: cancel.clone(),
            config: SchedulerConfig::new(16, false, NonZeroUsize::MAX),
            auto_disable: counting_auto_disable(Arc::clone(&disable_count), false),
        })
        .await;

    scheduler
        .enqueue(TriggerInvocation::new(key, payload()))
        .await
        .unwrap();
    while disable_count.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }

    assert_eq!(worker.attempts(), 1);
    assert_eq!(disable_count.load(Ordering::SeqCst), 1);
    assert!(!cancel.is_cancelled());
    scheduler.shutdown_trigger(key).await;
}

#[tokio::test]
async fn shutdown_cancels_stalled_auto_disable() {
    let worker = ManualCallbackWorker::new();
    let key = TriggerKey {
        db_id: DbId::new(1),
        trigger_id: TriggerId::new(2),
    };
    let scheduler = Scheduler::new(protocol_node("scheduler"), worker.factory());
    scheduler
        .register_trigger(TriggerRegistration {
            key,
            trigger_definition: trigger(ErrorBehavior::Disable, false),
            cancel: CancellationToken::new(),
            config: SchedulerConfig::new(16, false, NonZeroUsize::MAX),
            auto_disable: Arc::new(|| Box::pin(std::future::pending()) as AutoDisableFuture),
        })
        .await;

    scheduler
        .enqueue(TriggerInvocation::new(key, payload()))
        .await
        .unwrap();
    while worker.submission_count() < 1 {
        worker.submitted.notified().await;
    }
    let ((_scheduler_node_id, work_id), completion) = worker.submission(0);
    completion.work_finished(
        protocol_node("worker"),
        TriggerWorkResult {
            work_id,
            result: Err(TriggerExecutionError::new("boom")),
        },
    );

    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        scheduler.shutdown_trigger(key),
    )
    .await
    .expect("shutdown should cancel the pending auto-disable operation");
}

#[tokio::test]
async fn run_async_false_is_serial_fifo() {
    let worker = FakeWorker::blocking();
    let disable_count = Arc::new(AtomicUsize::new(0));
    let (scheduler, key) = spawn_test_scheduler(
        trigger(ErrorBehavior::Log, false),
        worker.factory(),
        CancellationToken::new(),
        disable_count,
    )
    .await;

    scheduler
        .enqueue(TriggerInvocation::new(key, payload()))
        .await
        .unwrap();
    scheduler
        .enqueue(TriggerInvocation::new(key, payload()))
        .await
        .unwrap();
    worker.started.notified().await;
    tokio::task::yield_now().await;
    assert_eq!(worker.attempts(), 1);
    worker.release.notify_waiters();
    worker.started.notified().await;
    assert_eq!(worker.attempts(), 2);
    worker.release.notify_waiters();
    scheduler.shutdown_trigger(key).await;
    assert_eq!(worker.max_active(), 1);
}

#[tokio::test]
async fn scheduler_slot_is_released_by_worker_completion_callback() {
    let worker = ManualCallbackWorker::new();
    let disable_count = Arc::new(AtomicUsize::new(0));
    let (scheduler, key) = spawn_test_scheduler(
        trigger(ErrorBehavior::Log, false),
        worker.factory(),
        CancellationToken::new(),
        disable_count,
    )
    .await;

    scheduler
        .enqueue(TriggerInvocation::new(key, payload()))
        .await
        .unwrap();
    while worker.submission_count() < 1 {
        worker.submitted.notified().await;
    }

    scheduler
        .enqueue(TriggerInvocation::new(key, payload()))
        .await
        .unwrap();
    tokio::task::yield_now().await;
    assert_eq!(worker.submission_count(), 1);

    let ((_scheduler_node_id, work_id), completion) = worker.submission(0);
    completion.work_progressed(protocol_node("worker"), work_id);
    completion.work_finished(
        protocol_node("worker"),
        TriggerWorkResult {
            work_id,
            result: Ok(TriggerWorkOutput::Complete),
        },
    );
    while worker.submission_count() < 2 {
        worker.submitted.notified().await;
    }

    let ((_scheduler_node_id, work_id), completion) = worker.submission(1);
    completion.work_finished(
        protocol_node("worker"),
        TriggerWorkResult {
            work_id,
            result: Ok(TriggerWorkOutput::Complete),
        },
    );
    scheduler.shutdown_trigger(key).await;
}

#[tokio::test]
async fn scheduler_detaches_work_when_worker_does_not_acknowledge_cancellation() {
    let worker = ManualCallbackWorker::new();
    let disable_count = Arc::new(AtomicUsize::new(0));
    let (scheduler, key) = spawn_test_scheduler(
        trigger(ErrorBehavior::Log, false),
        worker.factory(),
        CancellationToken::new(),
        disable_count,
    )
    .await;

    scheduler
        .enqueue(TriggerInvocation::new(key, payload()))
        .await
        .unwrap();
    while worker.submission_count() < 1 {
        worker.submitted.notified().await;
    }
    let ((scheduler_node_id, work_id), _completion) = worker.submission(0);
    assert_eq!(scheduler_node_id, protocol_node("scheduler"));

    let shutdown = tokio::spawn(async move {
        scheduler.shutdown_trigger(key).await;
    });
    while worker.cancellation_count() < 1 {
        worker.cancelled.notified().await;
    }
    assert_eq!(
        worker.cancellations.lock().as_slice(),
        &[(protocol_node("scheduler"), work_id)]
    );
    shutdown
        .await
        .expect("scheduler shutdown task should complete without a worker completion callback");
}

#[tokio::test]
async fn worker_progress_cancels_work_when_runtime_token_is_cancelled() {
    let worker = ManualCallbackWorker::new();
    let cancel = CancellationToken::new();
    let disable_count = Arc::new(AtomicUsize::new(0));
    let (scheduler, key) = spawn_test_scheduler(
        trigger(ErrorBehavior::Log, false),
        worker.factory(),
        cancel.clone(),
        disable_count,
    )
    .await;

    scheduler
        .enqueue(TriggerInvocation::new(key, payload()))
        .await
        .unwrap();
    while worker.submission_count() < 1 {
        worker.submitted.notified().await;
    }
    let ((_scheduler_node_id, work_id), completion) = worker.submission(0);

    cancel.cancel();
    completion.work_progressed(protocol_node("worker"), work_id);
    while worker.cancellation_count() < 1 {
        worker.cancelled.notified().await;
    }
    assert_eq!(
        worker.cancellations.lock().as_slice(),
        &[(protocol_node("scheduler"), work_id)]
    );

    completion.work_finished(
        protocol_node("worker"),
        TriggerWorkResult {
            work_id,
            result: Ok(TriggerWorkOutput::Complete),
        },
    );
    scheduler.shutdown_trigger(key).await;
}

#[tokio::test]
async fn scheduler_round_robins_workers_and_routes_cancellation() {
    let first_worker = ManualCallbackWorker::with_node_id(protocol_node("worker"));
    let second_worker = ManualCallbackWorker::with_node_id(protocol_node("worker-two"));
    let first_worker_for_factory = Arc::clone(&first_worker);
    let second_worker_for_factory = Arc::clone(&second_worker);
    let scheduler = Scheduler::new(protocol_node("scheduler"), move |completion| {
        vec![
            first_worker_for_factory.handle(Arc::clone(&completion)),
            second_worker_for_factory.handle(completion),
        ]
    });
    let key = TriggerKey {
        db_id: DbId::new(1),
        trigger_id: TriggerId::new(2),
    };
    scheduler
        .register_trigger(TriggerRegistration {
            key,
            trigger_definition: trigger(ErrorBehavior::Log, true),
            cancel: CancellationToken::new(),
            config: SchedulerConfig::new(16, true, NonZeroUsize::MAX),
            auto_disable: noop_auto_disable(),
        })
        .await;

    scheduler
        .enqueue(TriggerInvocation::new(key, payload()))
        .await
        .unwrap();
    scheduler
        .enqueue(TriggerInvocation::new(key, payload()))
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while first_worker.submission_count() < 1 {
            first_worker.submitted.notified().await;
        }
        while second_worker.submission_count() < 1 {
            second_worker.submitted.notified().await;
        }
    })
    .await
    .expect("two invocations should be dispatched to the two workers");

    let ((first_scheduler_node_id, first_work_id), _first_completion) = first_worker.submission(0);
    let ((second_scheduler_node_id, second_work_id), _second_completion) =
        second_worker.submission(0);
    assert_eq!(first_scheduler_node_id, protocol_node("scheduler"));
    assert_eq!(second_scheduler_node_id, protocol_node("scheduler"));

    let shutdown = tokio::spawn(async move {
        scheduler.shutdown_trigger(key).await;
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while first_worker.cancellation_count() < 1 {
            first_worker.cancelled.notified().await;
        }
        while second_worker.cancellation_count() < 1 {
            second_worker.cancelled.notified().await;
        }
    })
    .await
    .expect("cancellation should be routed to both selected workers");
    assert_eq!(
        first_worker.cancellations.lock().as_slice(),
        &[(protocol_node("scheduler"), first_work_id)]
    );
    assert_eq!(
        second_worker.cancellations.lock().as_slice(),
        &[(protocol_node("scheduler"), second_work_id)]
    );

    shutdown
        .await
        .expect("scheduler shutdown should complete after routing cancellation");
}

#[tokio::test]
async fn process_wide_scheduler_tracks_concurrency_per_trigger() {
    let worker = FakeWorker::blocking();
    let scheduler = Scheduler::new(protocol_node("scheduler"), worker.factory());
    let trigger_definition = trigger(ErrorBehavior::Log, false);
    let key_a = TriggerKey {
        db_id: DbId::new(1),
        trigger_id: TriggerId::new(2),
    };
    let key_b = TriggerKey {
        db_id: DbId::new(1),
        trigger_id: TriggerId::new(3),
    };
    for key in [key_a, key_b] {
        scheduler
            .register_trigger(TriggerRegistration {
                key,
                trigger_definition: Arc::clone(&trigger_definition),
                cancel: CancellationToken::new(),
                config: SchedulerConfig::new(16, false, NonZeroUsize::MAX),
                auto_disable: noop_auto_disable(),
            })
            .await;
    }

    scheduler
        .enqueue(TriggerInvocation::new(key_a, payload()))
        .await
        .unwrap();
    scheduler
        .enqueue(TriggerInvocation::new(key_b, payload()))
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while worker.attempts() < 2 {
            worker.started.notified().await;
        }
    })
    .await
    .expect("both trigger runtimes should dispatch on the shared scheduler");

    assert_eq!(worker.max_active(), 2);
    worker.release.notify_waiters();
    scheduler.shutdown_trigger(key_a).await;
    scheduler.shutdown_trigger(key_b).await;
}

#[tokio::test]
async fn run_async_true_respects_concurrency_bound() {
    let worker = FakeWorker::blocking();
    let key = TriggerKey {
        db_id: DbId::new(1),
        trigger_id: TriggerId::new(2),
    };
    let scheduler = Scheduler::new(protocol_node("scheduler"), worker.factory());
    scheduler
        .register_trigger(TriggerRegistration {
            key,
            trigger_definition: trigger(ErrorBehavior::Log, true),
            cancel: CancellationToken::new(),
            config: SchedulerConfig::new(16, true, NonZeroUsize::new(3).unwrap()),
            auto_disable: noop_auto_disable(),
        })
        .await;

    for _ in 0..7 {
        scheduler
            .enqueue(TriggerInvocation::new(key, payload()))
            .await
            .unwrap();
    }
    while worker.attempts() < 3 {
        worker.started.notified().await;
    }
    tokio::task::yield_now().await;
    assert_eq!(worker.attempts(), 3);
    assert_eq!(worker.max_active(), 3);

    // Queued invocations start as running ones complete, and the bound holds
    // throughout the drain.
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while worker.attempts() < 7 {
            worker.release.notify_waiters();
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("queued invocations should start after completions");
    assert_eq!(worker.max_active(), 3);
    worker.release.notify_waiters();
    scheduler.shutdown_trigger(key).await;
}

#[tokio::test]
async fn run_async_unlimited_dispatches_all_admitted() {
    let worker = FakeWorker::blocking();
    let key = TriggerKey {
        db_id: DbId::new(1),
        trigger_id: TriggerId::new(2),
    };
    let scheduler = Scheduler::new(protocol_node("scheduler"), worker.factory());
    // Nominal queue size 4 so this test proves the unlimited default also
    // lifts the outstanding-invocations capacity, not just the dispatch bound.
    scheduler
        .register_trigger(TriggerRegistration {
            key,
            trigger_definition: trigger(ErrorBehavior::Log, true),
            cancel: CancellationToken::new(),
            config: SchedulerConfig::new(4, true, NonZeroUsize::MAX),
            auto_disable: noop_auto_disable(),
        })
        .await;

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        for _ in 0..12 {
            scheduler
                .enqueue(TriggerInvocation::new(key, payload()))
                .await
                .unwrap();
        }
    })
    .await
    .expect("all invocations should be admitted without blocking");
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while worker.attempts() < 12 {
            worker.started.notified().await;
        }
    })
    .await
    .expect("all admitted invocations should be dispatched");
    tokio::task::yield_now().await;
    assert_eq!(worker.attempts(), 12);
    assert_eq!(worker.max_active(), 12);
    worker.release.notify_waiters();
    scheduler.shutdown_trigger(key).await;
}

#[tokio::test]
async fn async_limit_above_queue_size_raises_outstanding_capacity() {
    let worker = FakeWorker::blocking();
    let key = TriggerKey {
        db_id: DbId::new(1),
        trigger_id: TriggerId::new(2),
    };
    let scheduler = Scheduler::new(protocol_node("scheduler"), worker.factory());
    scheduler
        .register_trigger(TriggerRegistration {
            key,
            trigger_definition: trigger(ErrorBehavior::Log, true),
            cancel: CancellationToken::new(),
            config: SchedulerConfig::new(4, true, NonZeroUsize::new(8).unwrap()),
            auto_disable: noop_auto_disable(),
        })
        .await;

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        for _ in 0..8 {
            scheduler
                .enqueue(TriggerInvocation::new(key, payload()))
                .await
                .unwrap();
        }
    })
    .await
    .expect("a limit above the queue size should raise outstanding capacity");
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while worker.attempts() < 8 {
            worker.started.notified().await;
        }
    })
    .await
    .expect("all admitted invocations should be dispatched");
    tokio::task::yield_now().await;
    assert_eq!(worker.attempts(), 8);
    assert_eq!(worker.max_active(), 8);
    worker.release.notify_waiters();
    scheduler.shutdown_trigger(key).await;
}

#[tokio::test]
async fn async_concurrency_limit_applies_per_trigger() {
    let worker = FakeWorker::blocking();
    let scheduler = Scheduler::new(protocol_node("scheduler"), worker.factory());
    let trigger_definition = trigger(ErrorBehavior::Log, true);
    let key_a = TriggerKey {
        db_id: DbId::new(1),
        trigger_id: TriggerId::new(2),
    };
    let key_b = TriggerKey {
        db_id: DbId::new(1),
        trigger_id: TriggerId::new(3),
    };
    for key in [key_a, key_b] {
        scheduler
            .register_trigger(TriggerRegistration {
                key,
                trigger_definition: Arc::clone(&trigger_definition),
                cancel: CancellationToken::new(),
                config: SchedulerConfig::new(16, true, NonZeroUsize::new(2).unwrap()),
                auto_disable: noop_auto_disable(),
            })
            .await;
    }

    for key in [key_a, key_b] {
        for _ in 0..4 {
            scheduler
                .enqueue(TriggerInvocation::new(key, payload()))
                .await
                .unwrap();
        }
    }
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while worker.attempts() < 4 {
            worker.started.notified().await;
        }
    })
    .await
    .expect("each trigger should dispatch up to its own limit");
    tokio::task::yield_now().await;
    assert_eq!(worker.attempts(), 4);
    assert_eq!(worker.max_active(), 4);
    worker.release.notify_waiters();
    scheduler.shutdown_trigger(key_a).await;
    scheduler.shutdown_trigger(key_b).await;
}

#[tokio::test]
async fn sync_trigger_ignores_async_concurrency_limit() {
    let worker = FakeWorker::blocking();
    let key = TriggerKey {
        db_id: DbId::new(1),
        trigger_id: TriggerId::new(2),
    };
    let scheduler = Scheduler::new(protocol_node("scheduler"), worker.factory());
    scheduler
        .register_trigger(TriggerRegistration {
            key,
            trigger_definition: trigger(ErrorBehavior::Log, false),
            cancel: CancellationToken::new(),
            config: SchedulerConfig::new(16, false, NonZeroUsize::new(5).unwrap()),
            auto_disable: noop_auto_disable(),
        })
        .await;

    for _ in 0..3 {
        scheduler
            .enqueue(TriggerInvocation::new(key, payload()))
            .await
            .unwrap();
    }
    for expected in 1..=3 {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while worker.attempts() < expected {
                worker.started.notified().await;
            }
        })
        .await
        .expect("invocation should start");
        tokio::task::yield_now().await;
        assert_eq!(worker.attempts(), expected);
        assert_eq!(worker.max_active(), 1);
        worker.release.notify_waiters();
    }
    scheduler.shutdown_trigger(key).await;
}

#[tokio::test(start_paused = true)]
async fn retrying_invocation_does_not_block_dispatch_when_unlimited() {
    let worker = FakeWorker::new(1);
    let key = TriggerKey {
        db_id: DbId::new(1),
        trigger_id: TriggerId::new(2),
    };
    let scheduler = Scheduler::new(protocol_node("scheduler"), worker.factory());
    scheduler
        .register_trigger(TriggerRegistration {
            key,
            trigger_definition: trigger(ErrorBehavior::Retry, true),
            cancel: CancellationToken::new(),
            config: SchedulerConfig::new(4, true, NonZeroUsize::MAX),
            auto_disable: noop_auto_disable(),
        })
        .await;

    // First invocation fails and enters retry backoff, holding its
    // outstanding permit for the duration.
    scheduler
        .enqueue(TriggerInvocation::new(key, payload()))
        .await
        .unwrap();
    while worker.attempts() < 1 {
        worker.started.notified().await;
    }

    // With no concurrency limit, a second invocation dispatches while the
    // first is still waiting out its backoff (paused time has not advanced).
    scheduler
        .enqueue(TriggerInvocation::new(key, payload()))
        .await
        .unwrap();
    while worker.attempts() < 2 {
        worker.started.notified().await;
    }
    assert_eq!(worker.attempts(), 2);

    tokio::time::advance(std::time::Duration::from_millis(100)).await;
    while worker.attempts() < 3 {
        worker.started.notified().await;
    }
    assert_eq!(worker.attempts(), 3);
    scheduler.shutdown_trigger(key).await;
}
