use crate::scheduler_worker_protocol::{
    RequestTriggerWork, TriggerExecutionError, TriggerResponse, TriggerScheduler, TriggerWork,
    TriggerWorkId, TriggerWorkOutput, TriggerWorkPayload, TriggerWorkResult, TriggerWorker,
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::{StreamExt, stream::FuturesUnordered};
use hashbrown::HashMap;
use hyper::{
    StatusCode,
    http::{HeaderName, HeaderValue},
};
use influxdb3_catalog::catalog::{ErrorBehavior, TriggerDefinition};
use influxdb3_id::{DbId, TriggerId};
use influxdb3_py_api::wal::WalFlushElement;
use iox_http_util::{Response, ResponseBuilder, bytes_to_response_body};
use observability_deps::tracing::{debug, error, info, warn};
use parking_lot::Mutex;
use std::collections::{HashSet, VecDeque};
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

const SCHEDULER_INPUT_BUFFER_SIZE: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TriggerKey {
    pub(crate) db_id: DbId,
    pub(crate) trigger_id: TriggerId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct TriggerRuntimeId(u64);

impl TriggerRuntimeId {
    fn next() -> Self {
        static NEXT_RUNTIME_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        Self(NEXT_RUNTIME_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }
}

#[derive(Clone)]
pub(crate) struct RequestPayload {
    pub(crate) query_params: Arc<HashMap<String, String>>,
    pub(crate) headers: Arc<HashMap<String, String>>,
    pub(crate) body: Bytes,
    response_tx: Arc<Mutex<Option<oneshot::Sender<Response>>>>,
}

impl Debug for RequestPayload {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestPayload")
            .field("query_params", &self.query_params)
            .field("headers", &self.headers)
            .field("body_len", &self.body.len())
            .field("response_pending", &self.response_tx.lock().is_some())
            .finish()
    }
}

impl RequestPayload {
    pub(crate) fn new(
        query_params: HashMap<String, String>,
        headers: HashMap<String, String>,
        body: Bytes,
        response_tx: oneshot::Sender<Response>,
    ) -> Self {
        Self {
            query_params: Arc::new(query_params),
            headers: Arc::new(headers),
            body,
            response_tx: Arc::new(Mutex::new(Some(response_tx))),
        }
    }

    pub(crate) fn send_response(&self, response: Response) -> bool {
        let Some(tx) = self.response_tx.lock().take() else {
            return false;
        };
        tx.send(response).is_ok()
    }

    pub(crate) fn send_json_error(&self, status: StatusCode, message: impl AsRef<str>) {
        let body = serde_json::json!({"error": message.as_ref()}).to_string();
        match ResponseBuilder::new()
            .status(status)
            .body(bytes_to_response_body(body))
        {
            Ok(response) => {
                if !self.send_response(response) {
                    debug!("request trigger response already sent");
                }
            }
            Err(error) => {
                error!(%error, "building request trigger error response");
            }
        }
    }

    fn send_cancelled(&self) {
        self.send_json_error(StatusCode::SERVICE_UNAVAILABLE, "server is shutting down");
    }

    fn send_failure(&self, error: &TriggerExecutionError) {
        self.send_json_error(StatusCode::INTERNAL_SERVER_ERROR, error.message());
    }

    fn send_work_response(&self, response: TriggerResponse) {
        let status =
            StatusCode::from_u16(response.status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let mut builder = ResponseBuilder::new().status(status);
        for (key, value) in response.headers {
            let Ok(name) = HeaderName::from_bytes(key.as_bytes()) else {
                warn!(header_name = %key, "dropping response header with invalid name");
                continue;
            };
            let Ok(value) = HeaderValue::from_str(&value) else {
                warn!(header_name = %key, "dropping response header with invalid value");
                continue;
            };
            builder = builder.header(name, value);
        }

        match builder.body(bytes_to_response_body(response.body)) {
            Ok(response) => {
                if !self.send_response(response) {
                    debug!("request trigger response already sent");
                }
            }
            Err(error) => {
                error!(%error, "building request trigger response");
                self.send_json_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "building request trigger response failed",
                );
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum TriggerPayload {
    Wal {
        database_name: Arc<str>,
        wal_contents: Arc<[WalFlushElement]>,
    },
    Schedule {
        scheduled_at: DateTime<Utc>,
    },
    Request(RequestPayload),
}

impl TriggerPayload {
    pub(crate) fn send_cancelled(&self) {
        if let Self::Request(request) = self {
            request.send_cancelled();
        }
    }

    pub(crate) fn send_failure(&self, error: &TriggerExecutionError) {
        if let Self::Request(request) = self {
            request.send_failure(error);
        }
    }

    fn to_work_payload(&self) -> TriggerWorkPayload {
        match self {
            Self::Wal {
                database_name,
                wal_contents,
            } => TriggerWorkPayload::Wal {
                database_name: Arc::clone(database_name),
                wal_contents: Arc::clone(wal_contents),
            },
            Self::Schedule { scheduled_at } => TriggerWorkPayload::Schedule {
                scheduled_at: *scheduled_at,
            },
            Self::Request(request) => TriggerWorkPayload::Request(RequestTriggerWork {
                query_params: request.query_params.as_ref().clone(),
                headers: request.headers.as_ref().clone(),
                body: request.body.clone(),
            }),
        }
    }
}

pub(crate) type AutoDisableFuture = Pin<Box<dyn Future<Output = bool> + Send>>;
pub(crate) type AutoDisable = Arc<dyn Fn() -> AutoDisableFuture + Send + Sync>;

pub(crate) struct TriggerRegistration {
    pub(crate) key: TriggerKey,
    pub(crate) trigger_definition: Arc<TriggerDefinition>,
    pub(crate) cancel: CancellationToken,
    pub(crate) config: SchedulerConfig,
    pub(crate) auto_disable: AutoDisable,
}

struct TriggerRuntime {
    id: TriggerRuntimeId,
    key: TriggerKey,
    trigger_definition: Arc<TriggerDefinition>,
    cancel: CancellationToken,
    config: SchedulerConfig,
    auto_disable: AutoDisable,
    capacity: Arc<Semaphore>,
}

impl Debug for TriggerRuntime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TriggerRuntime")
            .field("id", &self.id)
            .field("key", &self.key)
            .field("trigger_name", &self.trigger_definition.trigger_name)
            .field("is_cancelled", &self.cancel.is_cancelled())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TriggerInvocation {
    pub(crate) key: TriggerKey,
    pub(crate) payload: TriggerPayload,
}

impl TriggerInvocation {
    pub(crate) fn new(key: TriggerKey, payload: TriggerPayload) -> Self {
        Self { key, payload }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RetryPolicy {
    max_attempts: usize,
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
        }
    }
}

impl RetryPolicy {
    fn max_attempts(&self) -> usize {
        self.max_attempts
    }

    fn delay_after_attempt(&self, completed_attempt: usize) -> Duration {
        let multiplier = 1u32
            .checked_shl(completed_attempt.saturating_sub(1) as u32)
            .unwrap_or(u32::MAX);
        self.initial_backoff
            .saturating_mul(multiplier)
            .min(self.max_backoff)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SchedulerConfig {
    queue_size: usize,
    max_concurrency: usize,
    retry_policy: RetryPolicy,
}

impl SchedulerConfig {
    pub(crate) fn new(
        queue_size: usize,
        run_async: bool,
        async_concurrency_limit: NonZeroUsize,
    ) -> Self {
        // Async triggers may run up to `async_concurrency_limit` invocations
        // concurrently (`NonZeroUsize::MAX` means effectively unlimited); a limit
        // above the nominal queue size also raises the outstanding-invocations
        // capacity so the limit is reachable. Sync triggers are always serial.
        let (queue_size, max_concurrency) = if run_async {
            let limit = async_concurrency_limit.get();
            (queue_size.max(limit).min(Semaphore::MAX_PERMITS), limit)
        } else {
            (queue_size, 1)
        };
        Self {
            queue_size,
            max_concurrency,
            retry_policy: RetryPolicy::default(),
        }
    }

    pub(crate) fn max_concurrency(&self) -> usize {
        self.max_concurrency
    }
}

#[derive(Clone)]
pub(crate) struct Scheduler {
    node_id: Arc<str>,
    scheduler: Arc<dyn TriggerScheduler>,
    enqueue_sender: mpsc::Sender<QueuedInvocation>,
    sender: mpsc::UnboundedSender<SchedulerCommand>,
    triggers: Arc<Mutex<HashMap<TriggerKey, Arc<TriggerRuntime>>>>,
}

impl Debug for Scheduler {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scheduler")
            .field("node_id", &self.node_id)
            .field("scheduler_node_id", &self.scheduler.node_id())
            .finish_non_exhaustive()
    }
}

impl Scheduler {
    pub(crate) fn new(
        node_id: Arc<str>,
        make_workers: impl FnOnce(Arc<dyn TriggerScheduler>) -> Vec<Arc<dyn TriggerWorker>>,
    ) -> Self {
        let (enqueue_sender, enqueue_receiver) = mpsc::channel(SCHEDULER_INPUT_BUFFER_SIZE);
        let (sender, receiver) = mpsc::unbounded_channel();
        let scheduler: Arc<dyn TriggerScheduler> = Arc::new(LocalTriggerScheduler {
            node_id: Arc::clone(&node_id),
            sender: sender.downgrade(),
        });
        let workers = make_workers(Arc::clone(&scheduler));
        assert!(
            !workers.is_empty(),
            "processing engine scheduler requires at least one trigger worker"
        );
        let worker_node_ids: HashSet<_> = workers.iter().map(|worker| worker.node_id()).collect();
        assert_eq!(
            worker_node_ids.len(),
            workers.len(),
            "processing engine scheduler requires uniquely identified trigger workers"
        );
        tokio::spawn(
            SchedulerRuntime::new(Arc::clone(&node_id), workers, enqueue_receiver, receiver).run(),
        );
        Self {
            node_id,
            scheduler,
            enqueue_sender,
            sender,
            triggers: Default::default(),
        }
    }

    pub(crate) async fn register_trigger(&self, registration: TriggerRegistration) {
        let runtime = Self::runtime_from_registration(registration);
        let old_runtime = self
            .triggers
            .lock()
            .insert(runtime.key, Arc::clone(&runtime));
        if let Some(old_runtime) = old_runtime {
            self.shutdown_runtime(old_runtime).await;
        }
    }

    fn runtime_from_registration(registration: TriggerRegistration) -> Arc<TriggerRuntime> {
        Arc::new(TriggerRuntime {
            id: TriggerRuntimeId::next(),
            key: registration.key,
            trigger_definition: registration.trigger_definition,
            cancel: registration.cancel,
            capacity: Arc::new(Semaphore::new(registration.config.queue_size)),
            config: registration.config,
            auto_disable: registration.auto_disable,
        })
    }

    pub(crate) async fn enqueue(
        &self,
        invocation: TriggerInvocation,
    ) -> Result<(), mpsc::error::SendError<TriggerInvocation>> {
        let runtime = self.triggers.lock().get(&invocation.key).cloned();
        let Some(runtime) = runtime else {
            return Err(mpsc::error::SendError(invocation));
        };
        if runtime.cancel.is_cancelled() {
            return Err(mpsc::error::SendError(invocation));
        }

        let permit = tokio::select! {
            _ = runtime.cancel.cancelled() => None,
            permit = Arc::clone(&runtime.capacity).acquire_owned() => permit.ok(),
        };
        let Some(permit) = permit else {
            return Err(mpsc::error::SendError(invocation));
        };
        if runtime.cancel.is_cancelled() {
            return Err(mpsc::error::SendError(invocation));
        }

        self.enqueue_sender
            .send(QueuedInvocation {
                runtime,
                invocation,
                attempt: 1,
                _permit: permit,
            })
            .await
            .map_err(|error| mpsc::error::SendError(error.0.invocation))
    }

    pub(crate) async fn shutdown_trigger(&self, key: TriggerKey) {
        let runtime = self.triggers.lock().remove(&key);
        if let Some(runtime) = runtime {
            self.shutdown_runtime(runtime).await;
        }
    }

    pub(crate) async fn shutdown_triggers_for_db(&self, db_id: DbId) {
        let keys: Vec<_> = self
            .triggers
            .lock()
            .keys()
            .filter(|key| key.db_id == db_id)
            .copied()
            .collect();
        for key in keys {
            self.shutdown_trigger(key).await;
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn shutdown_all(&self) {
        let keys: Vec<_> = self.triggers.lock().keys().copied().collect();
        for key in keys {
            self.shutdown_trigger(key).await;
        }
    }

    async fn shutdown_runtime(&self, runtime: Arc<TriggerRuntime>) {
        runtime.cancel.cancel();
        let (done, done_rx) = oneshot::channel();
        let command = SchedulerCommand::Shutdown {
            id: runtime.id,
            key: runtime.key,
            done,
        };
        if self.sender.send(command).is_err() {
            return;
        }
        if let Err(error) = done_rx.await {
            warn!(%error, ?runtime.key, "scheduler shutdown acknowledgement failed");
        }
    }
}

enum SchedulerCommand {
    WorkerFinished {
        worker_node_id: Arc<str>,
        result: TriggerWorkResult,
    },
    WorkerProgressed {
        worker_node_id: Arc<str>,
        work_id: TriggerWorkId,
    },
    Shutdown {
        id: TriggerRuntimeId,
        key: TriggerKey,
        done: oneshot::Sender<()>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct RetryDelayId(u64);

impl RetryDelayId {
    fn next() -> Self {
        static NEXT_RETRY_DELAY_ID: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(1);
        Self(NEXT_RETRY_DELAY_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }
}

struct QueuedInvocation {
    runtime: Arc<TriggerRuntime>,
    invocation: TriggerInvocation,
    attempt: usize,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

struct RunningInvocation {
    runtime: Arc<TriggerRuntime>,
    invocation: TriggerInvocation,
    attempt: usize,
    worker_node_id: Arc<str>,
    worker: Arc<dyn TriggerWorker>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

type RetryDelayFuture = Pin<Box<dyn Future<Output = RetryDelayResult> + Send>>;

struct RetryDelayResult {
    id: RetryDelayId,
    cancelled: bool,
}

type PendingAutoDisableFuture = Pin<Box<dyn Future<Output = AutoDisableResult> + Send>>;

struct AutoDisableResult {
    running: RunningInvocation,
    disabled: Option<bool>,
}

struct LocalTriggerScheduler {
    node_id: Arc<str>,
    sender: mpsc::WeakUnboundedSender<SchedulerCommand>,
}

impl TriggerScheduler for LocalTriggerScheduler {
    fn node_id(&self) -> Arc<str> {
        Arc::clone(&self.node_id)
    }

    fn work_progressed(&self, worker_node_id: Arc<str>, work_id: TriggerWorkId) {
        let Some(sender) = self.sender.upgrade() else {
            return;
        };
        if sender
            .send(SchedulerCommand::WorkerProgressed {
                worker_node_id: Arc::clone(&worker_node_id),
                work_id,
            })
            .is_err()
        {
            warn!(
                ?worker_node_id,
                ?work_id,
                "scheduler worker progress callback failed; scheduler stopped"
            );
        }
    }

    fn work_finished(&self, worker_node_id: Arc<str>, result: TriggerWorkResult) {
        let Some(sender) = self.sender.upgrade() else {
            return;
        };
        if sender
            .send(SchedulerCommand::WorkerFinished {
                worker_node_id,
                result,
            })
            .is_err()
        {
            warn!("scheduler worker completion callback failed; scheduler stopped");
        }
    }
}

struct TriggerState {
    runtime: Arc<TriggerRuntime>,
    queue: VecDeque<QueuedInvocation>,
    in_flight: usize,
    shutting_down: bool,
    shutdown_waiters: Vec<oneshot::Sender<()>>,
}

impl TriggerState {
    fn new(runtime: Arc<TriggerRuntime>) -> Self {
        Self {
            runtime,
            queue: VecDeque::new(),
            in_flight: 0,
            shutting_down: false,
            shutdown_waiters: Vec::new(),
        }
    }

    fn drain_cancelled(&mut self) {
        while let Some(queued) = self.queue.pop_front() {
            queued.invocation.payload.send_cancelled();
        }
    }
}

struct SchedulerRuntime {
    node_id: Arc<str>,
    workers: Vec<Arc<dyn TriggerWorker>>,
    next_worker_index: usize,
    enqueue_receiver: mpsc::Receiver<QueuedInvocation>,
    receiver: mpsc::UnboundedReceiver<SchedulerCommand>,
    states: HashMap<TriggerRuntimeId, TriggerState>,
    running: HashMap<TriggerWorkId, RunningInvocation>,
    delayed_retries: HashMap<RetryDelayId, QueuedInvocation>,
    retry_delays: FuturesUnordered<RetryDelayFuture>,
    auto_disables: FuturesUnordered<PendingAutoDisableFuture>,
}

impl SchedulerRuntime {
    fn new(
        node_id: Arc<str>,
        workers: Vec<Arc<dyn TriggerWorker>>,
        enqueue_receiver: mpsc::Receiver<QueuedInvocation>,
        receiver: mpsc::UnboundedReceiver<SchedulerCommand>,
    ) -> Self {
        Self {
            node_id,
            workers,
            next_worker_index: 0,
            enqueue_receiver,
            receiver,
            states: HashMap::new(),
            running: HashMap::new(),
            delayed_retries: HashMap::new(),
            retry_delays: FuturesUnordered::new(),
            auto_disables: FuturesUnordered::new(),
        }
    }

    async fn run(mut self) {
        info!(?self.node_id, worker_count = self.workers.len(), "starting processing engine scheduler");

        loop {
            // incoming "events"
            tokio::select! {
                maybe_queued = self.enqueue_receiver.recv() => {
                    let Some(queued) = maybe_queued else {
                        break;
                    };
                    self.enqueue(queued);
                }
                maybe_command = self.receiver.recv() => {
                    let Some(command) = maybe_command else {
                        break;
                    };
                    self.handle_command(command);
                }
                Some(result) = self.retry_delays.next(), if !self.retry_delays.is_empty() => {
                    self.handle_retry_delay(result);
                }
                Some(result) = self.auto_disables.next(), if !self.auto_disables.is_empty() => {
                    self.handle_auto_disable_result(result);
                }
            }

            // drive states
            self.detach_cancelled_runtimes();
            self.dispatch_ready();
            self.remove_stopped_states();
        }

        self.stop_all();
        info!("processing engine scheduler stopped");
    }

    fn stop_all(&mut self) {
        for state in self.states.values_mut() {
            state.shutting_down = true;
            state.runtime.cancel.cancel();
            state.drain_cancelled();
        }
        let running_work = self.running.keys().copied().collect::<Vec<_>>();
        self.detach_work_ids(running_work);
        for (_, queued) in self.delayed_retries.drain() {
            queued.invocation.payload.send_cancelled();
        }
        for (_, state) in self.states.drain() {
            for waiter in state.shutdown_waiters {
                let _ = waiter.send(());
            }
        }
    }

    fn handle_command(&mut self, command: SchedulerCommand) {
        match command {
            SchedulerCommand::WorkerFinished {
                worker_node_id,
                result,
            } => self.handle_worker_result(worker_node_id, result),
            SchedulerCommand::WorkerProgressed {
                worker_node_id,
                work_id,
            } => self.handle_worker_progress(worker_node_id, work_id),
            SchedulerCommand::Shutdown { id, key, done } => self.shutdown_trigger(id, key, done),
        }
    }

    fn enqueue(&mut self, queued: QueuedInvocation) {
        if queued.runtime.cancel.is_cancelled() {
            queued.invocation.payload.send_cancelled();
            return;
        }

        let state = self
            .states
            .entry(queued.runtime.id)
            .or_insert_with(|| TriggerState::new(Arc::clone(&queued.runtime)));
        if state.shutting_down || state.runtime.cancel.is_cancelled() {
            queued.invocation.payload.send_cancelled();
            return;
        }
        state.queue.push_back(queued);
    }

    fn shutdown_trigger(
        &mut self,
        id: TriggerRuntimeId,
        key: TriggerKey,
        done: oneshot::Sender<()>,
    ) {
        let Some(state) = self.states.get_mut(&id) else {
            let _ = done.send(());
            return;
        };
        debug!(?id, ?key, "scheduler trigger shutdown requested");
        state.shutting_down = true;
        state.runtime.cancel.cancel();
        state.drain_cancelled();
        state.shutdown_waiters.push(done);
    }

    fn detach_cancelled_runtimes(&mut self) {
        let runtime_ids: Vec<_> = self
            .states
            .iter_mut()
            .filter_map(|(id, state)| {
                let cancelled = state.shutting_down || state.runtime.cancel.is_cancelled();
                if !cancelled {
                    return None;
                }
                state.drain_cancelled();
                state.shutting_down = true;
                Some(*id)
            })
            .collect();

        for id in runtime_ids {
            self.detach_running_for_runtime(id);
        }
    }

    fn dispatch_ready(&mut self) {
        let mut ready = Vec::new();
        for state in self.states.values_mut() {
            if state.shutting_down || state.runtime.cancel.is_cancelled() {
                state.shutting_down = true;
                state.drain_cancelled();
                continue;
            }
            while state.in_flight < state.runtime.config.max_concurrency() {
                let Some(queued) = state.queue.pop_front() else {
                    break;
                };
                state.in_flight += 1;
                ready.push(queued);
            }
        }

        for queued in ready {
            self.submit_worker(queued);
        }
    }

    fn next_worker(&mut self) -> Arc<dyn TriggerWorker> {
        let worker = Arc::clone(&self.workers[self.next_worker_index]);
        self.next_worker_index = (self.next_worker_index + 1) % self.workers.len();
        worker
    }

    fn submit_worker(&mut self, queued: QueuedInvocation) {
        let work_id = TriggerWorkId::next();
        let work = TriggerWork {
            id: work_id,
            key: queued.invocation.key,
            payload: queued.invocation.payload.to_work_payload(),
        };
        let worker = self.next_worker();
        let worker_node_id = worker.node_id();

        self.running.insert(
            work_id,
            RunningInvocation {
                runtime: queued.runtime,
                invocation: queued.invocation,
                attempt: queued.attempt,
                worker_node_id,
                worker: Arc::clone(&worker),
                _permit: queued._permit,
            },
        );

        worker.submit_work(Arc::clone(&self.node_id), work);
    }

    fn handle_worker_progress(&mut self, worker_node_id: Arc<str>, work_id: TriggerWorkId) {
        let Some(running) = self.running.get(&work_id) else {
            debug!(?work_id, "stale trigger worker progress ignored");
            return;
        };
        if worker_node_id != running.worker_node_id {
            debug!(?worker_node_id, expected_worker_node_id = ?running.worker_node_id, ?work_id, "trigger worker progress from unexpected worker ignored");
        }
    }

    fn handle_worker_result(&mut self, worker_node_id: Arc<str>, result: TriggerWorkResult) {
        let Some(running) = self.running.get(&result.work_id) else {
            debug!(?result.work_id, "stale trigger worker completion ignored");
            return;
        };
        if worker_node_id != running.worker_node_id {
            debug!(?worker_node_id, expected_worker_node_id = ?running.worker_node_id, ?result.work_id, "trigger worker completion from unexpected worker ignored");
            return;
        }
        let running = self
            .running
            .remove(&result.work_id)
            .expect("running work was checked");
        self.handle_attempt_result(running, result.result);
    }

    fn detach_running_for_runtime(&mut self, id: TriggerRuntimeId) {
        let work_ids: Vec<_> = self
            .running
            .iter()
            .filter_map(|(work_id, running)| (running.runtime.id == id).then_some(*work_id))
            .collect();
        self.detach_work_ids(work_ids);
    }

    fn detach_work_ids(&mut self, work_ids: impl IntoIterator<Item = TriggerWorkId>) {
        for work_id in work_ids {
            let Some(running) = self.running.remove(&work_id) else {
                continue;
            };
            running
                .worker
                .cancel_work(Arc::clone(&self.node_id), work_id);
            running.invocation.payload.send_cancelled();
            self.release_in_flight(running.runtime.id);
        }
    }

    fn handle_attempt_result(
        &mut self,
        running: RunningInvocation,
        result: Result<TriggerWorkOutput, TriggerExecutionError>,
    ) {
        if running.runtime.cancel.is_cancelled() {
            running.invocation.payload.send_cancelled();
            self.release_in_flight(running.runtime.id);
            return;
        }

        match result {
            Ok(output) => {
                self.handle_work_output(&running.invocation.payload, output);
                self.release_in_flight(running.runtime.id);
            }
            Err(error) if error.is_cancelled() => {
                running.invocation.payload.send_cancelled();
                self.release_in_flight(running.runtime.id);
            }
            Err(error) => self.handle_attempt_error(running, error),
        }
    }

    fn handle_work_output(&self, payload: &TriggerPayload, output: TriggerWorkOutput) {
        match (payload, output) {
            (TriggerPayload::Request(request), TriggerWorkOutput::RequestResponse(response)) => {
                request.send_work_response(response);
            }
            (TriggerPayload::Request(request), TriggerWorkOutput::Complete) => {
                request.send_json_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "request trigger worker completed without a response",
                );
            }
            (_, TriggerWorkOutput::RequestResponse(_)) => {
                error!("non-request trigger worker completed with an HTTP response");
            }
            (_, TriggerWorkOutput::Complete) => {}
        }
    }

    fn handle_attempt_error(&mut self, running: RunningInvocation, error: TriggerExecutionError) {
        let error_behavior = running
            .runtime
            .trigger_definition
            .trigger_settings
            .error_behavior;
        match error_behavior {
            ErrorBehavior::Log => {
                error!(%error, ?running.invocation.key, "trigger execution failed");
                running.invocation.payload.send_failure(&error);
                self.release_in_flight(running.runtime.id);
            }
            ErrorBehavior::Disable => {
                warn!(
                    %error,
                    trigger_name = %running.runtime.trigger_definition.trigger_name,
                    "trigger execution failed; disabling trigger"
                );
                running.invocation.payload.send_failure(&error);
                self.schedule_auto_disable(running);
            }
            ErrorBehavior::Retry => {
                let retry_policy = running.runtime.config.retry_policy.clone();
                let max_attempts = retry_policy.max_attempts();
                if running.attempt >= max_attempts {
                    error!(
                        %error,
                        ?running.invocation.key,
                        attempts = running.attempt,
                        "trigger execution failed after retries"
                    );
                    running.invocation.payload.send_failure(&error);
                    self.release_in_flight(running.runtime.id);
                } else {
                    let delay = retry_policy.delay_after_attempt(running.attempt);
                    info!(
                        %error,
                        ?running.invocation.key,
                        attempt = running.attempt,
                        next_attempt = running.attempt + 1,
                        delay_ms = delay.as_millis(),
                        "trigger execution failed; retrying after backoff"
                    );
                    self.schedule_retry(running, delay);
                }
            }
        }
    }

    fn schedule_auto_disable(&mut self, running: RunningInvocation) {
        let auto_disable = Arc::clone(&running.runtime.auto_disable);
        let cancel = running.runtime.cancel.clone();
        let future = Box::pin(async move {
            let disabled = tokio::select! {
                disabled = auto_disable() => Some(disabled),
                _ = cancel.cancelled() => None,
            };
            AutoDisableResult { running, disabled }
        });
        self.auto_disables.push(future);
    }

    fn handle_auto_disable_result(&mut self, result: AutoDisableResult) {
        let running = result.running;
        let runtime_id = running.runtime.id;
        match result.disabled {
            Some(true) => {
                running.runtime.cancel.cancel();
                if let Some(state) = self.states.get_mut(&runtime_id) {
                    state.shutting_down = true;
                    state.drain_cancelled();
                }
            }
            Some(false) => {
                warn!(
                    trigger_name = %running.runtime.trigger_definition.trigger_name,
                    "trigger auto-disable failed; leaving trigger active"
                );
            }
            None => {
                debug!(
                    trigger_name = %running.runtime.trigger_definition.trigger_name,
                    "trigger auto-disable abandoned because runtime was cancelled"
                );
            }
        }
        self.release_in_flight(runtime_id);
    }

    fn schedule_retry(&mut self, running: RunningInvocation, delay: Duration) {
        let queued = QueuedInvocation {
            runtime: running.runtime,
            invocation: running.invocation,
            attempt: running.attempt + 1,
            _permit: running._permit,
        };
        let id = RetryDelayId::next();
        let cancel = queued.runtime.cancel.clone();
        self.delayed_retries.insert(id, queued);
        let future = Box::pin(async move {
            let cancelled = tokio::select! {
                _ = tokio::time::sleep(delay) => false,
                _ = cancel.cancelled() => true,
            };
            RetryDelayResult { id, cancelled }
        });
        self.retry_delays.push(future);
    }

    fn handle_retry_delay(&mut self, result: RetryDelayResult) {
        let Some(queued) = self.delayed_retries.remove(&result.id) else {
            debug!(?result.id, "stale trigger retry delay ignored");
            return;
        };
        let runtime_id = queued.runtime.id;
        let should_cancel = result.cancelled
            || queued.runtime.cancel.is_cancelled()
            || self
                .states
                .get(&runtime_id)
                .is_none_or(|state| state.shutting_down || state.runtime.cancel.is_cancelled());
        if should_cancel {
            queued.invocation.payload.send_cancelled();
            self.release_in_flight(runtime_id);
            return;
        }

        self.submit_worker(queued);
    }

    fn release_in_flight(&mut self, id: TriggerRuntimeId) {
        if let Some(state) = self.states.get_mut(&id) {
            state.in_flight = state.in_flight.saturating_sub(1);
        }
    }

    fn remove_stopped_states(&mut self) {
        self.states
            .extract_if(|_id, state| state.shutting_down && state.in_flight == 0)
            .for_each(|(id, state)| {
                for waiter in state.shutdown_waiters {
                    let _ = waiter.send(());
                }
                info!(?id, ?state.runtime.key, "trigger scheduler state stopped");
            });
    }
}
#[cfg(test)]
mod tests;
