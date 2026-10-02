//! Phase tracking for node startup.
//!
//! [`StartupPhases`] issues one [`PhaseGuard`] per phase. The guard logs a start line when it is
//! created and a completion line when the phase succeeds or fails. Completion events also feed a
//! [`StartupPhaseObserver`] so they reach the service log; start lines go to tracing only, so do
//! not rely on a service-log start event.
//!
//! Both destinations are deliberate. Tracing writes through line-buffered stdout, so a line reaches
//! the file descriptor as it completes and survives a SIGKILL; the service log hands entries to a
//! background writer thread, so an entry can still be in memory when the process dies. A node that
//! is OOM-killed mid-phase keeps its tracing lines and may lose its service log entries, so the two
//! are not redundant.

use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use observability_deps::tracing::{error, info};
// The startup path threads a `tokio::time::Instant` as its origin, so match that rather than
// converting at every call site. It also keeps these durations honest under a paused test clock.
use tokio::time::Instant;

/// A named startup phase the enterprise serve path emits SLL events for.
#[derive(Debug, Clone, Copy)]
pub enum StartupPhase {
    /// The throwaway catalog load `command` runs on the temporary runtime to look up the
    /// instance id and the persisted storage mode. Finishes before logging is initialised, so it
    /// is reported after the fact via `StartupPhases::report_completed`.
    /// Success detail: `storage_mode=<parquet|pacha_tree>`.
    TempCatalogLoad,
    /// Licensing init on the temporary runtime. The span covers the whole licensing bootstrap,
    /// including its own catalog access and onboarding telemetry, so boot wall-clock stays fully
    /// attributed. Also reported after the fact; never reported on `no_license` builds.
    /// Success detail: `licensed_cores=<n>`.
    Licensing,
    /// Load of the persisted catalog. Success detail: `uuid_<catalog uuid>`.
    CatalogLoad,
    /// Load (or first-boot create) of the persisted `EnterpriseConfig`.
    /// Success detail: `loaded` or `created_default`.
    EnterpriseConfig,
    /// Background table-index-cache initialization on ingest-capable nodes. Runs concurrently
    /// with later phases, so its lines interleave with theirs.
    /// Success detail: `snapshots=<split> entries=<held>`.
    TableIndexCache,
    /// Load of compacted data: the producer's state on compact nodes, or the consumer's copy on
    /// other modes when a compactor node is running.
    /// Success detail: `tables=<n> generations=<n>`, plus ` retries=<n>` for the consumer.
    CompactedDataLoad,
    /// Restore of persisted snapshots into the write buffer. Success detail:
    /// `checkpoints=<n> additional_snapshots=<n>`, `snapshots=<n>`, or `skipped`.
    SnapshotRestore,
    /// Replay of WAL files written since the last snapshot. Success detail:
    /// `no_wal files=<n>` or `replayed_through_seq_<seq> files=<n>`.
    WalReplay,
    /// Binding the HTTP listener and, when configured, the internode listener.
    /// Success detail: `internode_bound` or `http_only` (addresses stay out of the service log).
    ListenerBind,
    /// Creation of replicated buffers for every ingest peer on query-capable nodes.
    /// Success detail: `peers=<n>`.
    ReplicaBootstrap,
    /// Warm-up of the last-value and distinct-value caches.
    /// Success detail: `both_caches`, `lvc_only`, `dvc_only`, or `skipped`.
    CacheWarm,
    /// Registration of this node in the catalog. Success detail: `instance_<instance id>`.
    NodeRegistration,
    /// Processing engine setup and trigger start on process-capable nodes.
    /// Success detail: `triggers_started`.
    ProcessingEngine,
    /// Terminal phase: the node is serving. See [`StartupPhases::ready`].
    /// Detail: `listening` (the address is on the adjacent startup-time tracing line only).
    Ready,
}

impl StartupPhase {
    /// The snake_case name this phase carries on log lines and SLL events.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TempCatalogLoad => "temp_catalog_load",
            Self::Licensing => "licensing",
            Self::CatalogLoad => "catalog_load",
            Self::EnterpriseConfig => "enterprise_config",
            Self::TableIndexCache => "table_index_cache",
            Self::CompactedDataLoad => "compacted_data_load",
            Self::SnapshotRestore => "snapshot_restore",
            Self::WalReplay => "wal_replay",
            Self::ListenerBind => "listener_bind",
            Self::ReplicaBootstrap => "replica_bootstrap",
            Self::CacheWarm => "cache_warm",
            Self::NodeRegistration => "node_registration",
            Self::ProcessingEngine => "processing_engine",
            Self::Ready => "ready",
        }
    }
}

/// Subscriber for per-phase startup completion events. Fires once per
/// phase per node boot. Success carries a per-phase `detail` summary;
/// error carries a static `error_code` for the phase that failed.
/// Node-scoped.
pub trait StartupPhaseObserver: Send + Sync + Debug {
    /// Called when `phase` completes successfully.
    fn on_phase_success(&self, phase: StartupPhase, duration_ms: u64, detail: String);
    /// Called when `phase` fails with `error_code`.
    fn on_phase_error(&self, phase: StartupPhase, error_code: &'static str, duration_ms: u64);
}

/// No-op observer used when no subscriber is wired in.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopStartupPhaseObserver;

impl StartupPhaseObserver for NoopStartupPhaseObserver {
    fn on_phase_success(&self, _: StartupPhase, _: u64, _: String) {}
    fn on_phase_error(&self, _: StartupPhase, _: &'static str, _: u64) {}
}

/// Issues one [`PhaseGuard`] per startup phase.
///
/// `process_start` should be the same instant used to report total startup time, so that the
/// `elapsed_total_ms` on every phase line is measured against one origin.
#[derive(Debug, Clone)]
pub struct StartupPhases {
    observer: Arc<dyn StartupPhaseObserver>,
    process_start: Instant,
}

impl StartupPhases {
    /// Build a tracker that feeds `observer` and measures every `elapsed_total_ms` from
    /// `process_start`.
    pub fn new(observer: Arc<dyn StartupPhaseObserver>, process_start: Instant) -> Self {
        Self {
            observer,
            process_start,
        }
    }

    /// A tracker that discards every event, for tests and for paths with no subscriber.
    ///
    /// Each call takes its own origin, so `elapsed_total_ms` is only meaningful within one
    /// instance. Production code builds one [`StartupPhases`] from the process start instant and
    /// shares it, so that every phase measures against the same origin.
    pub fn noop() -> Self {
        Self::new(Arc::new(NoopStartupPhaseObserver), Instant::now())
    }

    /// Start `phase` and log what it is about to do.
    ///
    /// `plan` describes the work in terms already known at this point — counts, configured
    /// concurrency — and is logged verbatim. Pass an empty string when there is nothing useful to
    /// say up front. The guard must be finished with [`PhaseGuard::success`] or
    /// [`PhaseGuard::error`]; dropping it without either logs the phase as incomplete.
    pub fn begin(&self, phase: StartupPhase, plan: impl AsRef<str>) -> PhaseGuard<'_> {
        let plan = plan.as_ref();
        info!(
            startup_phase = phase.as_str(),
            elapsed_total_ms = self.elapsed_total_ms(),
            plan,
            "startup phase started"
        );
        PhaseGuard {
            phases: self,
            phase,
            started: Instant::now(),
            finished: AtomicBool::new(false),
        }
    }

    /// Report a phase that already ran to completion, for work that ran before logging was
    /// initialised (the temp-catalog load and licensing init run on a temporary runtime before
    /// tracing exists). Emits the started and finished lines a [`PhaseGuard`] would have emitted,
    /// using the recorded instants, and feeds the observer one success event.
    ///
    /// `started` may predate this tracker's origin: the origin stays tied to the reported total
    /// startup time, so `elapsed_total_ms` saturates to zero instead of moving that origin.
    /// Failures need no counterpart here — a failure in this pre-logging work aborts the boot
    /// before any tracker exists.
    pub fn report_completed(
        &self,
        phase: StartupPhase,
        started: Instant,
        finished: Instant,
        detail: impl Into<String>,
    ) {
        let detail = detail.into();
        info!(
            startup_phase = phase.as_str(),
            elapsed_total_ms = self.elapsed_total_ms_at(started),
            plan = "",
            "startup phase started"
        );
        let duration_ms = finished.saturating_duration_since(started).as_millis() as u64;
        info!(
            startup_phase = phase.as_str(),
            duration_ms,
            elapsed_total_ms = self.elapsed_total_ms_at(finished),
            detail = detail.as_str(),
            "startup phase finished"
        );
        self.observer.on_phase_success(phase, duration_ms, detail);
    }

    /// Report the terminal `ready` phase.
    ///
    /// Unlike the other phases this is a point event, not a span: its duration is the whole boot,
    /// measured from the same origin as every `elapsed_total_ms` above it. Call it once, after the
    /// last fallible setup step, so an aborted boot never reports ready.
    pub fn ready(&self, detail: impl Into<String>) {
        let detail = detail.into();
        let duration_ms = self.elapsed_total_ms();
        info!(
            startup_phase = StartupPhase::Ready.as_str(),
            duration_ms,
            detail = detail.as_str(),
            "startup finished"
        );
        self.observer
            .on_phase_success(StartupPhase::Ready, duration_ms, detail);
    }

    fn elapsed_total_ms(&self) -> u64 {
        self.process_start.elapsed().as_millis() as u64
    }

    /// Milliseconds from the origin to `at`, saturating to zero when `at` predates the origin.
    fn elapsed_total_ms_at(&self, at: Instant) -> u64 {
        at.saturating_duration_since(self.process_start).as_millis() as u64
    }
}

/// One in-progress startup phase. See [`StartupPhases::begin`].
#[derive(Debug)]
pub struct PhaseGuard<'a> {
    phases: &'a StartupPhases,
    phase: StartupPhase,
    started: Instant,
    finished: AtomicBool,
}

impl PhaseGuard<'_> {
    /// Record that the phase completed. `detail` is the machine-readable per-phase summary that
    /// reaches the service log, and must contain no names, query text or other user data.
    pub fn success(&self, detail: impl Into<String>) {
        if self.finish() {
            return;
        }
        let detail = detail.into();
        let duration_ms = self.duration_ms();
        info!(
            startup_phase = self.phase.as_str(),
            duration_ms,
            elapsed_total_ms = self.phases.elapsed_total_ms(),
            detail = detail.as_str(),
            "startup phase finished"
        );
        self.phases
            .observer
            .on_phase_success(self.phase, duration_ms, detail);
    }

    /// Record that the phase failed. Takes `&self` so it composes with `inspect_err` ahead of the
    /// `?` that propagates the original error.
    pub fn error(&self, error_code: &'static str) {
        if self.finish() {
            return;
        }
        let duration_ms = self.duration_ms();
        error!(
            startup_phase = self.phase.as_str(),
            duration_ms,
            elapsed_total_ms = self.phases.elapsed_total_ms(),
            error_code,
            "startup phase failed"
        );
        self.phases
            .observer
            .on_phase_error(self.phase, error_code, duration_ms);
    }

    fn duration_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    /// Mark the guard finished. Returns true if it was already finished, in which case the caller
    /// must emit nothing: a phase reports exactly once.
    fn finish(&self) -> bool {
        self.finished.swap(true, Ordering::Relaxed)
    }
}

impl Drop for PhaseGuard<'_> {
    fn drop(&mut self) {
        if *self.finished.get_mut() {
            return;
        }
        // Reached when a phase returns early without reporting. Logged rather than ignored so the
        // gap is visible instead of the phase silently never finishing.
        error!(
            startup_phase = self.phase.as_str(),
            duration_ms = self.duration_ms(),
            elapsed_total_ms = self.phases.elapsed_total_ms(),
            "startup phase did not report an outcome"
        );
    }
}

#[cfg(test)]
mod tests;
