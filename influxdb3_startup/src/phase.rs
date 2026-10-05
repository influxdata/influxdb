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
use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use observability_deps::tracing::{error, info};
// The startup path threads a `tokio::time::Instant` as its origin, so match that rather than
// converting at every call site. It also keeps these durations honest under a paused test clock.
use tokio::time::Instant;

/// A named startup phase the enterprise serve path emits SLL events for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// Success detail: `loaded bytes=<n>` or `created_default`.
    EnterpriseConfig,
    /// Background table-index-cache initialization on ingest-capable nodes. Runs concurrently
    /// with later phases, so its lines interleave with theirs.
    /// Success detail: `snapshots=<split> split_bytes=<listed bytes split> entries=<held>`.
    TableIndexCache,
    /// Load of compacted data: the producer's state on compact nodes, or the consumer's copy on
    /// other modes when a compactor node is running.
    /// Success detail: `tables=<n> generations=<n> files=<n> bytes=<n>`, plus ` retries=<n>`
    /// for the consumer.
    CompactedDataLoad,
    /// Restore of persisted snapshots into the write buffer. Success detail: `skipped`,
    /// `checkpoints=<n> checkpoint_bytes=<n> additional_snapshots=<n>`, or `snapshots=<n>`,
    /// followed by
    /// `files=<tracked> size_mb=<total> rows=<n> wal_seq=<n|none> snapshot_seq=<n|none>`.
    SnapshotRestore,
    /// Replay of WAL files written since the last snapshot. Success detail: `no_wal` or
    /// `replayed_through_seq_<seq>`, followed by
    /// `files=<n> ops=<n> bytes=<n> skipped=<n> snapshots=<n>`.
    WalReplay,
    /// Binding the HTTP listener and, when configured, the internode listener.
    /// Success detail: `internode_bound` or `http_only` (addresses stay out of the service log).
    ListenerBind,
    /// Creation of replicated buffers for every ingest peer on query-capable nodes.
    /// Success detail: `peers=<n> wal_files=<n> snapshots=<n>`, summed over the peers.
    ReplicaBootstrap,
    /// Warm-up of the last-value and distinct-value caches.
    /// Success detail: `both_caches`, `lvc_only`, `dvc_only`, or `skipped`, followed by
    /// `lvc_caches=<n> dvc_caches=<n>`.
    CacheWarm,
    /// Registration of this node in the catalog.
    /// Success detail: `instance_<instance id> known_nodes=<n>`.
    NodeRegistration,
    /// Processing engine setup and trigger start on process-capable nodes.
    /// Success detail: `triggers_attempted=<n> triggers_failed=<n>`. A trigger that reports no
    /// error may still not run on this node, so this counts attempts, not running triggers.
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

/// How a recorded phase ended, for the summary line.
#[derive(Debug, Clone, Copy)]
enum PhaseOutcome {
    /// Begun but not finished when the summary was rendered (background phases can outlive boot).
    Pending,
    Success,
    Failed,
    /// The guard was dropped without reporting, so the phase is dead, not still running.
    Incomplete,
}

/// Issues one [`PhaseGuard`] per startup phase.
///
/// `process_start` should be the same instant used to report total startup time, so that the
/// `elapsed_total_ms` on every phase line is measured against one origin.
#[derive(Debug, Clone)]
pub struct StartupPhases {
    observer: Arc<dyn StartupPhaseObserver>,
    process_start: Instant,
    /// Every phase that began or reported an outcome, with its duration, in begin order. Shared by
    /// clones (the tracker is cloned into background tasks) so [`StartupPhases::ready`] can log one
    /// summary line covering all of them.
    recorded: Arc<Mutex<Vec<(StartupPhase, u64, PhaseOutcome)>>>,
}

impl StartupPhases {
    /// Build a tracker that feeds `observer` and measures every `elapsed_total_ms` from
    /// `process_start`.
    pub fn new(observer: Arc<dyn StartupPhaseObserver>, process_start: Instant) -> Self {
        Self {
            observer,
            process_start,
            recorded: Arc::new(Mutex::new(Vec::new())),
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
        self.register_pending(phase);
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
        self.record_outcome(phase, duration_ms, PhaseOutcome::Success);
        self.observer.on_phase_success(phase, duration_ms, detail);
    }

    /// Report the terminal `ready` phase.
    ///
    /// Unlike the other phases this is a point event, not a span: its duration is the whole boot,
    /// measured from the same origin as every `elapsed_total_ms` above it. Call it once, after the
    /// last fallible setup step, so an aborted boot never reports ready.
    ///
    /// Logs the per-phase summary first. Ready itself is deliberately not in that summary: its
    /// duration is the summary's `total_ms`.
    pub fn ready(&self, detail: impl Into<String>) {
        let detail = detail.into();
        let duration_ms = self.elapsed_total_ms();
        info!(
            phases = %self.summary_line(),
            total_ms = duration_ms,
            "startup phase summary"
        );
        info!(
            startup_phase = StartupPhase::Ready.as_str(),
            duration_ms,
            detail = detail.as_str(),
            "startup finished"
        );
        self.observer
            .on_phase_success(StartupPhase::Ready, duration_ms, detail);
    }

    /// Lock the recorded-phase list. Observability must never panic a boot, so a poisoned lock
    /// is entered rather than unwrapped.
    fn lock_recorded(&self) -> MutexGuard<'_, Vec<(StartupPhase, u64, PhaseOutcome)>> {
        self.recorded.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Insert a `Pending` entry for `phase` unless one is already there.
    ///
    /// [`StartupPhases::begin`] calls this, but a phase that begins inside a spawned task can lose
    /// the race against [`StartupPhases::ready`] rendering the summary before the task is first
    /// polled. Call this on the boot thread right before such a spawn, so the summary shows the
    /// phase as `pending` instead of omitting it; the later `begin` updates the same entry.
    pub fn register_pending(&self, phase: StartupPhase) {
        let mut recorded = self.lock_recorded();
        if !recorded
            .iter()
            .any(|(p, _, o)| *p == phase && matches!(o, PhaseOutcome::Pending))
        {
            recorded.push((phase, 0, PhaseOutcome::Pending));
        }
    }

    /// Record `phase` finishing with `outcome`, updating its pending entry, or appending one for
    /// phases reported without a guard ([`StartupPhases::report_completed`]).
    fn record_outcome(&self, phase: StartupPhase, duration_ms: u64, outcome: PhaseOutcome) {
        let mut recorded = self.lock_recorded();
        match recorded
            .iter_mut()
            .rev()
            .find(|(p, _, o)| *p == phase && matches!(o, PhaseOutcome::Pending))
        {
            Some(entry) => *entry = (phase, duration_ms, outcome),
            None => recorded.push((phase, duration_ms, outcome)),
        }
    }

    /// One entry per recorded phase, in begin order: `phase=<n>ms`, `phase=<n>ms(failed)`, or
    /// `phase=pending` for a phase still running when the summary is rendered.
    fn summary_line(&self) -> String {
        let recorded = self.lock_recorded();
        let mut line = String::new();
        for (phase, duration_ms, outcome) in recorded.iter() {
            if !line.is_empty() {
                line.push(' ');
            }
            let _ = match outcome {
                PhaseOutcome::Pending => write!(line, "{}=pending", phase.as_str()),
                PhaseOutcome::Success => write!(line, "{}={duration_ms}ms", phase.as_str()),
                PhaseOutcome::Failed => write!(line, "{}={duration_ms}ms(failed)", phase.as_str()),
                PhaseOutcome::Incomplete => {
                    write!(line, "{}={duration_ms}ms(incomplete)", phase.as_str())
                }
            };
        }
        line
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
            .record_outcome(self.phase, duration_ms, PhaseOutcome::Success);
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
            .record_outcome(self.phase, duration_ms, PhaseOutcome::Failed);
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
        // gap is visible instead of the phase silently never finishing, and recorded as a terminal
        // outcome so the summary does not claim a dead phase is still running.
        let duration_ms = self.duration_ms();
        self.phases
            .record_outcome(self.phase, duration_ms, PhaseOutcome::Incomplete);
        error!(
            startup_phase = self.phase.as_str(),
            duration_ms,
            elapsed_total_ms = self.phases.elapsed_total_ms(),
            "startup phase did not report an outcome"
        );
    }
}

#[cfg(test)]
mod tests;
