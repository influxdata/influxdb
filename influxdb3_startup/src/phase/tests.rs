use std::sync::Mutex;

use super::*;

/// Records what reached the observer, so tests assert on emissions rather than log output.
#[derive(Debug, Default)]
struct RecordingObserver {
    events: Mutex<Vec<Event>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    Success {
        phase: &'static str,
        detail: String,
    },
    Error {
        phase: &'static str,
        code: &'static str,
    },
}

impl RecordingObserver {
    fn events(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }
}

impl StartupPhaseObserver for RecordingObserver {
    fn on_phase_success(&self, phase: StartupPhase, _duration_ms: u64, detail: String) {
        self.events.lock().unwrap().push(Event::Success {
            phase: phase.as_str(),
            detail,
        });
    }

    fn on_phase_error(&self, phase: StartupPhase, error_code: &'static str, _duration_ms: u64) {
        self.events.lock().unwrap().push(Event::Error {
            phase: phase.as_str(),
            code: error_code,
        });
    }
}

fn phases() -> (Arc<RecordingObserver>, StartupPhases) {
    let observer = Arc::new(RecordingObserver::default());
    let phases = StartupPhases::new(
        Arc::clone(&observer) as Arc<dyn StartupPhaseObserver>,
        Instant::now(),
    );
    (observer, phases)
}

#[test]
fn success_reports_once() {
    let (observer, phases) = phases();
    {
        let guard = phases.begin(StartupPhase::CatalogLoad, "");
        guard.success("uuid_abc");
    }
    assert_eq!(
        observer.events(),
        vec![Event::Success {
            phase: "catalog_load",
            detail: "uuid_abc".to_string(),
        }]
    );
}

#[test]
fn error_reports_once() {
    let (observer, phases) = phases();
    {
        let guard = phases.begin(StartupPhase::WalReplay, "");
        guard.error("wal_replay_failed");
    }
    assert_eq!(
        observer.events(),
        vec![Event::Error {
            phase: "wal_replay",
            code: "wal_replay_failed",
        }]
    );
}

/// `inspect_err` calls `error` and the `?` that follows drops the guard. Drop must not turn that
/// into a second event.
#[test]
fn error_then_drop_does_not_report_twice() {
    let (observer, phases) = phases();
    {
        let guard = phases.begin(StartupPhase::SnapshotRestore, "");
        let result: Result<(), &str> = Err("boom");
        let _ = result.inspect_err(|_| guard.error("snapshot_restore_failed"));
    }
    assert_eq!(observer.events().len(), 1, "phase must report exactly once");
}

/// A phase that reports success and is then dropped must not also report an outcome from `Drop`.
#[test]
fn success_then_drop_does_not_report_twice() {
    let (observer, phases) = phases();
    {
        let guard = phases.begin(StartupPhase::CacheWarm, "");
        guard.success("skipped");
    }
    assert_eq!(observer.events().len(), 1, "phase must report exactly once");
}

/// Dropping without reporting is a bug at the call site. It logs, but must not fabricate a success
/// or error event for the observer.
#[test]
fn drop_without_outcome_emits_no_observer_event() {
    let (observer, phases) = phases();
    {
        let _guard = phases.begin(StartupPhase::NodeRegistration, "");
    }
    assert!(
        observer.events().is_empty(),
        "an unreported phase must not reach the observer"
    );
}

/// A later phase must report a larger cumulative elapsed than an earlier one, since both measure
/// against the same process origin.
#[test]
fn elapsed_total_is_monotonic_across_phases() {
    let (_observer, phases) = phases();
    let first = phases.elapsed_total_ms();
    std::thread::sleep(std::time::Duration::from_millis(2));
    let second = phases.elapsed_total_ms();
    assert!(second >= first, "{second} should be >= {first}");
}

/// `report_completed` stands in for a whole begin/success pair, so it must feed the observer
/// exactly one success event carrying the detail.
#[test]
fn report_completed_emits_one_success_event() {
    let (observer, phases) = phases();
    let started = Instant::now();
    phases.report_completed(
        StartupPhase::TempCatalogLoad,
        started,
        started + std::time::Duration::from_millis(3),
        "storage_mode=parquet",
    );
    assert_eq!(
        observer.events(),
        vec![Event::Success {
            phase: "temp_catalog_load",
            detail: "storage_mode=parquet".to_string(),
        }]
    );
}

/// `report_completed` must emit the same started and finished lines a live guard would, and a
/// span that predates the tracker origin must clamp `elapsed_total_ms` to zero rather than
/// underflow — the pre-logging phases run before `startup_timer` exists.
#[test]
fn report_completed_logs_both_lines_and_clamps_before_origin() {
    let capture = test_helpers::tracing::TracingCapture::new();
    let (_observer, phases) = phases();
    // The instants are fixed, so the 5 ms duration below is exact, not timing-dependent.
    let origin = Instant::now();
    let started = origin - std::time::Duration::from_millis(10);
    let finished = origin - std::time::Duration::from_millis(5);
    let phases = StartupPhases {
        process_start: origin,
        ..phases
    };
    phases.report_completed(
        StartupPhase::Licensing,
        started,
        finished,
        "licensed_cores=2",
    );
    let logs = capture.to_string();
    let started_line = logs
        .lines()
        .find(|l| l.contains("startup phase started"))
        .expect("a started line is logged");
    assert!(
        started_line.contains(r#"startup_phase = "licensing""#),
        "{logs}"
    );
    assert!(started_line.contains("elapsed_total_ms = 0"), "{logs}");
    let finished_line = logs
        .lines()
        .find(|l| l.contains("startup phase finished"))
        .expect("a finished line is logged");
    assert!(
        finished_line.contains(r#"startup_phase = "licensing""#),
        "{logs}"
    );
    assert!(finished_line.contains("duration_ms = 5"), "{logs}");
    assert!(finished_line.contains("elapsed_total_ms = 0"), "{logs}");
    assert!(
        finished_line.contains(r#"detail = "licensed_cores=2""#),
        "{logs}"
    );
}
