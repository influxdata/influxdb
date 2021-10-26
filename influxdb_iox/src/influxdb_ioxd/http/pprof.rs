use observability_deps::tracing::info;
use tokio::time::Duration;

pub async fn dump_rsprof(seconds: u64, frequency: i32) -> pprof::Result<pprof::Report> {
    let guard = pprof::ProfilerGuard::new(frequency)?;
    info!(
        "start profiling {} seconds with frequency {} /s",
        seconds, frequency
    );

    tokio::time::sleep(Duration::from_secs(seconds)).await;

    info!(
        "done profiling {} seconds with frequency {} /s",
        seconds, frequency
    );
    guard.report().build()
}
