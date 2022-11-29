use observability_deps::tracing::*;
use std::time::Duration;

/// Rotate the `wal` segment file every `period` duration of time.
pub(crate) async fn periodic_rotation(wal: wal::Wal, period: Duration) {
    let handle = wal.rotation_handle();
    let mut interval = tokio::time::interval(period);

    loop {
        interval.tick().await;
        debug!("rotating wal file");

        let stats = handle.rotate().await.expect("failed to rotate WAL");
        info!(
            closed_id = %stats.id(),
            segment_bytes = stats.size(),
            "rotated wal"
        );
    }
}
