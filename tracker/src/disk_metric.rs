use std::{borrow::Cow, path::PathBuf, time::Duration};

use metric::{Attributes, U64Gauge};
use sysinfo::{DiskExt, RefreshKind, System, SystemExt};

/// The interval at which disk metrics are updated.
///
/// This is purposely chosen to be out-of-phase w.r.t the default metric scrape
/// interval.
const UPDATE_INTERVAL: Duration = Duration::from_secs(13);

/// A periodic reporter of disk capacity / free statistics for a given
/// directory.
#[derive(Debug)]
pub struct DiskSpaceMetrics {
    available_disk_space: U64Gauge,
    total_disk_space: U64Gauge,

    /// The [`System`] containing the disk list at construction time.
    system: System,

    /// The index into [`System::disks()`] for the disk containing the observed
    /// directory.
    disk_idx: usize,
}

impl DiskSpaceMetrics {
    /// Create a new [`DiskSpaceMetrics`], returning [`None`] if no disk can be
    /// found for the specified `directory`.
    pub fn new(directory: PathBuf, registry: &metric::Registry) -> Option<Self> {
        let path: Cow<'static, str> = Cow::from(directory.display().to_string());
        let mut directory = directory.canonicalize().ok()?;

        let attributes = Attributes::from([("path", path)]);

        let available_disk_space = registry
            .register_metric::<U64Gauge>(
                "disk_space_free",
                "The amount of disk space currently available at the labelled mount point.",
            )
            .recorder(attributes.clone());

        let total_disk_space = registry
            .register_metric::<U64Gauge>(
                "disk_capacity_total",
                "The disk capacity at the labelled mount point.",
            )
            .recorder(attributes);

        // Load the disk stats once, and refresh them later.
        let system = System::new_with_specifics(RefreshKind::new().with_disks_list());

        // Resolve the mount point once.
        // The directory path may be `/path/to/dir` and the mount point is `/`.
        let disk_idx = loop {
            if let Some((idx, _disk)) = system
                .disks()
                .iter()
                .enumerate()
                .find(|(_idx, disk)| disk.mount_point() == directory)
            {
                break idx;
            }
            // The mount point for this directory could not be found.
            if !directory.pop() {
                return None;
            }
        };

        Some(Self {
            available_disk_space,
            total_disk_space,
            system,
            disk_idx,
        })
    }

    /// Start the [`DiskSpaceMetrics`] evaluation loop, blocking forever.
    pub async fn run(mut self) {
        let mut interval = tokio::time::interval(UPDATE_INTERVAL);

        loop {
            interval.tick().await;

            let disk = self
                .system
                .disks_mut()
                .get_mut(self.disk_idx)
                .expect("disk list never refreshed so should not change");

            // Refresh the stats for this disk only.
            disk.refresh();

            self.available_disk_space.set(disk.available_space());
            self.total_disk_space.set(disk.total_space());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Instant};

    use metric::Metric;
    use tempfile::tempdir_in;

    use super::*;

    #[tokio::test]
    async fn test_metrics() {
        let tmp_dir = tempdir_in(".").ok().unwrap();
        let path = tmp_dir.path().display().to_string();
        // TempDir creates a directory in current directory, so test the relative path (if possible).
        let path = match path.find("/./") {
            Some(index) => &path[index + 3..],
            None => &path[..],
        };

        let pathbuf = PathBuf::from(path);
        let metric_label: Cow<'static, str> = path.to_string().into();

        let registry = Arc::new(metric::Registry::new());

        let _handle = tokio::spawn(
            DiskSpaceMetrics::new(pathbuf, &registry)
                .expect("root always exists")
                .run(),
        );

        // Wait for the metric to be emitted and non-zero - this should be very
        // quick!
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            // Bound the test duration
            if Instant::now() > deadline {
                panic!("timeout waiting for disk metrics to be resolved");
            }

            let recorded_free_metric = registry
                .get_instrument::<Metric<U64Gauge>>("disk_space_free")
                .expect("metric should exist")
                .get_observer(&Attributes::from([("path", metric_label.clone())]))
                .expect("metric should have labels")
                .fetch();

            let recorded_total_metric = registry
                .get_instrument::<Metric<U64Gauge>>("disk_capacity_total")
                .expect("metric should exist")
                .get_observer(&Attributes::from([("path", metric_label.clone())]))
                .expect("metric should have labels")
                .fetch();

            if recorded_free_metric > 0 && recorded_total_metric > 0 {
                return;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
