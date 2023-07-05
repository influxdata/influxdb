use std::{borrow::Cow, path::PathBuf, time::Duration};

use metric::{Attributes, U64Gauge};
use sysinfo::{DiskExt, System, SystemExt};
use tokio::{self, task::JoinHandle};

/// Metrics that can be used to create a [`InstrumentedDiskProtection`].
#[derive(Debug)]
struct DiskProtectionMetrics {
    available_disk_space: U64Gauge,
    total_disk_space: U64Gauge,
    directory: PathBuf,
}

impl DiskProtectionMetrics {
    /// Create a new [`DiskProtectionMetrics`].
    pub(crate) fn new(directory: PathBuf, registry: &metric::Registry) -> Self {
        let path: Cow<'static, str> = Cow::from(directory.display().to_string());
        let attributes = Attributes::from([("path", path)]);

        let available_disk_space = registry
            .register_metric::<U64Gauge>(
                "disk_free_disk_space",
                "The amount of disk space currently available.",
            )
            .recorder(attributes.clone());

        let total_disk_space = registry
            .register_metric::<U64Gauge>("disk_total_disk_space", "The total amount of disk space.")
            .recorder(attributes);

        Self {
            available_disk_space,
            total_disk_space,
            directory,
        }
    }

    /// Measure the disk space.
    pub(crate) fn measure_disk_space(&self, system: &mut System) {
        system.refresh_disks_list();

        let mut path = self.directory.clone();
        let fnd_disk = loop {
            if let Some(disk) = system
                .disks_mut()
                .iter_mut()
                .find(|disk| disk.mount_point() == path)
            {
                break Some(disk);
            }
            if !path.pop() {
                break None;
            }
        };

        if let Some(disk) = fnd_disk {
            disk.refresh();

            self.available_disk_space.set(disk.available_space());
            self.total_disk_space.set(disk.total_space());
        }
    }
}

/// Disk Protection instrument.
pub struct InstrumentedDiskProtection {
    /// The metrics that are reported to the registry.
    metrics: DiskProtectionMetrics,
}

impl std::fmt::Debug for InstrumentedDiskProtection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InstrumentedDiskProtection")
    }
}

impl InstrumentedDiskProtection {
    /// Create a new [`InstrumentedDiskProtection`].
    pub fn new(directory_to_track: PathBuf, registry: &metric::Registry) -> Self {
        let metrics = DiskProtectionMetrics::new(directory_to_track, registry);

        Self { metrics }
    }

    /// Start the [`InstrumentedDiskProtection`] background task.
    pub async fn start(self) -> JoinHandle<()> {
        tokio::task::spawn(async move { self.background_task().await })
    }

    /// The background task that periodically performs the disk protection check.
    async fn background_task(&self) {
        let mut system = System::new_all();
        let mut interval = tokio::time::interval(Duration::from_secs(10));

        loop {
            self.metrics.measure_disk_space(&mut system);

            interval.tick().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use metric::Metric;

    use super::*;

    #[tokio::test]
    async fn test_metrics() {
        let registry = Arc::new(metric::Registry::new());

        struct MockAnyStruct {
            abort_handle: JoinHandle<()>,
        }

        impl MockAnyStruct {
            pub(crate) async fn new(registry: &metric::Registry) -> Self {
                let disk_protection = InstrumentedDiskProtection::new(PathBuf::from("/"), registry);
                let abort_handle = disk_protection.start().await;

                Self { abort_handle }
            }
        }

        let mock = MockAnyStruct::new(&registry).await;

        tokio::time::sleep(2 * Duration::from_secs(2)).await;

        let recorded_free_metric = registry
            .get_instrument::<Metric<U64Gauge>>("disk_free_disk_space")
            .expect("metric should exist")
            .get_observer(&Attributes::from(&[("path", "/")]))
            .expect("metric should have labels")
            .fetch();

        let recorded_total_metric = registry
            .get_instrument::<Metric<U64Gauge>>("disk_total_disk_space")
            .expect("metric should exist")
            .get_observer(&Attributes::from(&[("path", "/")]))
            .expect("metric should have labels")
            .fetch();

        assert!(recorded_free_metric > 0_u64);
        assert!(recorded_total_metric > 0_u64);
        mock.abort_handle.abort();
    }
}
