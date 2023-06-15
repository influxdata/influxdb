use std::{sync::Arc, time::Duration};

use metric::{Attributes, U64Gauge};
use parking_lot::Mutex;
use sysinfo::{DiskExt, System, SystemExt};
use tokio::{self, task::JoinHandle};

/// Metrics that can be used to create a [`InstrumentedDiskProtection`].
#[derive(Debug)]
struct DiskProtectionMetrics {
    available_disk_space_percent: U64Gauge,
}

impl DiskProtectionMetrics {
    /// Create a new [`DiskProtectionMetrics`].
    pub(crate) fn new(registry: &metric::Registry, attributes: impl Into<Attributes>) -> Self {
        let attributes: Attributes = attributes.into();

        let available_disk_space_percent = registry
            .register_metric::<U64Gauge>(
                "disk_protection_free_disk_space",
                "The percentage amount of disk available.",
            )
            .recorder(attributes);

        Self {
            available_disk_space_percent,
        }
    }

    /// Measure the available disk space percentage.
    pub(crate) fn measure_available_disk_space_percent(&self, system: &System) -> u64 {
        let available_disk: u64 = system
            .disks()
            .iter()
            .map(|disk| disk.available_space())
            .sum();
        let total_disk: u64 = system.disks().iter().map(|disk| disk.total_space()).sum();
        let available_disk_percentage =
            ((available_disk as f64) / (total_disk as f64) * 100.0).round() as u64;
        self.available_disk_space_percent
            .set(available_disk_percentage);

        available_disk_percentage
    }
}

/// Protective action taken, per each cycle of background task
struct DiskProtectionAction {
    /// Function used to check if action should be triggered.
    trigger: Box<dyn FnMut(u64, DiskProtectionState) -> bool + Send + Sync>,
    /// Callback action taken.
    callback: Option<Box<dyn FnMut() + Send + Sync>>,
    /// Next state.
    next_state: DiskProtectionState,
}

impl DiskProtectionAction {
    /// Perform the protection action as per the [`DiskProtectionAction`] contract.
    pub(crate) fn check_trigger(&mut self, measured: u64, curr_state: &Mutex<DiskProtectionState>) {
        let mut curr_state = curr_state.lock();
        match (&mut self.callback, (self.trigger)(measured, *curr_state)) {
            (None, _) => {}
            (Some(_), false) => {}
            (Some(callback), true) => {
                *curr_state = self.next_state;
                callback();
            }
        }
    }
}

#[derive(Copy, Clone, PartialEq)]
/// Current state of disk protection.
enum DiskProtectionState {
    /// DiskProtection has activated by triggering the appropriate calllback.
    Activated,
    /// DiskProtection is not activated, but is still watching (and checking) the metrics.
    Watching,
}

/// Disk Protection instrument.
pub struct InstrumentedDiskProtection {
    /// How often to perform the disk protection check.
    interval_duration: Duration,
    /// The metrics that are reported to the registry.
    metrics: DiskProtectionMetrics,
    /// The handle to terminate the background task.
    background_task: Mutex<Option<JoinHandle<()>>>,
    /// Current state of disk protection.
    state: Mutex<DiskProtectionState>,
    /// Callback triggered when disk protection is enacted.
    callback_on_protection_begin: tokio::sync::Mutex<DiskProtectionAction>,
    /// Callback triggered when disk protection has ended.
    callback_on_protection_end: tokio::sync::Mutex<DiskProtectionAction>,
}

impl std::fmt::Debug for InstrumentedDiskProtection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InstrumentedDiskProtection")
    }
}

impl InstrumentedDiskProtection {
    /// Create a new [`InstrumentedDiskProtection`].
    pub fn new(
        registry: &metric::Registry,
        attributes: impl Into<Attributes> + Send,
        interval_duration: Duration,
        disk_threshold: u64,
        callback_on_protection_begin: Option<Box<dyn FnMut() + Send + Sync>>,
        callback_on_protection_end: Option<Box<dyn FnMut() + Send + Sync>>,
    ) -> Self {
        let metrics = DiskProtectionMetrics::new(registry, attributes);

        Self {
            interval_duration,
            metrics,
            background_task: Default::default(),
            state: Mutex::new(DiskProtectionState::Watching),
            callback_on_protection_begin: tokio::sync::Mutex::new(DiskProtectionAction {
                trigger: Box::new(move |curr_metric: u64, curr_state| {
                    curr_metric <= disk_threshold && curr_state == DiskProtectionState::Watching
                }),
                callback: callback_on_protection_begin,
                next_state: DiskProtectionState::Activated,
            }),
            callback_on_protection_end: tokio::sync::Mutex::new(DiskProtectionAction {
                trigger: Box::new(move |curr_metric: u64, curr_state| {
                    curr_metric > disk_threshold && curr_state == DiskProtectionState::Activated
                }),
                callback: callback_on_protection_end,
                next_state: DiskProtectionState::Watching,
            }),
        }
    }

    /// Start the [`InstrumentedDiskProtection`] background task.
    pub async fn start(self) {
        let rc_self = Arc::new(self);
        let rc_self_clone = Arc::clone(&rc_self);

        *rc_self.background_task.lock() = Some(tokio::task::spawn(async move {
            rc_self_clone.background_task().await
        }));
    }

    /// Stop the [`InstrumentedDiskProtection`] background task.
    pub fn stop(&mut self) {
        if let Some(t) = self.background_task.lock().take() {
            t.abort()
        }
    }

    /// The background task that periodically performs the disk protection check.
    async fn background_task(&self) {
        let mut system = System::new_all();
        let mut interval = tokio::time::interval(self.interval_duration);

        loop {
            interval.tick().await;

            system.refresh_all();

            // Protective actions based upon available_disk_percentage.
            let available_disk_percentage =
                self.metrics.measure_available_disk_space_percent(&system);
            self.callback_on_protection_begin
                .lock()
                .await
                .check_trigger(available_disk_percentage, &self.state);
            self.callback_on_protection_end
                .lock()
                .await
                .check_trigger(available_disk_percentage, &self.state);
        }
    }
}

impl Drop for InstrumentedDiskProtection {
    fn drop(&mut self) {
        // future-proof, such that stop does not need to be explicitly called.
        self.stop();
    }
}
