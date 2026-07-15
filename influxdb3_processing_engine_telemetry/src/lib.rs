use hashbrown::HashMap;
use influxdb3_catalog::catalog::TriggerSpecificationDefinition;
use influxdb3_telemetry::{PluginTriggerInvocation, PluginTriggerInvocationMetrics};
use parking_lot::Mutex;
use std::sync::Arc;

// Keep each telemetry payload small even when an instance has many triggers.
// We send the 255 most-run plugin triggers for the interval and drop the rest.
// This is enough to show the plugins doing meaningful work without sending a
// large list of rarely-run triggers.
pub const MAX_PLUGIN_TRIGGER_INVOCATION_TELEMETRY_ENTRIES: usize = 255;

const UNKNOWN_PLUGIN_NAME: &str = "unknown";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PluginTriggerEntrypoint {
    Writes,
    Request,
    ScheduledCall,
}

impl PluginTriggerEntrypoint {
    pub fn from_spec(spec: &TriggerSpecificationDefinition) -> Self {
        match spec {
            TriggerSpecificationDefinition::AllTablesWalWrite
            | TriggerSpecificationDefinition::SingleTableWalWrite { .. } => Self::Writes,
            TriggerSpecificationDefinition::Schedule { .. }
            | TriggerSpecificationDefinition::Every { .. } => Self::ScheduledCall,
            TriggerSpecificationDefinition::RequestPath { .. } => Self::Request,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Writes => "process_writes",
            Self::Request => "process_request",
            Self::ScheduledCall => "process_scheduled_call",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginTriggerInvocationSnapshot {
    pub database_name: String,
    pub trigger_name: String,
    pub plugin_name: String,
    pub trigger_type: &'static str,
    pub invocation_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PluginTriggerInvocationKey {
    pub(crate) database_name: Arc<str>,
    pub(crate) trigger_name: Arc<str>,
    pub(crate) plugin_name: Arc<str>,
    pub(crate) entrypoint: PluginTriggerEntrypoint,
}

impl PluginTriggerInvocationKey {
    pub fn new(
        database_name: Arc<str>,
        trigger_name: Arc<str>,
        raw_plugin_filename: &str,
        entrypoint: PluginTriggerEntrypoint,
    ) -> Self {
        Self {
            database_name,
            trigger_name,
            plugin_name: normalize_plugin_name(raw_plugin_filename),
            entrypoint,
        }
    }
}

#[derive(Debug, Default)]
pub struct PluginTriggerInvocationRegistry {
    counters: Mutex<HashMap<PluginTriggerInvocationKey, u64>>,
}

impl PluginTriggerInvocationRegistry {
    pub fn record_invocation(&self, key: &PluginTriggerInvocationKey) {
        let mut counters = self.counters.lock();
        if let Some(invocation_count) = counters.get_mut(key) {
            *invocation_count += 1;
        } else {
            counters.insert(key.clone(), 1);
        }
    }

    pub fn snapshot(&self) -> Vec<PluginTriggerInvocationSnapshot> {
        let mut entries = self
            .counters
            .lock()
            .iter()
            .map(|(key, invocation_count)| PluginTriggerInvocationSnapshot {
                database_name: key.database_name.to_string(),
                trigger_name: key.trigger_name.to_string(),
                plugin_name: key.plugin_name.to_string(),
                trigger_type: key.entrypoint.as_str(),
                invocation_count: *invocation_count,
            })
            .collect::<Vec<_>>();

        entries.sort_by(|left, right| {
            right
                .invocation_count
                .cmp(&left.invocation_count)
                .then_with(|| left.database_name.cmp(&right.database_name))
                .then_with(|| left.trigger_name.cmp(&right.trigger_name))
                .then_with(|| left.plugin_name.cmp(&right.plugin_name))
                .then_with(|| left.trigger_type.cmp(right.trigger_type))
        });
        entries.truncate(MAX_PLUGIN_TRIGGER_INVOCATION_TELEMETRY_ENTRIES);
        entries
    }

    pub fn reset(&self) {
        self.counters.lock().clear();
    }
}

fn normalize_plugin_name(raw_plugin_filename: &str) -> Arc<str> {
    let trimmed = raw_plugin_filename.trim();
    let without_gh_prefix = trimmed.strip_prefix("gh:").unwrap_or(trimmed);
    let without_trailing_separators = without_gh_prefix.trim_end_matches(['/', '\\']);

    let Some(component) = without_trailing_separators
        .split(['/', '\\'])
        .rev()
        .find(|component| !component.is_empty())
    else {
        return Arc::from(UNKNOWN_PLUGIN_NAME);
    };

    let plugin_name = component.strip_suffix(".py").unwrap_or(component);
    if plugin_name.is_empty() {
        Arc::from(UNKNOWN_PLUGIN_NAME)
    } else {
        Arc::from(plugin_name)
    }
}

#[derive(Debug)]
struct PluginTriggerInvocationTelemetryImpl {
    registry: Arc<PluginTriggerInvocationRegistry>,
}

impl PluginTriggerInvocationMetrics for PluginTriggerInvocationTelemetryImpl {
    fn plugin_trigger_invocations(&self) -> Vec<PluginTriggerInvocation> {
        self.registry
            .snapshot()
            .into_iter()
            .map(|entry| PluginTriggerInvocation {
                database_name: entry.database_name,
                trigger_name: entry.trigger_name,
                plugin_name: entry.plugin_name,
                trigger_type: entry.trigger_type.to_owned(),
                invocation_count: entry.invocation_count,
            })
            .collect()
    }

    fn reset_plugin_trigger_invocations(&self) {
        self.registry.reset();
    }
}

pub fn setup_plugin_trigger_invocation_registry() -> Arc<PluginTriggerInvocationRegistry> {
    Arc::new(PluginTriggerInvocationRegistry::default())
}

pub fn setup_plugin_trigger_invocation_telemetry(
    registry: Arc<PluginTriggerInvocationRegistry>,
) -> Arc<dyn PluginTriggerInvocationMetrics> {
    Arc::new(PluginTriggerInvocationTelemetryImpl { registry })
}

#[cfg(test)]
mod tests;
