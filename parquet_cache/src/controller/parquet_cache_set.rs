use crate::data_types::{InstanceState, State};
use k8s_openapi::api::core::v1::PodTemplateSpec;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use k8s_openapi::schemars::JsonSchema;
use kube::CustomResource;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Specification of a ParquetCacheSet.
#[derive(Debug, Default, Clone, CustomResource, Deserialize, Serialize, JsonSchema)]
#[kube(
    kind = "ParquetCacheSet",
    group = "iox.influxdata.com",
    version = "v1alpha1",
    namespaced
)]
#[kube(status = "ParquetCacheSetStatus")]
#[kube(derive = "Default")]
#[serde(rename_all = "camelCase")]
pub struct ParquetCacheSetSpec {
    /// Suffixes for the pods required to be in the set.
    pub replica_suffixes: Option<Vec<String>>,

    /// Selector is a label query over pods that should match the replica
    /// count. Label keys and values that must match in order to be controlled
    /// by this parquet cache set. It must match the pod template's labels.
    pub selector: LabelSelector,

    /// Port to connect to on the pod in order to enquire about the status of
    /// the cache.
    pub state_port: Option<String>,

    /// Template is the object that describes the pod that will be created
    /// if insufficient replicas are detected.
    pub template: Option<PodTemplateSpec>,
}

/// Status of a ParquetCacheSet.
#[derive(Debug, Default, Deserialize, Serialize, Clone, JsonSchema)]
pub struct ParquetCacheSetStatus {
    /// Status of the pods that form the set.
    pub pods: Option<BTreeMap<String, InstanceStatus>>,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone, JsonSchema)]
pub struct InstanceStatus {
    /// The phase the pod is in.
    pub phase: Option<String>,

    /// The state reported by the pod. This is only included if the pod is in the "Running" phase
    /// and the state could be queried successfully.
    pub state: Option<State>,
}

impl InstanceStatus {
    /// Determine if the status represents a warming instance.
    pub(super) fn is_warming(&self) -> bool {
        match &self.phase {
            None => false,
            Some(phase) => match phase.as_str() {
                "Running" => match &self.state {
                    None => true,
                    Some(state) => state.state == InstanceState::Warming,
                },
                _ => false,
            },
        }
    }
}

impl ParquetCacheSet {
    pub(super) fn selectors(&self) -> Option<String> {
        super::kube_util::selectors(&self.spec.selector)
    }
}
