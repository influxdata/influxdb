use super::{Error, Result, PARQUET_CACHE_REPLICAS_LABEL};
use k8s_openapi::api::core::v1::PodTemplateSpec;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use k8s_openapi::schemars::JsonSchema;
use kube::CustomResource;
use serde::{Deserialize, Serialize};

/// Specification of a ParquetCache.
#[derive(Debug, Default, Clone, CustomResource, Deserialize, Serialize, JsonSchema)]
#[kube(
    kind = "ParquetCache",
    group = "iox.influxdata.com",
    version = "v1alpha1",
    namespaced
)]
#[kube(status = "ParquetCacheStatus")]
#[kube(derive = "Default")]
#[serde(rename_all = "camelCase")]
pub struct ParquetCacheSpec {
    /// The name of the config map to generate containing the data cache set
    /// state. This config map must be volume mounted in the pod template.
    /// If a name isn't set then the config map will have the same name as
    /// the data cache set.
    pub config_map_name: Option<String>,

    /// The number of replicas that are required to be in the data cache set.
    pub replicas: Option<i32>,

    /// Selector is a label query over pods that should match the replica
    /// count. Label keys and values that must match in order to be controlled
    /// by this data cache set. It must match the pod template's labels.
    pub selector: LabelSelector,

    /// Port running on the pods that should be used to query the working state
    /// using the `/state` endpoint.
    pub state_port: Option<String>,

    /// Template is the object that describes the pod that will be created
    /// if insufficient replicas are detected.
    pub template: PodTemplateSpec,
}

/// Status of a ParquetCache.
#[derive(Debug, Default, Deserialize, Serialize, Clone, JsonSchema)]
pub struct ParquetCacheStatus {
    /// The current cache instance set.
    pub current: ParquetCacheInstanceSet,

    /// The upcoming cache instance set.
    pub next: ParquetCacheInstanceSet,
}

/// The set of instances that form a parquet cache group.
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct ParquetCacheInstanceSet {
    /// The revision number of the cache instance set.
    pub revision: i64,

    /// The set of instances that form the cache set.
    pub instances: Vec<String>,
}

impl ParquetCache {
    fn name(&self) -> Result<&String> {
        self.metadata
            .name
            .as_ref()
            .ok_or(Error::internal("ParquetCache has no name"))
    }

    /// Get the name of the [k8s_openapi::api::core::v1::ConfigMap] that should be created to
    /// contain the status information required by the parquet servers.
    pub(super) fn config_map_name(&self) -> Result<&String> {
        if let Some(name) = &self.spec.config_map_name {
            Ok(name)
        } else {
            self.name()
        }
    }

    /// The number of replicas specified for this ParquetCache.
    pub(super) fn replicas(&self) -> i32 {
        self.spec.replicas.unwrap_or(1)
    }

    /// Get the PodTemplateSpec to pass on to the [super::ParquetCacheSet]. This will make necessary
    /// changes to the template supplied in the [ParquetCacheSpec].
    ///
    /// The generated [PodTemplateSpec] includes a label containing the requested replica count.
    /// This ensures that a different [super::ParquetCacheSet] is created even if the only change to the
    /// [ParquetCache] is a change in the replica count.
    pub(super) fn parquet_cache_set_template(&self) -> PodTemplateSpec {
        let mut template = self.spec.template.clone();
        let metadata = template.metadata.get_or_insert(Default::default());
        let labels = metadata.labels.get_or_insert(Default::default());
        labels.insert(
            String::from(PARQUET_CACHE_REPLICAS_LABEL),
            format!("{}", self.replicas()),
        );
        template
    }

    /// Generate a name for a ParquetCacheSet derived from this ParquetCache.
    pub(super) fn parquet_cache_set_name(&self, pod_template_hash: &str) -> Result<String> {
        let name = self.name()?;
        Ok(format!("{name}-{pod_template_hash}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    #[test]
    fn config_map_name() {
        let pc = ParquetCache {
            metadata: ObjectMeta {
                name: Some(String::from("test-data-cache-set")),
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!("test-data-cache-set", pc.config_map_name().unwrap());

        let pc = ParquetCache {
            metadata: ObjectMeta {
                name: Some("test-data-cache-set".to_string()),
                ..Default::default()
            },
            spec: ParquetCacheSpec {
                config_map_name: Some(String::from("config-map")),
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!("config-map", pc.config_map_name().unwrap());
    }
}
