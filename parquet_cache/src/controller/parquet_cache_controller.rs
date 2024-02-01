use super::{
    kube_util::{hash_object, list_owned, owner_reference},
    Error, ParquetCache, ParquetCacheInstanceSet, ParquetCacheSet, ParquetCacheSetSpec,
    ParquetCacheStatus, Result, LONG_WAIT, PARQUET_CACHE_REPLICAS_LABEL, POD_TEMPLATE_HASH_LABEL,
    SHORT_WAIT,
};
use crate::data_types::InstanceState;
use chrono::Utc;
use futures::StreamExt;
use k8s_openapi::api::core::v1::{ConfigMap, PodTemplateSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::runtime::controller::Action;
use kube::runtime::Controller;
use kube::{Api, Client, Resource, ResourceExt};
use observability_deps::tracing::{debug, error, info};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// Start a new controller task to reconcile [ParquetCacheSet] objects.
pub fn spawn_controller(client: Client, ns: Option<String>) -> JoinHandle<Result<(), kube::Error>> {
    tokio::spawn(run_controller(client, ns))
}

async fn run_controller(client: Client, ns: Option<String>) -> Result<(), kube::Error> {
    let parquet_cache_api = match &ns {
        Some(ns) => Api::<ParquetCache>::namespaced(client.clone(), ns),
        None => Api::<ParquetCache>::all(client.clone()),
    };
    let parquet_cache_set_api = match &ns {
        Some(ns) => Api::<ParquetCacheSet>::namespaced(client.clone(), ns),
        None => Api::<ParquetCacheSet>::all(client.clone()),
    };

    Controller::new(parquet_cache_api, Default::default())
        .owns(parquet_cache_set_api, Default::default())
        .run(reconcile, error_policy, Arc::new(Context { client }))
        .for_each(|_| futures::future::ready(()))
        .await;
    Ok(())
}

async fn reconcile(obj: Arc<ParquetCache>, ctx: Arc<Context>) -> Result<Action> {
    let namespace = obj.metadata.namespace.as_deref();
    let name = obj.name_any();
    info!(namespace, name, "reconcile request");
    let sleep = ParquetCacheController::new(obj.as_ref().clone(), ctx.client.clone())
        .reconcile()
        .await?;
    Ok(Action::requeue(sleep))
}

fn error_policy(_object: Arc<ParquetCache>, err: &Error, _ctx: Arc<Context>) -> Action {
    // TODO add exponential backoff
    let sleep = Duration::from_secs(5);
    error!(
        err = err as &dyn std::error::Error,
        "reconcile failed, requeue in {:?}", sleep
    );
    Action::requeue(sleep)
}

/// Context used when reconciling [ParquetCacheSet] objects.
struct Context {
    client: Client,
}

const COOLING_SECONDS: i64 = 300;

/// Controller for the ParquetCache custom resource. This controller maintains ParquetCacheSet
/// resources for a ParquetCache.
#[derive(Debug)]
struct ParquetCacheController {
    config_map_api: Api<ConfigMap>,
    parquet_cache_api: Api<ParquetCache>,
    parquet_cache_set_api: Api<ParquetCacheSet>,

    parquet_cache: ParquetCache,
}

impl ParquetCacheController {
    /// Create a new ParquetCacheSetController instance for the provided [ParquetCacheSet]
    /// and [Client].
    fn new(parquet_cache: ParquetCache, client: Client) -> Self {
        let ns = parquet_cache.metadata.namespace.as_ref().unwrap();
        let config_maps = Api::namespaced(client.clone(), ns);
        let parquet_caches = Api::namespaced(client.clone(), ns);
        let parquet_cache_sets = Api::namespaced(client.clone(), ns);

        Self {
            config_map_api: config_maps,
            parquet_cache_api: parquet_caches,
            parquet_cache_set_api: parquet_cache_sets,
            parquet_cache,
        }
    }

    /// Perform the business logic required to move the DataCacheSet state forward towards the
    /// desired state.
    pub async fn reconcile(&mut self) -> Result<Duration> {
        // ensure the config map exists before attempting to start pods.
        let cm = self.status_config_map()?;
        match self.config_map_api.create(&Default::default(), &cm).await {
            Ok(_) => {
                info!(name = cm.metadata.name, "Created ConfigMap");
            }
            Err(kube::Error::Api(status)) if status.reason == "AlreadyExists" => (),
            Err(error) => return Err(error)?,
        }

        let duration = self.reconcile_inner().await?;

        // update the config map with the latest set.
        let cm = self.status_config_map()?;
        debug!("update config map");
        self.config_map_api
            .replace(
                self.parquet_cache.config_map_name()?,
                &Default::default(),
                &cm,
            )
            .await?;
        debug!("update ParquetCache status");
        self.parquet_cache_api
            .replace_status(
                self.parquet_cache.metadata.name.as_ref().unwrap(),
                &Default::default(),
                serde_json::to_vec(&self.parquet_cache)?,
            )
            .await?;
        Ok(duration)
    }

    /// Perform the changes required to reconcile the state of the ParquetCache. Changes to the
    /// status are written to memory and will updated after this method returns.
    async fn reconcile_inner(&mut self) -> Result<Duration> {
        let template = self.parquet_cache.parquet_cache_set_template();
        let pod_template_hash = hash_object(&template)?;

        // find and remove any owned cache sets that are no longer required.
        self.remove_empty_cache_sets(&pod_template_hash).await?;

        if self.check_warming_pods().await? {
            self.status_mut().current = self.status_mut().next.clone();
        } else {
            // Some pods are still warming, check again soon.
            return Ok(SHORT_WAIT);
        }
        if !self.check_cooling_pods(&pod_template_hash).await? {
            // Some pods are still cooling, check again soon.
            return Ok(SHORT_WAIT);
        }
        if self.status_mut().current.instances.len() != self.parquet_cache.replicas() as usize {
            self.resize(&pod_template_hash, &template).await?;
        } else {
            self.migrate(&pod_template_hash, &template).await?;
        }

        // If we get to here then either there is nothing to change, or some changes
        // have been made and the controller will be woken by those changes.
        Ok(LONG_WAIT)
    }

    async fn remove_empty_cache_sets(&mut self, pod_template_hash: &String) -> Result<()> {
        let parquet_cache_sets = self.owned_parquet_cache_sets().await?;
        let to_delete = parquet_cache_sets
            .into_iter()
            .filter(|pcs| {
                let is_latest = if let Some(pth) = pcs
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|labels| labels.get(POD_TEMPLATE_HASH_LABEL).cloned())
                {
                    &pth == pod_template_hash
                } else {
                    false
                };
                let is_empty = if let Some(pods) =
                    pcs.status.as_ref().and_then(|status| status.pods.as_ref())
                {
                    pods.is_empty()
                } else {
                    true
                };
                !is_latest && is_empty
            })
            .collect::<Vec<ParquetCacheSet>>();

        for pcs in to_delete {
            info!(name = pcs.metadata.name, "Deleting ParquetCacheSet");
            self.parquet_cache_set_api
                .delete(&pcs.metadata.name.unwrap(), &Default::default())
                .await?;
        }
        Ok(())
    }

    async fn check_warming_pods(&mut self) -> Result<bool> {
        let status = self.status_mut();
        if status.current.revision == status.next.revision {
            return Ok(true);
        }
        for instance in status.next.instances.clone() {
            let (parquet_cache_set_name, _) = instance.rsplit_once('-').unwrap();
            let parquet_cache_set = self
                .parquet_cache_set_api
                .get(parquet_cache_set_name)
                .await?;
            let parquet_cache_set_status = parquet_cache_set.status.unwrap_or_default();
            let pod_status = parquet_cache_set_status
                .pods
                .as_ref()
                .and_then(|pods| pods.get(&instance));
            let phase = pod_status
                .and_then(|status| status.phase.as_ref())
                .map(String::as_str);
            let state = pod_status
                .and_then(|status| status.state.as_ref())
                .map(|state| state.state.to_string());
            debug!(name = &instance, phase, state, "Checking Pod status");
            if phase.unwrap_or("") != "Running" {
                return Ok(false);
            }
            if pod_status
                .and_then(|status| status.state.as_ref())
                .map(|state| state.state != InstanceState::Warming)
                .unwrap_or(true)
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn check_cooling_pods(&mut self, pod_template_hash: &String) -> Result<bool> {
        let mut live_pods = self
            .status_mut()
            .current
            .instances
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        for pod in &self.status_mut().next.instances {
            live_pods.insert(pod.clone());
        }
        let parquet_cache_sets = self.owned_parquet_cache_sets().await?;

        let current_status = parquet_cache_sets
            .iter()
            .filter_map(|pcs| pcs.status.as_ref())
            .filter_map(|status| status.pods.as_ref())
            .flat_map(|pods| pods.clone().into_iter().collect::<Vec<_>>())
            .filter(|(k, _)| self.status_mut().current.instances.contains(k))
            .map(|(k, status)| {
                let (_, suffix) = split_pod_name(&k);
                (suffix, status)
            })
            .collect::<BTreeMap<_, _>>();

        let cooling_pods = parquet_cache_sets
            .iter()
            .filter(|&pcs| !has_pod_template_hash(pcs, pod_template_hash))
            .filter_map(|pcs| pcs.status.as_ref())
            .filter_map(|status| status.pods.as_ref())
            .flat_map(|pods| pods.keys().cloned().collect::<Vec<_>>())
            .filter(|key| !live_pods.contains(key))
            .collect::<Vec<_>>();

        let mut cooling = false;
        for pod in cooling_pods {
            let (pcs_name, suffix) = split_pod_name(&pod);
            if let Some(change) = current_status
                .get(&suffix)
                .and_then(|status| status.state.as_ref())
                .map(|state| state.state_changed)
            {
                if change > Utc::now().timestamp() - COOLING_SECONDS {
                    // If the pod has been cooling for less than the wait time, keep waiting.
                    cooling = true;
                    continue;
                }
            }
            let mut pcs = self.parquet_cache_set_api.get(&pcs_name).await?;
            pcs.spec
                .replica_suffixes
                .as_mut()
                .unwrap()
                .retain(|s| s != &suffix);
            self.parquet_cache_set_api
                .replace(&pcs_name, &Default::default(), &pcs)
                .await?;
        }
        Ok(!cooling)
    }

    async fn resize(
        &mut self,
        pod_template_hash: &String,
        template: &PodTemplateSpec,
    ) -> Result<()> {
        let owned = self.owned_parquet_cache_sets().await?;

        // Clear any ParquetCacheSets that are not the required one.
        for mut pcs in owned {
            let is_current = pcs
                .metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(POD_TEMPLATE_HASH_LABEL))
                .map(|v| v == pod_template_hash)
                .unwrap_or_default();
            if is_current {
                continue;
            }
            pcs.spec.replica_suffixes = None;
            self.set_parquet_cache_set(&pcs).await?;
        }

        // Create the desired ParquetCacheSet.
        let mut pcs = self
            .get_parquet_cache_set(pod_template_hash, template)
            .await?;
        let suffixes = (0..self.parquet_cache.replicas())
            .map(|n| format!("{n}"))
            .collect::<Vec<_>>();
        pcs.spec.replica_suffixes = Some(suffixes.clone());
        self.set_parquet_cache_set(&pcs).await?;
        let next_revision = self.status_mut().next.revision + 1;
        let instances = suffixes
            .iter()
            .map(|suffix| format!("{}-{suffix}", pcs.metadata.name.as_ref().unwrap()))
            .collect();
        self.status_mut().next = ParquetCacheInstanceSet {
            revision: next_revision,
            instances,
        };
        self.status_mut().current = self.status_mut().next.clone();
        Ok(())
    }

    async fn migrate(&mut self, pod_template_hash: &str, template: &PodTemplateSpec) -> Result<()> {
        let current = self.status_mut().current.clone();
        assert_eq!(current.revision, self.status_mut().next.revision);
        let parquet_cache_set_name = self
            .parquet_cache
            .parquet_cache_set_name(pod_template_hash)?;

        for (idx, name) in current.instances.iter().enumerate() {
            let (prefix, suffix) = split_pod_name(name);
            if prefix == parquet_cache_set_name {
                continue;
            }
            let mut pcs = self
                .get_parquet_cache_set(pod_template_hash, template)
                .await?;
            if pcs.spec.replica_suffixes.is_none() {
                pcs.spec.replica_suffixes = Some(vec![]);
            }
            pcs.spec
                .replica_suffixes
                .as_mut()
                .unwrap()
                .push(suffix.clone());
            self.set_parquet_cache_set(&pcs).await?;
            self.status_mut().next.revision = current.revision + 1;
            self.status_mut().next.instances[idx] = format!("{parquet_cache_set_name}-{suffix}");
            break;
        }
        Ok(())
    }

    async fn owned_parquet_cache_sets(&self) -> Result<Vec<ParquetCacheSet>> {
        let uid = self
            .parquet_cache
            .metadata
            .uid
            .as_ref()
            .ok_or(Error::internal("ParquetCache has no uid"))?;
        Ok(list_owned(&self.parquet_cache_set_api, uid).await?)
    }

    /// Create or update the specified ParquetCacheSet.
    async fn set_parquet_cache_set(&mut self, pcs: &ParquetCacheSet) -> Result<ParquetCacheSet> {
        let name = pcs.metadata.name.as_ref().ok_or(Error::internal(
            "attempt to set a ParquetCacheSet without a name",
        ))?;
        let pp = Default::default();
        if pcs.metadata.uid.is_some() {
            Ok(self.parquet_cache_set_api.replace(name, &pp, pcs).await?)
        } else {
            Ok(self.parquet_cache_set_api.create(&pp, pcs).await?)
        }
    }

    /// Retrieve the ParquetCacheSet for the specified Pod template hash. If there is no such
    /// ParquetCacheSet then create a ParquetCacheSet object with appropriate defaults taken from
    /// the current ParquetCache document.
    async fn get_parquet_cache_set(
        &mut self,
        pod_template_hash: &str,
        template: &PodTemplateSpec,
    ) -> Result<ParquetCacheSet> {
        let name = self
            .parquet_cache
            .parquet_cache_set_name(pod_template_hash)?;
        Ok(self
            .parquet_cache_set_api
            .get_opt(&name)
            .await?
            .unwrap_or_else(|| self.new_parquet_cache_set(name, pod_template_hash, template)))
    }

    fn new_parquet_cache_set(
        &self,
        name: String,
        pod_template_hash: &str,
        template: &PodTemplateSpec,
    ) -> ParquetCacheSet {
        let pod_template_hash_key = String::from(POD_TEMPLATE_HASH_LABEL);
        let replica_count_key = String::from(PARQUET_CACHE_REPLICAS_LABEL);
        let replica_count_value = format!("{}", self.parquet_cache.replicas());

        let mut labels = self
            .parquet_cache
            .metadata
            .labels
            .clone()
            .unwrap_or_default();
        labels.insert(
            pod_template_hash_key.clone(),
            String::from(pod_template_hash),
        );
        labels.insert(replica_count_key.clone(), replica_count_value.clone());
        let mut selector = self.parquet_cache.spec.selector.clone();
        let match_labels = selector.match_labels.get_or_insert(Default::default());
        match_labels.insert(
            pod_template_hash_key.clone(),
            String::from(pod_template_hash),
        );
        match_labels.insert(replica_count_key.clone(), replica_count_value.clone());

        let mut template = template.clone();
        let template_metadata = template.metadata.get_or_insert(Default::default());
        template_metadata.namespace = self.parquet_cache.metadata.namespace.clone();
        let template_labels = template_metadata.labels.get_or_insert(Default::default());
        template_labels.insert(
            pod_template_hash_key.clone(),
            String::from(pod_template_hash),
        );

        ParquetCacheSet {
            metadata: ObjectMeta {
                labels: Some(labels),
                name: Some(name),
                namespace: self.parquet_cache.metadata.namespace.clone(),
                owner_references: Some(vec![self.owner_reference()]),
                ..Default::default()
            },
            spec: ParquetCacheSetSpec {
                replica_suffixes: None,
                selector,
                state_port: self.parquet_cache.spec.state_port.clone(),
                template: Some(template),
            },
            status: None,
        }
    }

    fn status_config_map(&mut self) -> Result<ConfigMap> {
        let mut data = BTreeMap::new();
        let status = self.status_mut();
        data.insert(
            "current".to_string(),
            serde_json::to_string(&status.current)?,
        );
        data.insert("next".to_string(), serde_json::to_string(&status.next)?);
        Ok(ConfigMap {
            metadata: ObjectMeta {
                namespace: self.parquet_cache.metadata.namespace.clone(),
                name: Some(self.parquet_cache.config_map_name()?.clone()),
                owner_references: Some(vec![self.owner_reference()]),
                ..Default::default()
            },
            data: Some(data),
            ..Default::default()
        })
    }

    /// Generate an owner reference for the current ParquetCache document.
    fn owner_reference(&self) -> OwnerReference {
        owner_reference(&self.parquet_cache)
    }

    fn status_mut(&mut self) -> &mut ParquetCacheStatus {
        self.parquet_cache.status.get_or_insert(Default::default())
    }
}

fn split_pod_name(name: &str) -> (String, String) {
    if let Some((prefix, suffix)) = name.rsplit_once('-') {
        (String::from(prefix), String::from(suffix))
    } else {
        (String::from(name), String::from(""))
    }
}

fn has_pod_template_hash<K>(obj: &K, pod_template_hash: &String) -> bool
where
    K: Resource,
{
    if let Some(hash) = obj
        .meta()
        .labels
        .as_ref()
        .and_then(|labels| labels.get(POD_TEMPLATE_HASH_LABEL))
    {
        hash == pod_template_hash
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::parquet_cache::{ParquetCacheInstanceSet, ParquetCacheSpec};
    use crate::controller::parquet_cache_set::InstanceStatus;
    use crate::controller::{ParquetCacheSet, ParquetCacheSetStatus, SHORT_WAIT};
    use crate::data_types::{InstanceState, State};
    use hyper::Body;
    use kube::client::ClientBuilder;
    use kube::ResourceExt;
    use kube_test::{AsHandler, ResourceHandler, Service};
    use std::ops::Sub;
    use std::sync::Arc;

    #[tokio::test]
    async fn create_config_map() {
        let ns = "create_config_map";
        let name = "parquet-cache";
        let fixture: Fixture = Default::default();
        let pc = fixture.parquet_caches.set(ns, name, Default::default());
        let uid = pc.metadata.uid.clone().unwrap_or_default();

        fixture.reconcile(ns, pc).await.unwrap();

        let cm = fixture.config_maps.get(ns, name).unwrap();
        assert_eq!(ns, cm.metadata.namespace.as_ref().unwrap());
        assert_eq!(name, cm.metadata.name.as_ref().unwrap());
        assert_eq!(uid, cm.metadata.owner_references.as_ref().unwrap()[0].uid);
        assert!(!cm.data.as_ref().unwrap().get("current").unwrap().is_empty());
        assert!(!cm.data.as_ref().unwrap().get("next").unwrap().is_empty());
    }

    #[tokio::test]
    async fn create_config_map_no_fail_on_existing() {
        let ns = "create_config_map_no_fail_on_existing";
        let name = "parquet-cache";
        let fixture: Fixture = Default::default();
        fixture.config_maps.set(ns, name, Default::default());
        let pc = fixture.parquet_caches.set(ns, name, Default::default());

        fixture.reconcile(ns, pc).await.unwrap();

        let cm = fixture.config_maps.get(ns, name).unwrap();
        assert_eq!(ns, cm.metadata.namespace.as_ref().unwrap());
        assert_eq!(name, cm.metadata.name.as_ref().unwrap());
    }

    #[tokio::test]
    async fn create_initial_parquet_cache_set_at_full_size() {
        let ns = "create_initial_parquet_cache_set_at_full_size";
        let name = "parquet-cache";
        let fixture: Fixture = Default::default();
        let pc = fixture.parquet_caches.set(
            ns,
            name,
            ParquetCache {
                spec: ParquetCacheSpec {
                    replicas: Some(5),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        let uid = pc.metadata.uid.clone().unwrap_or_default();
        let template_hash = hash_object(&pc.parquet_cache_set_template()).unwrap();

        fixture.reconcile(ns, pc.clone()).await.unwrap();

        let parquet_cache_sets = fixture
            .parquet_cache_sets
            .all(ns)
            .into_iter()
            .filter(|pcs| pcs.owner_references().iter().any(|or| or.uid == uid))
            .collect::<Vec<_>>();

        assert_eq!(1, parquet_cache_sets.len());
        let pcs = &parquet_cache_sets[0];
        assert_eq!(
            &template_hash,
            pcs.metadata
                .labels
                .as_ref()
                .and_then(|map| map.get(POD_TEMPLATE_HASH_LABEL))
                .unwrap()
        );
        assert_eq!(
            5,
            pcs.spec
                .replica_suffixes
                .as_ref()
                .map(Vec::len)
                .unwrap_or_default()
        );

        let cm = fixture.config_maps.get(ns, name).unwrap();
        let current = cm.data.as_ref().unwrap().get("current").unwrap().clone();
        let next = cm.data.as_ref().unwrap().get("next").unwrap().clone();
        assert_eq!(current, next);

        let pcis = serde_json::from_str::<ParquetCacheInstanceSet>(&current).unwrap();
        assert_eq!(5, pcis.instances.len());
    }

    #[tokio::test]
    async fn old_parquet_cache_set_removed() {
        let ns = "old_parquet_cache_set_removed";
        let name = "parquet-cache";
        let fixture: Fixture = Default::default();
        let pc = fixture.parquet_caches.set(ns, name, Default::default());
        let pcs1_name = format!("{name}-aaaaaaaaaa");
        fixture.parquet_cache_sets.set(
            ns,
            &pcs1_name,
            ParquetCacheSet {
                metadata: ObjectMeta {
                    labels: Some(BTreeMap::from([(
                        String::from(POD_TEMPLATE_HASH_LABEL),
                        String::from("aaaaaaaaaa"),
                    )])),
                    owner_references: Some(vec![owner_reference(&pc)]),
                    ..Default::default()
                },
                spec: ParquetCacheSetSpec {
                    replica_suffixes: Some(vec![]),
                    ..Default::default()
                },
                status: Some(ParquetCacheSetStatus {
                    pods: Some(BTreeMap::from([])),
                }),
            },
        );
        let pcs2_name = format!("{name}-bbbbbbbbbb");
        fixture.parquet_cache_sets.set(
            ns,
            &pcs2_name,
            ParquetCacheSet {
                metadata: ObjectMeta {
                    labels: Some(BTreeMap::from([(
                        String::from(POD_TEMPLATE_HASH_LABEL),
                        String::from("bbbbbbbbbb"),
                    )])),
                    owner_references: Some(vec![owner_reference(&pc)]),
                    ..Default::default()
                },
                spec: ParquetCacheSetSpec {
                    replica_suffixes: Some(vec![String::from("0")]),
                    ..Default::default()
                },
                status: Some(ParquetCacheSetStatus {
                    pods: Some(BTreeMap::from([(
                        format!("{pcs2_name}-0"),
                        Default::default(),
                    )])),
                }),
            },
        );
        let template_hash = hash_object(&pc.parquet_cache_set_template()).unwrap();
        let pcs3_name = format!("{name}-{template_hash}");
        fixture.parquet_cache_sets.set(
            ns,
            &pcs3_name,
            ParquetCacheSet {
                metadata: ObjectMeta {
                    labels: Some(BTreeMap::from([(
                        String::from(POD_TEMPLATE_HASH_LABEL),
                        template_hash.clone(),
                    )])),
                    owner_references: Some(vec![owner_reference(&pc)]),
                    ..Default::default()
                },
                spec: ParquetCacheSetSpec {
                    replica_suffixes: Some(vec![]),
                    ..Default::default()
                },
                status: Some(ParquetCacheSetStatus {
                    pods: Some(BTreeMap::from([])),
                }),
            },
        );

        fixture.reconcile(ns, pc).await.unwrap();

        assert!(fixture.parquet_cache_sets.get(ns, &pcs1_name).is_none());
        assert!(fixture.parquet_cache_sets.get(ns, &pcs2_name).is_some());
        assert!(fixture.parquet_cache_sets.get(ns, &pcs3_name).is_some());
    }

    #[tokio::test]
    async fn warming_pods_retry_shortly() {
        let ns = "warming_pods_retry_shortly";
        let name = "parquet-cache";
        let fixture: Fixture = Default::default();
        let pc = fixture.parquet_caches.set(
            ns,
            name,
            ParquetCache {
                status: Some(ParquetCacheStatus {
                    current: ParquetCacheInstanceSet {
                        revision: 1,
                        instances: vec![format!("{name}-aaaaaaaaaa-0")],
                    },
                    next: ParquetCacheInstanceSet {
                        revision: 2,
                        instances: vec![format!("{name}-bbbbbbbbbb-0")],
                    },
                }),
                ..Default::default()
            },
        );

        let pcs1_name = format!("{name}-aaaaaaaaaa");
        fixture.parquet_cache_sets.set(
            ns,
            &pcs1_name,
            ParquetCacheSet {
                metadata: ObjectMeta {
                    labels: Some(BTreeMap::from([(
                        String::from(POD_TEMPLATE_HASH_LABEL),
                        String::from("aaaaaaaaaa"),
                    )])),
                    owner_references: Some(vec![owner_reference(&pc)]),
                    ..Default::default()
                },
                spec: ParquetCacheSetSpec {
                    replica_suffixes: Some(vec![]),
                    ..Default::default()
                },
                status: Some(ParquetCacheSetStatus {
                    pods: Some(BTreeMap::from([(
                        format!("{name}-aaaaaaaaaa-0"),
                        InstanceStatus {
                            phase: Some(String::from("Running")),
                            state: Some(State {
                                state: InstanceState::Warming,
                                state_changed: chrono::Utc::now()
                                    .sub(Duration::from_secs(600))
                                    .timestamp(),
                                current_node_set_revision: 1,
                                next_node_set_revision: 2,
                            }),
                        },
                    )])),
                }),
            },
        );

        let pcs2_name = format!("{name}-bbbbbbbbbb");
        fixture.parquet_cache_sets.set(
            ns,
            &pcs2_name,
            ParquetCacheSet {
                metadata: ObjectMeta {
                    labels: Some(BTreeMap::from([(
                        String::from(POD_TEMPLATE_HASH_LABEL),
                        String::from("bbbbbbbbbb"),
                    )])),
                    owner_references: Some(vec![owner_reference(&pc)]),
                    ..Default::default()
                },
                spec: ParquetCacheSetSpec {
                    replica_suffixes: Some(vec![]),
                    ..Default::default()
                },
                status: Some(ParquetCacheSetStatus {
                    pods: Some(BTreeMap::from([(
                        format!("{name}-bbbbbbbbbb-0"),
                        InstanceStatus {
                            phase: Some(String::from("Running")),
                            state: Some(State {
                                state: InstanceState::Warming,
                                state_changed: chrono::Utc::now()
                                    .sub(Duration::from_secs(30))
                                    .timestamp(),
                                current_node_set_revision: 1,
                                next_node_set_revision: 2,
                            }),
                        },
                    )])),
                }),
            },
        );

        assert_eq!(SHORT_WAIT, fixture.reconcile(ns, pc).await.unwrap());
    }

    #[tokio::test]
    async fn warm_pods_update_status() {
        let ns = "warm_pods_update_status";
        let name = "parquet-cache";
        let fixture: Fixture = Default::default();
        let pc = fixture.parquet_caches.set(
            ns,
            name,
            ParquetCache {
                status: Some(ParquetCacheStatus {
                    current: ParquetCacheInstanceSet {
                        revision: 1,
                        instances: vec![format!("{name}-aaaaaaaaaa-0")],
                    },
                    next: ParquetCacheInstanceSet {
                        revision: 2,
                        instances: vec![format!("{name}-bbbbbbbbbb-0")],
                    },
                }),
                ..Default::default()
            },
        );

        let pcs1_name = format!("{name}-aaaaaaaaaa");
        fixture.parquet_cache_sets.set(
            ns,
            &pcs1_name,
            ParquetCacheSet {
                metadata: ObjectMeta {
                    labels: Some(BTreeMap::from([(
                        String::from(POD_TEMPLATE_HASH_LABEL),
                        String::from("aaaaaaaaaa"),
                    )])),
                    owner_references: Some(vec![owner_reference(&pc)]),
                    ..Default::default()
                },
                spec: ParquetCacheSetSpec {
                    replica_suffixes: Some(vec![]),
                    ..Default::default()
                },
                status: Some(ParquetCacheSetStatus {
                    pods: Some(BTreeMap::from([(
                        format!("{name}-aaaaaaaaaa-0"),
                        InstanceStatus {
                            phase: Some(String::from("Running")),
                            state: Some(State {
                                state: InstanceState::Warming,
                                state_changed: chrono::Utc::now()
                                    .sub(Duration::from_secs(600))
                                    .timestamp(),
                                current_node_set_revision: 1,
                                next_node_set_revision: 2,
                            }),
                        },
                    )])),
                }),
            },
        );

        let pcs2_name = format!("{name}-bbbbbbbbbb");
        fixture.parquet_cache_sets.set(
            ns,
            &pcs2_name,
            ParquetCacheSet {
                metadata: ObjectMeta {
                    labels: Some(BTreeMap::from([(
                        String::from(POD_TEMPLATE_HASH_LABEL),
                        String::from("bbbbbbbbbb"),
                    )])),
                    owner_references: Some(vec![owner_reference(&pc)]),
                    ..Default::default()
                },
                spec: ParquetCacheSetSpec {
                    replica_suffixes: Some(vec![]),
                    ..Default::default()
                },
                status: Some(ParquetCacheSetStatus {
                    pods: Some(BTreeMap::from([(
                        format!("{name}-bbbbbbbbbb-0"),
                        InstanceStatus {
                            phase: Some(String::from("Running")),
                            state: Some(State {
                                state: InstanceState::Warming,
                                state_changed: chrono::Utc::now()
                                    .sub(Duration::from_secs(30))
                                    .timestamp(),
                                current_node_set_revision: 1,
                                next_node_set_revision: 2,
                            }),
                        },
                    )])),
                }),
            },
        );

        fixture.reconcile(ns, pc).await.unwrap();

        let status = fixture
            .parquet_caches
            .get(ns, name)
            .unwrap()
            .status
            .unwrap();
        assert_eq!(status.next, status.current);
    }

    #[tokio::test]
    async fn cooling_pods_retry_shortly() {
        let ns = "cooling_pods_retry_shortly";
        let name = "parquet-cache";
        let fixture: Fixture = Default::default();
        let pc = fixture.parquet_caches.set(
            ns,
            name,
            ParquetCache {
                status: Some(ParquetCacheStatus {
                    current: ParquetCacheInstanceSet {
                        revision: 2,
                        instances: vec![format!("{name}-bbbbbbbbbb-0")],
                    },
                    next: ParquetCacheInstanceSet {
                        revision: 2,
                        instances: vec![format!("{name}-bbbbbbbbbb-0")],
                    },
                }),
                ..Default::default()
            },
        );

        let pcs1_name = format!("{name}-aaaaaaaaaa");
        fixture.parquet_cache_sets.set(
            ns,
            &pcs1_name,
            ParquetCacheSet {
                metadata: ObjectMeta {
                    labels: Some(BTreeMap::from([(
                        String::from(POD_TEMPLATE_HASH_LABEL),
                        String::from("aaaaaaaaaa"),
                    )])),
                    owner_references: Some(vec![owner_reference(&pc)]),
                    ..Default::default()
                },
                spec: ParquetCacheSetSpec {
                    replica_suffixes: Some(vec![]),
                    ..Default::default()
                },
                status: Some(ParquetCacheSetStatus {
                    pods: Some(BTreeMap::from([(
                        format!("{name}-aaaaaaaaaa-0"),
                        InstanceStatus {
                            phase: Some(String::from("Running")),
                            state: Some(State {
                                state: InstanceState::Warming,
                                state_changed: chrono::Utc::now()
                                    .sub(Duration::from_secs(600))
                                    .timestamp(),
                                current_node_set_revision: 2,
                                next_node_set_revision: 2,
                            }),
                        },
                    )])),
                }),
            },
        );

        let pcs2_name = format!("{name}-bbbbbbbbbb");
        fixture.parquet_cache_sets.set(
            ns,
            &pcs2_name,
            ParquetCacheSet {
                metadata: ObjectMeta {
                    labels: Some(BTreeMap::from([(
                        String::from(POD_TEMPLATE_HASH_LABEL),
                        String::from("bbbbbbbbbb"),
                    )])),
                    owner_references: Some(vec![owner_reference(&pc)]),
                    ..Default::default()
                },
                spec: ParquetCacheSetSpec {
                    replica_suffixes: Some(vec![]),
                    ..Default::default()
                },
                status: Some(ParquetCacheSetStatus {
                    pods: Some(BTreeMap::from([(
                        format!("{name}-bbbbbbbbbb-0"),
                        InstanceStatus {
                            phase: Some(String::from("Running")),
                            state: Some(State {
                                state: InstanceState::Warming,
                                state_changed: chrono::Utc::now()
                                    .sub(Duration::from_secs(30))
                                    .timestamp(),
                                current_node_set_revision: 2,
                                next_node_set_revision: 2,
                            }),
                        },
                    )])),
                }),
            },
        );

        assert_eq!(SHORT_WAIT, fixture.reconcile(ns, pc).await.unwrap());
    }

    #[tokio::test]
    async fn cooled_pods_are_removed() {
        let ns = "cooled_pods_are_removed";
        let name = "parquet-cache";
        let fixture: Fixture = Default::default();
        let pc = fixture.parquet_caches.set(
            ns,
            name,
            ParquetCache {
                status: Some(ParquetCacheStatus {
                    current: ParquetCacheInstanceSet {
                        revision: 2,
                        instances: vec![format!("{name}-bbbbbbbbbb-0")],
                    },
                    next: ParquetCacheInstanceSet {
                        revision: 2,
                        instances: vec![format!("{name}-bbbbbbbbbb-0")],
                    },
                }),
                ..Default::default()
            },
        );

        let pcs1_name = format!("{name}-aaaaaaaaaa");
        fixture.parquet_cache_sets.set(
            ns,
            &pcs1_name,
            ParquetCacheSet {
                metadata: ObjectMeta {
                    labels: Some(BTreeMap::from([(
                        String::from(POD_TEMPLATE_HASH_LABEL),
                        String::from("aaaaaaaaaa"),
                    )])),
                    owner_references: Some(vec![owner_reference(&pc)]),
                    ..Default::default()
                },
                spec: ParquetCacheSetSpec {
                    replica_suffixes: Some(vec![]),
                    ..Default::default()
                },
                status: Some(ParquetCacheSetStatus {
                    pods: Some(BTreeMap::from([(
                        format!("{name}-aaaaaaaaaa-0"),
                        InstanceStatus {
                            phase: Some(String::from("Running")),
                            state: Some(State {
                                state: InstanceState::Warming,
                                state_changed: chrono::Utc::now()
                                    .sub(Duration::from_secs(600))
                                    .timestamp(),
                                current_node_set_revision: 2,
                                next_node_set_revision: 2,
                            }),
                        },
                    )])),
                }),
            },
        );

        let pcs2_name = format!("{name}-bbbbbbbbbb");
        fixture.parquet_cache_sets.set(
            ns,
            &pcs2_name,
            ParquetCacheSet {
                metadata: ObjectMeta {
                    labels: Some(BTreeMap::from([(
                        String::from(POD_TEMPLATE_HASH_LABEL),
                        String::from("bbbbbbbbbb"),
                    )])),
                    owner_references: Some(vec![owner_reference(&pc)]),
                    ..Default::default()
                },
                spec: ParquetCacheSetSpec {
                    replica_suffixes: Some(vec![]),
                    ..Default::default()
                },
                status: Some(ParquetCacheSetStatus {
                    pods: Some(BTreeMap::from([(
                        format!("{name}-bbbbbbbbbb-0"),
                        InstanceStatus {
                            phase: Some(String::from("Running")),
                            state: Some(State {
                                state: InstanceState::Warming,
                                state_changed: chrono::Utc::now()
                                    .sub(Duration::from_secs(400))
                                    .timestamp(),
                                current_node_set_revision: 2,
                                next_node_set_revision: 2,
                            }),
                        },
                    )])),
                }),
            },
        );

        fixture.reconcile(ns, pc).await.unwrap();

        let pcs1 = fixture.parquet_cache_sets.get(ns, pcs1_name).unwrap();
        assert!(
            pcs1.spec.replica_suffixes.is_none() || pcs1.spec.replica_suffixes.unwrap().is_empty()
        );
    }

    #[tokio::test]
    async fn resizing_recreates_everything() {
        let ns = "resizing_recreates_everything";
        let name = "parquet-cache";
        let fixture: Fixture = Default::default();
        let pc = fixture.parquet_caches.set(
            ns,
            name,
            ParquetCache {
                spec: ParquetCacheSpec {
                    replicas: Some(2),
                    ..Default::default()
                },
                status: Some(ParquetCacheStatus {
                    current: ParquetCacheInstanceSet {
                        revision: 1,
                        instances: vec![format!("{name}-aaaaaaaaaa-0")],
                    },
                    next: ParquetCacheInstanceSet {
                        revision: 1,
                        instances: vec![format!("{name}-aaaaaaaaaa-0")],
                    },
                }),
                ..Default::default()
            },
        );

        let pcs1_name = format!("{name}-aaaaaaaaaa");
        fixture.parquet_cache_sets.set(
            ns,
            &pcs1_name,
            ParquetCacheSet {
                metadata: ObjectMeta {
                    labels: Some(BTreeMap::from([(
                        String::from(POD_TEMPLATE_HASH_LABEL),
                        String::from("aaaaaaaaaa"),
                    )])),
                    owner_references: Some(vec![owner_reference(&pc)]),
                    ..Default::default()
                },
                spec: ParquetCacheSetSpec {
                    replica_suffixes: Some(vec![String::from("0")]),
                    ..Default::default()
                },
                status: Some(ParquetCacheSetStatus {
                    pods: Some(BTreeMap::from([(
                        format!("{name}-aaaaaaaaaa-0"),
                        InstanceStatus {
                            phase: Some(String::from("Running")),
                            state: Some(State {
                                state: InstanceState::Warming,
                                state_changed: chrono::Utc::now()
                                    .sub(Duration::from_secs(600))
                                    .timestamp(),
                                current_node_set_revision: 1,
                                next_node_set_revision: 1,
                            }),
                        },
                    )])),
                }),
            },
        );
        let template = pc.parquet_cache_set_template();
        let hash = hash_object(&template).unwrap();
        let pcs2_name = pc.parquet_cache_set_name(&hash).unwrap();

        fixture.reconcile(ns, pc).await.unwrap();

        let pcs1 = fixture.parquet_cache_sets.get(ns, pcs1_name).unwrap();
        assert!(
            pcs1.spec.replica_suffixes.is_none() || pcs1.spec.replica_suffixes.unwrap().is_empty()
        );
        let pcs2 = fixture.parquet_cache_sets.get(ns, pcs2_name).unwrap();
        assert_eq!(2, pcs2.spec.replica_suffixes.unwrap().len())
    }

    #[tokio::test]
    async fn template_change_starts_migration() {
        let ns = "template_change_starts_migration";
        let name = "parquet-cache";
        let fixture: Fixture = Default::default();
        let pc = fixture.parquet_caches.set(
            ns,
            name,
            ParquetCache {
                status: Some(ParquetCacheStatus {
                    current: ParquetCacheInstanceSet {
                        revision: 1,
                        instances: vec![format!("{name}-aaaaaaaaaa-0")],
                    },
                    next: ParquetCacheInstanceSet {
                        revision: 1,
                        instances: vec![format!("{name}-aaaaaaaaaa-0")],
                    },
                }),
                ..Default::default()
            },
        );

        let pcs1_name = format!("{name}-aaaaaaaaaa");
        fixture.parquet_cache_sets.set(
            ns,
            &pcs1_name,
            ParquetCacheSet {
                metadata: ObjectMeta {
                    labels: Some(BTreeMap::from([(
                        String::from(POD_TEMPLATE_HASH_LABEL),
                        String::from("aaaaaaaaaa"),
                    )])),
                    owner_references: Some(vec![owner_reference(&pc)]),
                    ..Default::default()
                },
                spec: ParquetCacheSetSpec {
                    replica_suffixes: Some(vec![String::from("0")]),
                    ..Default::default()
                },
                status: Some(ParquetCacheSetStatus {
                    pods: Some(BTreeMap::from([(
                        format!("{name}-aaaaaaaaaa-0"),
                        InstanceStatus {
                            phase: Some(String::from("Running")),
                            state: Some(State {
                                state: InstanceState::Warming,
                                state_changed: chrono::Utc::now()
                                    .sub(Duration::from_secs(600))
                                    .timestamp(),
                                current_node_set_revision: 1,
                                next_node_set_revision: 1,
                            }),
                        },
                    )])),
                }),
            },
        );
        let template = pc.parquet_cache_set_template();
        let hash = hash_object(&template).unwrap();
        let pcs2_name = pc.parquet_cache_set_name(&hash).unwrap();

        fixture.reconcile(ns, pc).await.unwrap();

        let pc = fixture.parquet_caches.get(ns, name).unwrap();
        let status = &pc.status.unwrap();
        assert!(status.current.revision < status.next.revision);
        assert_eq!(format!("{name}-aaaaaaaaaa-0"), status.current.instances[0]);
        assert_eq!(format!("{name}-{hash}-0"), status.next.instances[0]);

        let pcs1 = fixture.parquet_cache_sets.get(ns, pcs1_name).unwrap();
        assert_eq!(1, pcs1.spec.replica_suffixes.unwrap().len());
        let pcs2 = fixture.parquet_cache_sets.get(ns, pcs2_name).unwrap();
        assert_eq!(1, pcs2.spec.replica_suffixes.unwrap().len())
    }

    #[tokio::test]
    async fn one_pod_migrated_at_a_time() {
        let ns = "template_change_starts_migration";
        let name = "parquet-cache";
        let fixture: Fixture = Default::default();
        let pc = fixture.parquet_caches.set(
            ns,
            name,
            ParquetCache {
                spec: ParquetCacheSpec {
                    replicas: Some(3),
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        let pcs1_name = format!("{name}-aaaaaaaaaa");
        fixture.parquet_cache_sets.set(
            ns,
            &pcs1_name,
            ParquetCacheSet {
                metadata: ObjectMeta {
                    labels: Some(BTreeMap::from([(
                        String::from(POD_TEMPLATE_HASH_LABEL),
                        String::from("aaaaaaaaaa"),
                    )])),
                    owner_references: Some(vec![owner_reference(&pc)]),
                    ..Default::default()
                },
                spec: ParquetCacheSetSpec {
                    replica_suffixes: Some(vec![String::from("1"), String::from("2")]),
                    ..Default::default()
                },
                status: Some(ParquetCacheSetStatus {
                    pods: Some(BTreeMap::from([
                        (
                            format!("{name}-aaaaaaaaaa-1"),
                            InstanceStatus {
                                phase: Some(String::from("Running")),
                                state: Some(State {
                                    state: InstanceState::Warming,
                                    state_changed: chrono::Utc::now()
                                        .sub(Duration::from_secs(1800))
                                        .timestamp(),
                                    current_node_set_revision: 2,
                                    next_node_set_revision: 2,
                                }),
                            },
                        ),
                        (
                            format!("{name}-aaaaaaaaaa-2"),
                            InstanceStatus {
                                phase: Some(String::from("Running")),
                                state: Some(State {
                                    state: InstanceState::Warming,
                                    state_changed: chrono::Utc::now()
                                        .sub(Duration::from_secs(1800))
                                        .timestamp(),
                                    current_node_set_revision: 2,
                                    next_node_set_revision: 2,
                                }),
                            },
                        ),
                    ])),
                }),
            },
        );
        let template = pc.parquet_cache_set_template();
        let hash = hash_object(&template).unwrap();
        let pcs2_name = pc.parquet_cache_set_name(&hash).unwrap();
        fixture.parquet_cache_sets.set(
            ns,
            &pcs2_name,
            ParquetCacheSet {
                metadata: ObjectMeta {
                    labels: Some(BTreeMap::from([(
                        String::from(POD_TEMPLATE_HASH_LABEL),
                        hash.clone(),
                    )])),
                    owner_references: Some(vec![owner_reference(&pc)]),
                    ..Default::default()
                },
                spec: ParquetCacheSetSpec {
                    replica_suffixes: Some(vec![String::from("0")]),
                    ..Default::default()
                },
                status: Some(ParquetCacheSetStatus {
                    pods: Some(BTreeMap::from([(
                        format!("{name}-{hash}-0"),
                        InstanceStatus {
                            phase: Some(String::from("Running")),
                            state: Some(State {
                                state: InstanceState::Warming,
                                state_changed: chrono::Utc::now()
                                    .sub(Duration::from_secs(600))
                                    .timestamp(),
                                current_node_set_revision: 2,
                                next_node_set_revision: 2,
                            }),
                        },
                    )])),
                }),
            },
        );

        let pc = fixture.parquet_caches.set(
            ns,
            name,
            ParquetCache {
                status: Some(ParquetCacheStatus {
                    current: ParquetCacheInstanceSet {
                        revision: 2,
                        instances: vec![
                            format!("{name}-{hash}-0"),
                            format!("{name}-aaaaaaaaaa-1"),
                            format!("{name}-aaaaaaaaaa-2"),
                        ],
                    },
                    next: ParquetCacheInstanceSet {
                        revision: 2,
                        instances: vec![
                            format!("{name}-{hash}-0"),
                            format!("{name}-aaaaaaaaaa-1"),
                            format!("{name}-aaaaaaaaaa-2"),
                        ],
                    },
                }),
                ..pc
            },
        );

        fixture.reconcile(ns, pc).await.unwrap();

        let pc = fixture.parquet_caches.get(ns, name).unwrap();
        let status = &pc.status.unwrap();
        assert!(status.current.revision < status.next.revision);
        assert_eq!(status.current.instances[0], status.next.instances[0]);
        assert_eq!(format!("{name}-aaaaaaaaaa-1"), status.current.instances[1]);
        assert_eq!(format!("{name}-{hash}-1"), status.next.instances[1]);
        assert_eq!(status.current.instances[2], status.next.instances[2]);

        let pcs1 = fixture.parquet_cache_sets.get(ns, pcs1_name).unwrap();
        assert_eq!(2, pcs1.spec.replica_suffixes.unwrap().len());
        let pcs2 = fixture.parquet_cache_sets.get(ns, pcs2_name).unwrap();
        assert_eq!(2, pcs2.spec.replica_suffixes.unwrap().len())
    }

    #[derive(Debug, Default)]
    struct Fixture {
        pub config_maps: Arc<ResourceHandler<ConfigMap>>,
        pub parquet_cache_sets: Arc<ResourceHandler<ParquetCacheSet>>,
        pub parquet_caches: Arc<ResourceHandler<ParquetCache>>,
    }

    impl Fixture {
        fn service(&self) -> Service {
            let service = Service::new();
            service.add_handler(self.config_maps.as_handler());
            service.add_handler(self.parquet_cache_sets.as_handler());
            service.add_handler(self.parquet_caches.as_handler());
            service
        }

        async fn reconcile(
            &self,
            ns: impl Into<String> + Send,
            pc: ParquetCache,
        ) -> Result<Duration> {
            let service = self.service();
            let client = ClientBuilder::new(service, ns).build::<Body>();
            let mut controller = ParquetCacheController::new(pc, client);
            let hnd = tokio::spawn(async move { controller.reconcile().await });
            hnd.await.unwrap()
        }
    }
}
