use super::{
    kube_util::owner_reference, parquet_cache_set::InstanceStatus, state_service, Error,
    ParquetCacheSet, ParquetCacheSetStatus, Result, CONTROLLER_NAME, LONG_WAIT,
    PARQUET_CACHE_REPLICAS_LABEL, POD_TEMPLATE_HASH_LABEL, SHORT_WAIT,
};
use futures::StreamExt;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::api::{ListParams, PostParams};
use kube::runtime::controller::Action;
use kube::runtime::watcher::Config;
use kube::runtime::Controller;
use kube::{Api, Client, ResourceExt};
use observability_deps::tracing::{error, info};
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// Start a new controller task to reconcile [ParquetCacheSet] objects.
pub fn spawn_controller(client: Client, ns: Option<String>) -> JoinHandle<Result<(), kube::Error>> {
    tokio::spawn(run_controller(client, ns))
}

async fn run_controller(client: Client, ns: Option<String>) -> Result<(), kube::Error> {
    let parquet_cache_set_api = match &ns {
        Some(ns) => Api::<ParquetCacheSet>::namespaced(client.clone(), ns),
        None => Api::<ParquetCacheSet>::all(client.clone()),
    };
    let pod_api = match &ns {
        Some(ns) => Api::<Pod>::namespaced(client.clone(), ns),
        None => Api::<Pod>::all(client.clone()),
    };

    Controller::new(parquet_cache_set_api, Default::default())
        .owns(
            pod_api,
            Config::default().labels(&format!(
                "{},{}",
                PARQUET_CACHE_REPLICAS_LABEL, POD_TEMPLATE_HASH_LABEL
            )),
        )
        .run(
            reconcile,
            error_policy,
            Arc::new(Context {
                client,
                state_service: Default::default(),
            }),
        )
        .for_each(|_| futures::future::ready(()))
        .await;
    Ok(())
}

async fn reconcile(obj: Arc<ParquetCacheSet>, ctx: Arc<Context>) -> Result<Action> {
    let namespace = obj.metadata.namespace.as_deref();
    let name = obj.name_any();
    info!(namespace, name, "reconcile request");
    let sleep = ParquetCacheSetController::new(
        obj.as_ref().clone(),
        ctx.state_service.clone(),
        ctx.client.clone(),
    )
    .reconcile()
    .await?;
    Ok(Action::requeue(sleep))
}

fn error_policy(_object: Arc<ParquetCacheSet>, err: &Error, _ctx: Arc<Context>) -> Action {
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
    state_service: state_service::Client,
}

/// Controller for the ParquetCacheSet custom resource. This controller maintains the set of pods
/// created by a ParquetCacheSet.
#[derive(Debug)]
struct ParquetCacheSetController {
    parquet_cache_set_api: Api<ParquetCacheSet>,
    pod_api: Api<Pod>,
    state_service: state_service::Client,

    parquet_cache_set: ParquetCacheSet,
}

impl ParquetCacheSetController {
    /// Create a new ParquetCacheSetController instance for the provided [ParquetCacheSet]
    /// and [Client].
    fn new(
        parquet_cache_set: ParquetCacheSet,
        state_service: state_service::Client,
        client: Client,
    ) -> Self {
        let ns = parquet_cache_set.metadata.namespace.as_ref().unwrap();
        let parquet_cache_sets: Api<ParquetCacheSet> = Api::namespaced(client.clone(), ns);
        let pods: Api<Pod> = Api::namespaced(client.clone(), ns);

        Self {
            parquet_cache_set_api: parquet_cache_sets,
            pod_api: pods,
            state_service,
            parquet_cache_set,
        }
    }

    /// Perform the business logic required to move the ParquetCacheSet state forward towards the
    /// desired state.
    async fn reconcile(&mut self) -> Result<Duration> {
        let duration = self.reconcile_inner().await?;

        // Ensure the status is always kept up-to-date.
        self.parquet_cache_set_api
            .replace_status(
                self.parquet_cache_set.metadata.name.as_ref().unwrap(),
                &Default::default(),
                serde_json::to_vec(&self.parquet_cache_set)?,
            )
            .await?;
        Ok(duration)
    }

    async fn reconcile_inner(&mut self) -> Result<Duration> {
        let prefix = self.parquet_cache_set.metadata.name.as_ref().unwrap();
        let pod_names = self
            .parquet_cache_set
            .spec
            .replica_suffixes
            .as_ref()
            .map_or_else(BTreeSet::new, |v| {
                v.iter()
                    .map(|suffix| format!("{prefix}-{suffix}"))
                    .collect::<BTreeSet<_>>()
            });

        self.delete_removed(&pod_names).await?;
        self.create_missing(&pod_names).await?;
        self.update_status(&pod_names).await?;

        let warming = self
            .status_mut()
            .pods
            .as_ref()
            .map(|pods| pods.iter().any(|(_, status)| status.is_warming()))
            .unwrap_or(false);

        // If there are cache pods in the warming state then check them in a minute, otherwise wait
        // for an hour, or for a state change.
        Ok(if warming { SHORT_WAIT } else { LONG_WAIT })
    }

    async fn delete_removed(&mut self, pod_names: &BTreeSet<String>) -> Result<()> {
        let pods = self
            .pod_api
            .list(&ListParams {
                label_selector: self.parquet_cache_set.selectors(),
                ..Default::default()
            })
            .await?;
        let to_delete = pods
            .iter()
            .filter_map(|pod| pod.metadata.name.as_ref())
            .filter(|&name| !pod_names.contains(name))
            .collect::<Vec<_>>();

        for pod_name in to_delete {
            info!(name = pod_name, "Deleting Pod");
            self.pod_api.delete(pod_name, &Default::default()).await?;
        }
        Ok(())
    }

    async fn create_missing(&mut self, pods: &BTreeSet<String>) -> Result<()> {
        for pod in pods {
            if !self.pod_exists(pod).await? {
                info!(name = pod, "Creating Pod");
                self.create_pod(pod.clone()).await?;
            }
        }
        Ok(())
    }

    async fn update_status(&mut self, pod_names: &BTreeSet<String>) -> Result<()> {
        if let Some(pods) = self.status_mut().pods.as_mut() {
            pods.clear();
        }
        for name in pod_names {
            let pod = self.pod_api.get_status(name).await?;
            let phase = pod.status.clone().and_then(|status| status.phase);
            let state = match phase.as_deref() {
                Some("Running") => {
                    self.state_service
                        .state(&pod, &self.parquet_cache_set.spec.state_port)
                        .await?
                }
                _ => None,
            };
            self.status_mut()
                .pods
                .get_or_insert(Default::default())
                .insert(name.clone(), InstanceStatus { phase, state });
        }
        Ok(())
    }

    async fn pod_exists(&self, name: &str) -> Result<bool> {
        match self.pod_api.get(name).await {
            Ok(_) => Ok(true),
            Err(kube::Error::Api(error_response)) if error_response.reason == "NotFound" => {
                Ok(false)
            }
            Err(error) => Err(Error::from(error)),
        }
    }

    /// Create a new data cache instance pod.
    async fn create_pod(&self, name: String) -> Result<Pod> {
        let template = self
            .parquet_cache_set
            .spec
            .template
            .clone()
            .unwrap_or_default();
        let pod = Pod {
            metadata: ObjectMeta {
                namespace: self.parquet_cache_set.metadata.namespace.clone(),
                name: Some(name),
                owner_references: Some(vec![self.owner_reference()]),
                ..template.metadata.unwrap_or_default()
            },
            spec: template.spec,
            ..Default::default()
        };
        Ok(self
            .pod_api
            .create(
                &PostParams {
                    dry_run: false,
                    field_manager: Some(CONTROLLER_NAME.to_string()),
                },
                &pod,
            )
            .await?)
    }

    /// Generate an owner reference for the current ParquetCacheSet document.
    fn owner_reference(&self) -> OwnerReference {
        owner_reference(&self.parquet_cache_set)
    }

    fn status_mut(&mut self) -> &mut ParquetCacheSetStatus {
        self.parquet_cache_set
            .status
            .get_or_insert(Default::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::state_service::Request;
    use crate::controller::{ParquetCacheSet, ParquetCacheSetSpec};
    use crate::data_types::{InstanceState, State};
    use hyper::Body;
    use k8s_openapi::api::core::v1::{Pod, PodSpec, PodTemplateSpec};
    use kube::client::ClientBuilder;
    use kube_test::{AsHandler, ResourceHandler, Service};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    #[tokio::test]
    async fn create_pods() {
        let ns = "create_pods";
        let name = "parquet-cache-aaaaaaaaaa";

        let fixture: Fixture = Default::default();

        let pcs = fixture.parquet_cache_sets.set(
            ns,
            name,
            ParquetCacheSet {
                spec: ParquetCacheSetSpec {
                    replica_suffixes: Some(vec![String::from("0"), String::from("1")]),
                    template: Some(PodTemplateSpec {
                        spec: Some(PodSpec {
                            priority: Some(2),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        fixture.reconcile(ns, pcs.clone()).await.unwrap();

        let pods = fixture.pods.all(ns);
        assert_eq!(2, pods.len());

        let mut pod_names = pods
            .iter()
            .map(|pod| pod.metadata.name.as_ref().unwrap().clone())
            .collect::<Vec<_>>();
        pod_names.sort();
        assert_eq!(
            &vec!["parquet-cache-aaaaaaaaaa-0", "parquet-cache-aaaaaaaaaa-1"],
            &pod_names
        );

        // Make sure the provided template has been used, and the pods are owned by the
        // ParquetCacheSet.
        for pod in &pods {
            assert_eq!(2, pod.spec.as_ref().unwrap().priority.unwrap());
            assert_eq!(
                owner_reference(&pcs),
                pod.metadata.owner_references.as_ref().unwrap()[0].clone()
            );
        }
    }

    #[tokio::test]
    async fn remove_pods() {
        let ns = "remove_pods";
        let name = "parquet-cache-aaaaaaaaaa";

        let fixture: Fixture = Default::default();

        let pod0_name = format!("{name}-0");
        let pod1_name = format!("{name}-1");
        let pod2_name = format!("{name}-2");

        let pcs = fixture.parquet_cache_sets.set(
            ns,
            name,
            ParquetCacheSet {
                spec: ParquetCacheSetSpec {
                    replica_suffixes: Some(vec![String::from("1"), String::from("2")]),
                    template: Some(PodTemplateSpec {
                        spec: Some(PodSpec {
                            priority: Some(2),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                status: Some(ParquetCacheSetStatus {
                    pods: Some(BTreeMap::from([
                        (
                            pod0_name.clone(),
                            InstanceStatus {
                                phase: Some(String::from("Running")),
                                state: Some(State {
                                    state: InstanceState::Warming,
                                    ..Default::default()
                                }),
                            },
                        ),
                        (
                            pod1_name.clone(),
                            InstanceStatus {
                                phase: Some(String::from("Running")),
                                state: Some(State {
                                    state: InstanceState::Warming,
                                    ..Default::default()
                                }),
                            },
                        ),
                        (
                            pod2_name.clone(),
                            InstanceStatus {
                                phase: Some(String::from("Running")),
                                state: Some(State {
                                    state: InstanceState::Warming,
                                    ..Default::default()
                                }),
                            },
                        ),
                    ])),
                }),
                ..Default::default()
            },
        );

        fixture.pods.set(
            ns,
            &pod0_name,
            Pod {
                metadata: ObjectMeta {
                    owner_references: Some(vec![owner_reference(&pcs)]),
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        fixture.pods.set(
            ns,
            &pod1_name,
            Pod {
                metadata: ObjectMeta {
                    owner_references: Some(vec![owner_reference(&pcs)]),
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        fixture.pods.set(
            ns,
            &pod2_name,
            Pod {
                metadata: ObjectMeta {
                    owner_references: Some(vec![owner_reference(&pcs)]),
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        fixture.reconcile(ns, pcs).await.unwrap();

        let pods = fixture.pods.all(ns);
        assert_eq!(2, pods.len());

        let mut pod_names = pods
            .iter()
            .map(|pod| pod.metadata.name.as_ref().unwrap().clone())
            .collect::<Vec<_>>();
        pod_names.sort();
        assert_eq!(vec![pod1_name.clone(), pod2_name.clone()], pod_names);
    }

    #[tokio::test]
    async fn warming_pods_refresh_shortly() {
        let ns = "warming_pods_refresh_shortly";
        let name = "parquet-cache-aaaaaaaaaa";

        let mut fixture: Fixture = Default::default();

        let pod0_name = format!("{name}-0");

        let pcs = fixture.parquet_cache_sets.set(
            ns,
            name,
            ParquetCacheSet {
                spec: ParquetCacheSetSpec {
                    replica_suffixes: Some(vec![String::from("0")]),
                    template: Some(PodTemplateSpec {
                        spec: Some(PodSpec {
                            priority: Some(2),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                status: Some(ParquetCacheSetStatus {
                    pods: Some(BTreeMap::from([(
                        pod0_name.clone(),
                        InstanceStatus {
                            phase: Some(String::from("Pending")),
                            state: None,
                        },
                    )])),
                }),
                ..Default::default()
            },
        );

        fixture.pods.set(
            ns,
            &pod0_name,
            Pod {
                metadata: ObjectMeta {
                    owner_references: Some(vec![owner_reference(&pcs)]),
                    ..Default::default()
                },
                status: Some(k8s_openapi::api::core::v1::PodStatus {
                    phase: Some(String::from("Running")),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );

        fixture.instance_state.insert(
            pod0_name.clone(),
            State {
                state: InstanceState::Warming,
                ..Default::default()
            },
        );

        assert_eq!(SHORT_WAIT, fixture.reconcile(ns, pcs).await.unwrap());

        let pcs = fixture.parquet_cache_sets.get(ns, name).unwrap();
        assert_eq!(
            "Running",
            pcs.status
                .as_ref()
                .unwrap()
                .pods
                .as_ref()
                .unwrap()
                .get(&pod0_name)
                .unwrap()
                .phase
                .as_deref()
                .unwrap()
        );
        assert_eq!(
            InstanceState::Warming,
            pcs.status
                .as_ref()
                .unwrap()
                .pods
                .as_ref()
                .unwrap()
                .get(&pod0_name)
                .unwrap()
                .state
                .as_ref()
                .unwrap()
                .state
        );
    }

    #[tokio::test]
    async fn no_status_pods_refresh_shortly() {
        let ns = "no_status_pods_refresh_shortly";
        let name = "parquet-cache-aaaaaaaaaa";

        let fixture: Fixture = Default::default();

        let pod0_name = format!("{name}-0");

        let pcs = fixture.parquet_cache_sets.set(
            ns,
            name,
            ParquetCacheSet {
                spec: ParquetCacheSetSpec {
                    replica_suffixes: Some(vec![String::from("0")]),
                    template: Some(PodTemplateSpec {
                        spec: Some(PodSpec {
                            priority: Some(2),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                status: Some(ParquetCacheSetStatus {
                    pods: Some(BTreeMap::from([(
                        pod0_name.clone(),
                        InstanceStatus {
                            phase: Some(String::from("Pending")),
                            state: None,
                        },
                    )])),
                }),
                ..Default::default()
            },
        );

        fixture.pods.set(
            ns,
            &pod0_name,
            Pod {
                metadata: ObjectMeta {
                    owner_references: Some(vec![owner_reference(&pcs)]),
                    ..Default::default()
                },
                status: Some(k8s_openapi::api::core::v1::PodStatus {
                    phase: Some(String::from("Running")),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );

        assert_eq!(SHORT_WAIT, fixture.reconcile(ns, pcs).await.unwrap());

        let pcs = fixture.parquet_cache_sets.get(ns, name).unwrap();
        assert_eq!(
            "Running",
            pcs.status
                .as_ref()
                .unwrap()
                .pods
                .as_ref()
                .unwrap()
                .get(&pod0_name)
                .unwrap()
                .phase
                .as_deref()
                .unwrap()
        );
        assert!(&pcs
            .status
            .unwrap()
            .pods
            .unwrap()
            .get(&pod0_name)
            .unwrap()
            .state
            .is_none());
    }

    #[derive(Debug, Default)]
    struct Fixture {
        pub parquet_cache_sets: Arc<ResourceHandler<ParquetCacheSet>>,
        pub pods: Arc<ResourceHandler<Pod>>,
        pub instance_state: BTreeMap<String, State>,
    }

    impl Fixture {
        fn service(&self) -> Service {
            let service = Service::new();
            service.add_handler(self.parquet_cache_sets.as_handler());
            service.add_handler(self.pods.as_handler());
            service
        }

        async fn reconcile(
            &self,
            ns: impl Into<String> + Send,
            pcs: ParquetCacheSet,
        ) -> Result<Duration> {
            let service = self.service();
            let client = ClientBuilder::new(service, ns).build::<Body>();
            let state_service_client =
                state_service::Client::new(StateService(self.instance_state.clone()));
            let mut controller = ParquetCacheSetController::new(pcs, state_service_client, client);
            let hnd = tokio::spawn(async move { controller.reconcile().await });
            hnd.await.unwrap()
        }
    }

    #[derive(Debug, Clone)]
    struct StateService(BTreeMap<String, State>);

    impl tower::Service<state_service::Request> for StateService {
        type Response = Option<State>;
        type Error = Box<dyn std::error::Error + Send + Sync>;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: Request) -> Self::Future {
            std::future::ready(Ok(self
                .0
                .get(req.pod.metadata.name.as_deref().unwrap_or_default())
                .cloned()))
        }
    }
}
