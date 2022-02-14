use std::{
    io::ErrorKind,
    sync::Arc,
    time::{Duration, SystemTime},
};

use chrono::{DateTime, Utc};
use dotenv::dotenv;
use futures::StreamExt;
use kube::{api::ListParams, Api, Client as K8sClient};
use kube_runtime::controller::{Context, Controller, ReconcilerAction};
use std::process::Command as Cmd;
use thiserror::Error;
use tracing::*;
use trogging::{cli::LoggingConfig, LogFormat};

use crate::kafka_topic_list::{
    api::KafkaTopicListApi,
    resources::{KafkaTopicList, KafkaTopicListStatus, KafkaTopicListStatusCondition},
};

pub mod kafka_topic_list;

static CONDITION_TYPE_RECONCILED: &str = "Reconciled";
static CONDITION_STATUS_TRUE: &str = "True";
static CONDITION_STATUS_FALSE: &str = "False";

#[derive(Debug, Error)]
enum CatalogError {
    #[error("Malformed KafkaTopicList resource: {message}")]
    MalformedKafkaTopicListResource { message: String },

    #[error("Request to patch status of k8s custom resource failed: {0}")]
    PatchStatusError(#[from] kube::Error),

    #[error("Failed to execute iox binary to update catalog: {0}")]
    IOxBinaryExecFailed(#[from] std::io::Error),

    #[error("Request to update catalog with topic failed: {stderr}")]
    UpdateTopicError { stderr: String },

    #[error("Failed to parse stdout of catalog update command to ID: {0}")]
    TopicIdParseError(#[from] std::num::ParseIntError),
}

// Config defines the runtime configuration variables settable on the command
// line.
//
// These fields are automatically converted into a [Clap] CLI.
//
// This has an `allow(missing_docs)` annotation as otherwise the comment is
// added to the CLI help text.
//
// [Clap]: https://github.com/clap-rs/clap
#[derive(Debug, clap::Parser)]
#[clap(
    name = "iox_gitops_adapter",
    about = "Adapter to configure IOx Catalog from Kubernetes Custom Resources",
    long_about = r#"Kubernetes controller responsible for synchronising the IOx Catalog to cluster configuration in a Kubernetes Custom Resource.

Examples:
    # Run the gitops adapter server:
    iox_gitops_adapter

    # See all configuration options
    iox_gitops_adapter --help
"#,
    version = concat!(env!("CARGO_PKG_VERSION"), " - ", env!("GIT_HASH"))
)]
#[allow(missing_docs)]
pub struct Config {
    /// Configure the log level & filter.
    ///
    /// Example values:
    ///     iox_gitops_adapter=debug
    #[clap(flatten)]
    logging_config: LoggingConfig,

    /// Configure the Kubernetes namespace where custom resources are found.
    ///
    /// Example values:
    ///     namespace=conductor
    #[clap(long = "--namespace", env = "GITOPS_ADAPTER_NAMESPACE")]
    namespace: String,

    /// Configure the Catalog's Postgres DSN.
    ///
    /// Example values:
    ///     catalog-dsn=postgres://postgres:postgres@localhost:5432/iox_shared
    #[clap(long = "--catalog-dsn", env = "GITOPS_ADAPTER_CATALOG_DSN")]
    catalog_dsn: String,

    /// Configure the path to the IOx CLI.
    ///
    /// Example values:
    ///     iox-cli=/usr/bin/influxdb_iox
    #[clap(long = "--iox-cli", env = "GITOPS_ADAPTER_IOX_CLI")]
    iox_cli: String,
}

#[derive(Debug, clap::Parser)]
enum Command {
    Config,
}

impl Config {
    /// Returns the (possibly invalid) log filter string.
    pub fn log_filter(&self) -> &Option<String> {
        &self.logging_config.log_filter
    }

    /// Returns the (possibly invalid) log format string.
    pub fn log_format(&self) -> &LogFormat {
        &self.logging_config.log_format
    }
}

/// Load the config.
///
/// This pulls in config from the following sources, in order of precedence:
///
///     - command line arguments
///     - user set environment variables
///     - .env file contents
///     - pre-configured default values
pub fn load_config() -> Result<Config, Box<dyn std::error::Error>> {
    // Source the .env file before initialising the Config struct - this sets
    // any envs in the file, which the Config struct then uses.
    //
    // Precedence is given to existing env variables.
    match dotenv() {
        Ok(_) => {}
        Err(dotenv::Error::Io(err)) if err.kind() == ErrorKind::NotFound => {
            // Ignore this - a missing env file is not an error,
            // defaults will be applied when initialising the Config struct.
        }
        Err(e) => return Err(Box::new(e)),
    };

    // Load the Config struct - this pulls in any envs set by the user or
    // sourced above, and applies any defaults.
    Ok(clap::Parser::parse())
}

/// Initialise the tracing subscribers.
fn setup_tracing(
    logging_config: &LoggingConfig,
    log_env_var: Option<String>,
) -> Result<trogging::TroggingGuard, trogging::Error> {
    let drop_handle = logging_config
        .to_builder()
        .with_default_log_filter(log_env_var.unwrap_or_else(|| "info".to_string()))
        .install_global()?;

    trace!("logging initialised!");

    Ok(drop_handle)
}

async fn reconcile_topics(
    path_to_iox_binary: &str,
    catalog_dsn: &str,
    topics: &[String],
) -> Result<Vec<u32>, CatalogError> {
    trace!(
        "calling out to {} for topics {:?}",
        path_to_iox_binary,
        topics
    );
    topics
        .iter()
        .map(|topic| {
            match Cmd::new(path_to_iox_binary)
                .arg("catalog")
                .arg("topic")
                .arg("update")
                .arg("--catalog-dsn")
                .arg(catalog_dsn)
                .arg(topic)
                .output()
            {
                Ok(output) => match output.status.success() {
                    true => {
                        trace!(
                            "Updated catalog with kafka topic {}. stdout: {}",
                            topic,
                            String::from_utf8_lossy(&output.stdout).trim()
                        );
                        // The CLI returns an ID on success; try to parse it here to ensure it
                        // worked; not sure that return zero is enough? e.g. --help will return 0.
                        // also, we'd like to print the IDs out later
                        String::from_utf8_lossy(&output.stdout)
                            .trim()
                            .parse::<u32>()
                            .map_err(CatalogError::TopicIdParseError)
                    }
                    false => Err(CatalogError::UpdateTopicError {
                        stderr: String::from_utf8_lossy(&output.stderr).into(),
                    }),
                },
                Err(e) => Err(CatalogError::IOxBinaryExecFailed(e)),
            }
        })
        .collect()
}

/// Controller triggers this whenever our main object or our children changed
async fn reconcile<T>(
    topics: Arc<KafkaTopicList>,
    ctx: Context<Data<T>>,
) -> Result<ReconcilerAction, CatalogError>
where
    T: KafkaTopicListApi,
{
    debug!(
        "got a change to the kafka topic list custom resource: {:?}",
        topics.spec
    );
    let kafka_topic_list_api = ctx.get_ref().kafka_topic_list_api.clone();
    let topics = Arc::new(topics);

    // if CR doesn't contain status field, add it
    let mut topics_status = match &topics.status {
        Some(status) => status.clone(),
        None => KafkaTopicListStatus::default(),
    };
    let kafka_topic_list_name = match &topics.metadata.name {
        Some(n) => n.clone(),
        None => {
            return Err(CatalogError::MalformedKafkaTopicListResource {
                message: "Missing metadata.name field".to_string(),
            })
        }
    };

    // have we seen this update before?
    // NOTE: we may find that we'd prefer to do the reconcile anyway, if it's cheap.
    //       for now this seems okay
    let generation = match topics.metadata.generation {
        Some(gen) => {
            if topics_status.observed_generation() == gen {
                info!("Nothing to reconcile; observedGeneration == generation");
                return Ok(ReconcilerAction {
                    requeue_after: None,
                });
            }
            gen
        }
        _ => {
            return Err(CatalogError::MalformedKafkaTopicListResource {
                message: "Missing metadata.generation field".to_string(),
            })
        }
    };
    // make a note that we've seen this update
    topics_status.set_observed_generation(generation);

    // call out to the iox CLI to update the catalog for each topic name in the list
    let reconcile_result = reconcile_topics(
        &ctx.get_ref().path_to_iox_binary,
        &ctx.get_ref().catalog_dsn,
        topics.spec.topics(),
    )
    .await;

    // update status subresource based on outcome of reconcile
    let now: DateTime<Utc> = SystemTime::now().into();
    let now_str = now.to_rfc3339();
    let prev_condition = topics_status.conditions().get(0);
    let last_transition_time = match prev_condition {
        Some(c) if c.status() == CONDITION_STATUS_TRUE => c.last_transition_time().clone(),
        _ => now_str.clone(),
    };
    let new_status = match &reconcile_result {
        Ok(v) => {
            debug!(
                "Updated catalog with kafka topic list: {:?}. IDs returned: {:?}.",
                topics.spec.topics(),
                v
            );
            KafkaTopicListStatusCondition::new(
                CONDITION_TYPE_RECONCILED.to_string(),
                CONDITION_STATUS_TRUE.to_string(),
                "".to_string(),
                last_transition_time,
                now_str.clone(),
            )
        }
        Err(e) => KafkaTopicListStatusCondition::new(
            CONDITION_TYPE_RECONCILED.to_string(),
            CONDITION_STATUS_FALSE.to_string(),
            e.to_string(),
            last_transition_time,
            now_str.clone(),
        ),
    };
    if topics_status.conditions().is_empty() {
        topics_status.conditions_mut().insert(0, new_status);
    } else {
        topics_status.conditions_mut()[0] = new_status;
    }

    // patch the status field with the updated condition and observed generation
    match kafka_topic_list_api
        .patch_resource_status(kafka_topic_list_name.clone(), topics_status)
        .await
    {
        Ok(_) => {}
        Err(e) => {
            // Not great to silently swallow the error here but doesn't feel warranted to requeue
            // just because the status wasn't updated
            error!("Failed to patch KafkaTopicList status subresource: {}", e);
        }
    }

    reconcile_result.map(|_| ReconcilerAction {
        requeue_after: None,
    })
}

/// an error handler that will be called when the reconciler fails
fn error_policy<T>(error: &CatalogError, _ctx: Context<Data<T>>) -> ReconcilerAction
where
    T: KafkaTopicListApi,
{
    error!(%error, "reconciliation error");
    ReconcilerAction {
        // if a sync fails we want to retry- it could simply be in the process of
        // doing another redeploy. there may be a deeper problem, in which case it'll keep trying
        // and we'll see errors and investigate. arbitrary duration chosen ¯\_(ツ)_/¯
        requeue_after: Some(Duration::from_secs(5)),
    }
}

// Data we want access to in error/reconcile calls
struct Data<T>
where
    T: KafkaTopicListApi,
{
    path_to_iox_binary: String,
    catalog_dsn: String,
    kafka_topic_list_api: T,
}

#[tokio::main]
async fn main() {
    let config = load_config().expect("failed to load config");
    let _drop_handle = setup_tracing(&config.logging_config, None).unwrap();
    debug!(?config, "loaded config");

    info!(git_hash = env!("GIT_HASH"), "starting iox_gitops_adapter");

    let k8s_client = K8sClient::try_default()
        .await
        .expect("couldn't create k8s client");
    let topics = Api::<KafkaTopicList>::namespaced(k8s_client.clone(), config.namespace.as_str());
    info!("initialised Kubernetes API client");

    info!("starting IOx GitOps Adapter");
    Controller::new(topics.clone(), ListParams::default())
        .run(
            reconcile,
            error_policy,
            Context::new(Data {
                path_to_iox_binary: config.iox_cli.clone(),
                catalog_dsn: config.catalog_dsn.clone(),
                kafka_topic_list_api: topics,
            }),
        )
        .for_each(|res| async move {
            match res {
                Ok(o) => info!("reconciled {:?}", o),
                Err(e) => info!("reconcile failed: {:?}", e),
            }
        })
        .await; // controller does nothing unless polled
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use kafka_topic_list::{
        mock_api::{MockKafkaTopicListApi, MockKafkaTopicListApiCall},
        resources::KafkaTopicListSpec,
    };

    use super::*;

    fn create_topics(
        name: &str,
        spec: KafkaTopicListSpec,
        generation: i64,
        status: KafkaTopicListStatus,
    ) -> KafkaTopicList {
        let mut c = KafkaTopicList::new(name, spec);
        c.metadata.generation = Some(generation);
        c.status = Some(status);
        c
    }

    fn create_topics_status(
        observed_generation: i64,
        reconciled: bool,
        message: String,
        t: SystemTime,
    ) -> KafkaTopicListStatus {
        let now: DateTime<Utc> = t.into();
        let now_str = now.to_rfc3339();
        let mut status = KafkaTopicListStatus::default();
        status
            .conditions_mut()
            .push(KafkaTopicListStatusCondition::new(
                CONDITION_TYPE_RECONCILED.to_string(),
                if reconciled {
                    CONDITION_STATUS_TRUE.to_string()
                } else {
                    CONDITION_STATUS_FALSE.to_string()
                },
                message,
                now_str.clone(),
                now_str,
            ));
        status.set_observed_generation(observed_generation);
        status
    }

    #[tokio::test]
    async fn test_single_topic_success() {
        let now = SystemTime::now();
        let mock_topics_api = Arc::new(MockKafkaTopicListApi::default().with_patch_status_ret(
            vec![Ok(create_topics(
                "iox",
                KafkaTopicListSpec::new(vec!["iox_shared".to_string()]),
                1,
                create_topics_status(0, true, "".to_string(), now),
            ))],
        ));
        let data = Data {
            path_to_iox_binary: "test/mock-iox-single-topic.sh".to_string(),
            catalog_dsn: "unused".to_string(),
            kafka_topic_list_api: Arc::clone(&mock_topics_api),
        };
        let c = create_topics(
            "iox",
            KafkaTopicListSpec::new(vec!["iox_shared".to_string()]),
            1,
            create_topics_status(0, true, "".to_string(), now),
        );
        let result = reconcile(Arc::new(c), Context::new(data)).await;
        // whole operation returns a successful result.
        assert_matches!(result, Ok(ReconcilerAction { .. }));
        // ensure status was updated accordingly.
        // alas, we don't have a success patch result either, due to the above
        assert_eq!(
            mock_topics_api.get_calls(),
            vec![MockKafkaTopicListApiCall::PatchStatus {
                kafka_topic_list_name: "iox".to_string(),
                status: create_topics_status(1, true, "".to_string(), now),
            }]
        );
    }

    #[tokio::test]
    async fn test_multi_topic_success() {
        let now = SystemTime::now();
        let mock_topics_api = Arc::new(MockKafkaTopicListApi::default().with_patch_status_ret(
            vec![Ok(create_topics(
                "iox",
                KafkaTopicListSpec::new(vec!["one".to_string(), "two".to_string()]),
                1,
                create_topics_status(0, true, "".to_string(), now),
            ))],
        ));
        let data = Data {
            path_to_iox_binary: "test/mock-iox-single-topic.sh".to_string(),
            catalog_dsn: "unused".to_string(),
            kafka_topic_list_api: Arc::clone(&mock_topics_api),
        };
        let c = create_topics(
            "iox",
            KafkaTopicListSpec::new(vec!["one".to_string(), "two".to_string()]),
            1,
            create_topics_status(0, true, "".to_string(), now),
        );
        let result = reconcile(Arc::new(c), Context::new(data)).await;
        // whole operation returns a successful result.
        assert_matches!(result, Ok(ReconcilerAction { .. }));
        // ensure status was updated accordingly.
        assert_eq!(
            mock_topics_api.get_calls(),
            vec![MockKafkaTopicListApiCall::PatchStatus {
                kafka_topic_list_name: "iox".to_string(),
                status: create_topics_status(1, true, "".to_string(), now),
            }]
        );
    }

    #[tokio::test]
    async fn test_single_topic_error() {
        let now = SystemTime::now();
        let mock_topics_api = Arc::new(MockKafkaTopicListApi::default().with_patch_status_ret(
            vec![Ok(create_topics(
                "iox",
                KafkaTopicListSpec::new(vec!["iox_shared".to_string()]),
                1,
                create_topics_status(0, true, "".to_string(), now),
            ))],
        ));
        let data = Data {
            path_to_iox_binary: "test/mock-iox-failure.sh".to_string(),
            catalog_dsn: "unused".to_string(),
            kafka_topic_list_api: Arc::clone(&mock_topics_api),
        };
        let c = create_topics(
            "iox",
            KafkaTopicListSpec::new(vec!["iox_shared".to_string()]),
            1,
            create_topics_status(0, false, "".to_string(), now),
        );
        let result = reconcile(Arc::new(c), Context::new(data)).await;
        // whole operation returns a successful result
        assert_matches!(result, Err(CatalogError::UpdateTopicError { .. }));
        // Ensure status was updated accordingly
        assert_eq!(
            mock_topics_api.get_calls(),
            vec![MockKafkaTopicListApiCall::PatchStatus {
                kafka_topic_list_name: "iox".to_string(),
                status: create_topics_status(
                    1,
                    false,
                    "Request to update catalog with topic failed: ".to_string(),
                    now
                ),
            }]
        );
    }
}
