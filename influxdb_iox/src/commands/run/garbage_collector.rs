use clap_blocks::{
    catalog_dsn::CatalogDsnConfig, object_store::make_object_store, run_config::RunConfig,
};
use iox_time::SystemProvider;
use ioxd_common::{
    server_type::{CommonServerState, CommonServerStateError},
    Service,
};
use ioxd_garbage_collector as gc;
use object_store::DynObjectStore;
use object_store_metrics::ObjectStoreMetrics;
use observability_deps::tracing::*;
use snafu::prelude::*;
use std::sync::Arc;

use crate::process_info::setup_metric_registry;

use super::main;

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(flatten)]
    pub run_config: RunConfig,

    #[clap(flatten)]
    catalog_dsn: CatalogDsnConfig,

    #[clap(flatten)]
    pub sub_config: gc::SubConfig,
}

pub async fn command(config: Config) -> Result<()> {
    let time_provider = Arc::new(SystemProvider::new());
    let metric_registry = setup_metric_registry();

    let catalog = config
        .catalog_dsn
        .get_catalog("garbage-collector", Arc::clone(&metric_registry))
        .await?;

    let object_store = make_object_store(config.run_config.object_store_config())?;

    // Decorate the object store with a metric recorder.
    let object_store: Arc<DynObjectStore> = Arc::new(ObjectStoreMetrics::new(
        object_store,
        time_provider,
        &metric_registry,
    ));

    let sub_config = config.sub_config;

    info!("starting garbage-collector");

    let server_type = Arc::new({
        let config = gc::Config {
            object_store,
            catalog,
            sub_config,
        };
        let metric_registry = Arc::clone(&metric_registry);

        gc::Server::start(metric_registry, config)
    });

    let common_state = CommonServerState::from_config(config.run_config)?;

    let services = vec![Service::create(server_type, common_state.run_config())];

    Ok(main::main(common_state, services, metric_registry).await?)
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Could not parse the catalog configuration"))]
    #[snafu(context(false))]
    CatalogConfigParsing {
        source: clap_blocks::catalog_dsn::Error,
    },

    #[snafu(display("Could not parse the object store configuration"))]
    #[snafu(context(false))]
    ObjectStoreConfigParsing {
        source: clap_blocks::object_store::ParseError,
    },

    #[snafu(display("Could not create the common server state"))]
    #[snafu(context(false))]
    CommonServerStateCreation { source: CommonServerStateError },

    #[snafu(display("Could not start the garbage collector"))]
    #[snafu(context(false))]
    ServiceExecution { source: super::main::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
