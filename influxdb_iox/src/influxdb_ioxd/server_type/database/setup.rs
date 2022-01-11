use std::sync::Arc;

use object_store::ObjectStore;
use observability_deps::tracing::warn;
use server::{ApplicationState, Server, ServerConfig};
use snafu::{ResultExt, Snafu};
use trace::TraceCollector;

use crate::{
    commands::run::database::Config,
    structopt_blocks::object_store::{check_object_store, warn_about_inmem_store},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Cannot parse object store config: {}", source))]
    ObjectStoreParsing {
        source: crate::structopt_blocks::object_store::ParseError,
    },

    #[snafu(display("Cannot check object store config: {}", source))]
    ObjectStoreCheck {
        source: crate::structopt_blocks::object_store::CheckError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn make_application(
    config: &Config,
    trace_collector: Option<Arc<dyn TraceCollector>>,
) -> Result<Arc<ApplicationState>> {
    warn_about_inmem_store(&config.run_config.object_store_config);
    let object_store = ObjectStore::try_from(&config.run_config.object_store_config)
        .context(ObjectStoreParsingSnafu)?;
    check_object_store(&object_store)
        .await
        .context(ObjectStoreCheckSnafu)?;
    let object_storage = Arc::new(object_store);

    Ok(Arc::new(ApplicationState::new(
        object_storage,
        config.num_worker_threads,
        trace_collector,
    )))
}

pub fn make_server(application: Arc<ApplicationState>, config: &Config) -> Arc<Server> {
    let server_config = ServerConfig {
        wipe_catalog_on_error: config.wipe_catalog_on_error.into(),
        skip_replay_and_seek_instead: config.skip_replay_and_seek_instead.into(),
    };

    let app_server = Arc::new(Server::new(application, server_config));

    // if this ID isn't set the server won't be usable until this is set via an API
    // call
    if let Some(id) = config.run_config.server_id_config.server_id {
        app_server.set_id(id).expect("server id already set");
    } else {
        warn!("server ID not set. ID must be set via the INFLUXDB_IOX_ID config or API before writing or querying data.");
    }

    app_server
}
