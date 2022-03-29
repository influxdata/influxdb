use std::sync::Arc;

use clap_blocks::run_config::RunConfig;
use object_store::ObjectStoreImpl;
use observability_deps::tracing::warn;
use server::config::ConfigProvider;
use server::{ApplicationState, Server, ServerConfig};
use snafu::{ResultExt, Snafu};
use trace::TraceCollector;

use crate::config::ServerConfigFile;
use clap_blocks::object_store::{check_object_store, warn_about_inmem_store};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Cannot parse object store config: {}", source))]
    ObjectStoreParsing {
        source: clap_blocks::object_store::ParseError,
    },

    #[snafu(display("Cannot check object store config: {}", source))]
    ObjectStoreCheck {
        source: clap_blocks::object_store::CheckError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn make_application(
    run_config: &RunConfig,
    config_file: Option<String>,
    num_worker_threads: Option<usize>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
) -> Result<Arc<ApplicationState>> {
    let object_store_config = run_config.object_store_config();
    warn_about_inmem_store(object_store_config);
    let object_store =
        ObjectStoreImpl::try_from(object_store_config).context(ObjectStoreParsingSnafu)?;

    check_object_store(&object_store)
        .await
        .context(ObjectStoreCheckSnafu)?;

    let object_storage = Arc::new(object_store);

    let config_provider =
        config_file.map(|path| Arc::new(ServerConfigFile::new(path)) as Arc<dyn ConfigProvider>);

    Ok(Arc::new(ApplicationState::new(
        object_storage,
        num_worker_threads,
        trace_collector,
        config_provider,
    )))
}

pub fn make_server(
    application: Arc<ApplicationState>,
    wipe_catalog_on_error: bool,
    skip_replay_and_seek_instead: bool,
    run_config: &RunConfig,
) -> Result<Arc<Server>> {
    let server_config = ServerConfig {
        wipe_catalog_on_error,
        skip_replay_and_seek_instead,
    };

    let app_server = Arc::new(Server::new(application, server_config));

    // if this ID isn't set the server won't be usable until this is set via an API
    // call
    if let Some(id) = run_config.server_id_config().server_id {
        app_server.set_id(id).expect("server id already set");
    } else {
        warn!("server ID not set. ID must be set via the INFLUXDB_IOX_ID config or API before writing or querying data.");
    }

    Ok(app_server)
}
