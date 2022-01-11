use crate::structopt_blocks::{object_store::ObjectStoreConfig, server_id::ServerIdConfig};
use iox_object_store::IoxObjectStore;
use object_store::ObjectStore;
use snafu::{OptionExt, ResultExt, Snafu};
use std::{convert::TryFrom, sync::Arc};
use structopt::StructOpt;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Cannot parse object store config: {}", source))]
    ObjectStoreParsing {
        source: crate::structopt_blocks::object_store::ParseError,
    },

    #[snafu(display("No server ID provided"))]
    NoServerId,

    #[snafu(display("Can't read server config from object storage: {}", source))]
    CantReadServerConfig { source: object_store::Error },

    #[snafu(display("Error deserializing server config from protobuf: {}", source))]
    CantDeserializeServerConfig {
        source: generated_types::DecodeError,
    },

    #[snafu(display("Can't find a database with this name on this server"))]
    CantFindDatabase,

    #[snafu(display("Cannot open IOx object store: {}", source))]
    IoxObjectStoreFailure {
        source: iox_object_store::IoxObjectStoreError,
    },

    #[snafu(display("Cannot dump catalog: {}", source))]
    DumpCatalogFailure {
        source: parquet_catalog::dump::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Dump preserved catalog.
#[derive(Debug, StructOpt)]
pub struct Config {
    // object store config
    #[structopt(flatten)]
    object_store_config: ObjectStoreConfig,

    // server ID config
    #[structopt(flatten)]
    server_id_config: ServerIdConfig,

    /// The name of the database
    db_name: String,

    // dump options
    #[structopt(flatten)]
    dump_options: DumpOptions,
}

#[derive(Debug, StructOpt)]
pub struct DumpOptions {
    /// Show debug output of `DecodedIoxParquetMetaData` if decoding succeeds, show the decoding
    /// error otherwise.
    ///
    /// Since this contains the entire Apache Parquet metadata object this is quite verbose and is
    /// usually not recommended.
    #[structopt(long = "--show-parquet-metadata")]
    show_parquet_metadata: bool,

    /// Show debug output of `IoxMetadata` if decoding succeeds, show the decoding
    /// error otherwise.
    #[structopt(long = "--show-iox-metadata")]
    show_iox_metadata: bool,

    /// Show debug output of `Schema` if decoding succeeds, show the decoding
    /// error otherwise.
    #[structopt(long = "--show-schema")]
    show_schema: bool,

    /// Show debug output of `ColumnSummary` if decoding succeeds,
    /// show the decoding error otherwise.
    #[structopt(long = "--show-statistics")]
    show_statistics: bool,

    /// Show unparsed `IoxParquetMetaData` -- which are Apache Thrift bytes -- as part of the
    /// transaction actions.
    ///
    /// Since this binary data is usually quite hard to read, it is recommended to set this to
    /// `false` which will replace the actual bytes with `b"metadata omitted"`. Use the other
    /// toggles to instead show the content of the Apache Thrift message.
    #[structopt(long = "--show-unparsed-metadata")]
    show_unparsed_metadata: bool,
}

impl From<DumpOptions> for parquet_catalog::dump::DumpOptions {
    fn from(options: DumpOptions) -> Self {
        Self {
            show_parquet_metadata: options.show_parquet_metadata,
            show_iox_metadata: options.show_iox_metadata,
            show_schema: options.show_schema,
            show_statistics: options.show_statistics,
            show_unparsed_metadata: options.show_unparsed_metadata,
        }
    }
}

pub async fn command(config: Config) -> Result<()> {
    let object_store = Arc::new(
        ObjectStore::try_from(&config.object_store_config).context(ObjectStoreParsingSnafu)?,
    );
    let server_id = config.server_id_config.server_id.context(NoServerIdSnafu)?;
    let server_config_bytes = IoxObjectStore::get_server_config_file(&object_store, server_id)
        .await
        .context(CantReadServerConfigSnafu)?;

    let server_config =
        generated_types::server_config::decode_persisted_server_config(server_config_bytes)
            .context(CantDeserializeServerConfigSnafu)?;

    let database_location = server_config
        .databases
        .get(&config.db_name)
        .context(CantFindDatabaseSnafu)?;

    let iox_object_store =
        IoxObjectStore::load_at_root_path(Arc::clone(&object_store), database_location)
            .await
            .context(IoxObjectStoreFailureSnafu)?;

    let mut writer = std::io::stdout();
    let options = config.dump_options.into();
    parquet_catalog::dump::dump(&iox_object_store, &mut writer, options)
        .await
        .context(DumpCatalogFailureSnafu)?;

    Ok(())
}
