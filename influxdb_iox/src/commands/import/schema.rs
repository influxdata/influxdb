use std::{
    fmt::{Display, Formatter},
    fs,
    path::PathBuf,
    sync::Arc,
};

use clap::Parser;
use clap_blocks::{
    catalog_dsn::CatalogDsnConfig,
    object_store::{make_object_store, ObjectStoreConfig},
};
use influxdb_iox_client::connection::Connection;
use iox_time::{SystemProvider, TimeProvider};
use object_store::{path::Path, DynObjectStore};
use object_store_metrics::ObjectStoreMetrics;
use thiserror::Error;

use import::{
    aggregate_tsm_schema::{
        fetch::{fetch_schema, FetchError},
        merge::{SchemaMergeError, SchemaMerger},
        update_catalog::{update_iox_catalog, UpdateCatalogError},
        validate::{validate_schema, ValidationError},
    },
    AggregateTSMSchemaOverride,
};

use crate::process_info::setup_metric_registry;

// Possible errors from schema commands
#[derive(Debug, Error)]
pub enum SchemaCommandError {
    #[error("Cannot parse object store config: {0}")]
    ObjectStoreParsing(#[from] clap_blocks::object_store::ParseError),

    #[error("Catalog DSN error: {0}")]
    CatalogDsn(#[from] clap_blocks::catalog_dsn::Error),

    #[error("Error fetching schemas from object storage: {0}")]
    Fetching(#[from] FetchError),

    #[error("Error merging schemas: {0}")]
    Merging(#[from] SchemaMergeError),

    #[error("Schema conflicts during merge:\n{0}")]
    Validating(#[from] ValidationErrors),

    #[error("Merged schema must have one valid Influx type only")]
    InvalidFieldTypes(),

    #[error("Error updating IOx catalog with merged schema: {0}")]
    UpdateCatalogError(#[from] UpdateCatalogError),

    #[error("Error reading schema override file from disk: {0}")]
    SchemaOverrideFileReadError(#[from] std::io::Error),

    #[error("Error parsing schema override file: {0}")]
    SchemaOverrideParseError(#[from] serde_json::Error),
}

#[derive(Debug, Error)]
pub struct ValidationErrors(Vec<ValidationError>);

impl Display for ValidationErrors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.iter().fold(Ok(()), |result, e| {
            result.and_then(|_| writeln!(f, "- {e}"))
        })
    }
}

#[derive(Parser, Debug)]
pub enum Config {
    /// Merge schemas produced in the IOx prestep
    Merge(MergeConfig),
}

/// Merge schema created in pre-step
#[derive(Parser, Debug)]
pub struct MergeConfig {
    #[clap(flatten)]
    object_store: ObjectStoreConfig,

    #[clap(flatten)]
    catalog_dsn: CatalogDsnConfig,

    /// Write buffer topic/database that should be used.
    // This isn't really relevant to the RPC write path and will be removed eventually.
    #[clap(
        long = "write-buffer-topic",
        env = "INFLUXDB_IOX_WRITE_BUFFER_TOPIC",
        default_value = "iox-shared",
        action
    )]
    pub topic: String,

    /// Query pool name to dispatch writes to.
    // This isn't really relevant to the RPC write path and will be removed eventually.
    #[clap(
        long = "query-pool",
        env = "INFLUXDB_IOX_QUERY_POOL_NAME",
        default_value = "iox-shared",
        action
    )]
    pub query_pool_name: String,

    #[clap(long)]
    /// Retention setting setting (used only if we need to create the namespace)
    retention: Option<String>,

    #[clap(long)]
    /// The Org ID of the schemas to merge
    org_id: String,

    #[clap(long)]
    /// The Bucket ID of the schemas to merge
    bucket_id: String,

    #[clap(long)]
    /// The filename prefix to look for in the object store bucket.
    /// Convention is to use `{org_id}/{bucket_id}/{job_name}/`
    prefix: String,

    #[clap(long, default_value = ".schema.json")]
    /// The filename suffix to look for in the object store bucket
    suffix: String,

    #[clap(long)]
    /// Filename of schema override file used to instruct this tool on how to resolve schema
    /// conflicts in the TSM schemas before updating the schema in the IOx catalog.
    schema_override_file: Option<PathBuf>,
}

/// Entry-point for the schema command
pub async fn command(connection: Connection, config: Config) -> Result<(), SchemaCommandError> {
    match config {
        Config::Merge(merge_config) => {
            let time_provider = Arc::new(SystemProvider::new()) as Arc<dyn TimeProvider>;
            let metrics = setup_metric_registry();

            let object_store = make_object_store(&merge_config.object_store)
                .map_err(SchemaCommandError::ObjectStoreParsing)?;
            // Decorate the object store with a metric recorder.
            let object_store: Arc<DynObjectStore> = Arc::new(ObjectStoreMetrics::new(
                object_store,
                time_provider,
                &metrics,
            ));

            let catalog = merge_config
                .catalog_dsn
                .get_catalog("import", Arc::clone(&metrics))
                .await?;

            // fetch the TSM schemas and merge into one aggregate schema
            let schemas = fetch_schema(
                Arc::clone(&object_store),
                Some(&Path::from(merge_config.prefix)),
                &merge_config.suffix,
            )
            .await?;
            let mut merger = SchemaMerger::new(
                merge_config.org_id.clone(),
                merge_config.bucket_id.clone(),
                schemas,
            );

            // load a schema override file, if provided, to resolve field type conflicts
            if let Some(schema_override_file) = merge_config.schema_override_file {
                let data = fs::read(schema_override_file)
                    .map_err(SchemaCommandError::SchemaOverrideFileReadError)?;
                let schema_override: AggregateTSMSchemaOverride = data
                    .try_into()
                    .map_err(SchemaCommandError::SchemaOverrideParseError)?;
                merger = merger.with_schema_override(schema_override);
            }

            // note that this will also apply the schema override, if the user provided one
            let merged_tsm_schema = merger.merge().map_err(SchemaCommandError::Merging)?;
            // just print the merged schema for now; we'll do more with this in future PRs
            println!("Merged schema:\n{merged_tsm_schema:?}");

            // don't proceed unless we produce a valid merged schema
            if let Err(errors) = validate_schema(&merged_tsm_schema) {
                return Err(SchemaCommandError::Validating(ValidationErrors(errors)));
            }

            // From here we can happily .unwrap() the field types knowing they're valid
            if !merged_tsm_schema.types_are_valid() {
                return Err(SchemaCommandError::InvalidFieldTypes());
            }

            // given we have a valid aggregate TSM schema, fetch the schema for the namespace from
            // the IOx catalog, if it exists, and update it with our aggregate schema
            update_iox_catalog(
                &merged_tsm_schema,
                &merge_config.topic,
                &merge_config.query_pool_name,
                Arc::clone(&catalog),
                connection.clone(),
            )
            .await?;

            Ok(())
        }
    }
}
