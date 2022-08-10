use std::sync::Arc;

use clap::Parser;
use clap_blocks::object_store::{make_object_store, ObjectStoreConfig};
use object_store::path::Path;
use thiserror::Error;

use import::schema::{
    fetch::{fetch_schema, FetchError},
    merge::{SchemaMergeError, SchemaMerger},
    validate::{validate_schema, ValidationError},
};

// Possible errors from schema commands
#[derive(Debug, Error)]
pub enum SchemaCommandError {
    #[error("Cannot parse object store config: {0}")]
    ObjectStoreParsing(#[from] clap_blocks::object_store::ParseError),

    #[error("Error fetching schemas from object storage: {0}")]
    Fetching(#[from] FetchError),

    #[error("Error merging schemas: {0}")]
    Merging(#[from] SchemaMergeError),

    #[error("{0}")]
    Validating(#[from] ValidationError),
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
}

/// Entry-point for the schema command
pub async fn command(config: Config) -> Result<(), SchemaCommandError> {
    match config {
        Config::Merge(merge_config) => {
            let object_store = make_object_store(&merge_config.object_store)
                .map_err(SchemaCommandError::ObjectStoreParsing)?;

            let schemas = fetch_schema(
                Arc::clone(&object_store),
                Some(&Path::from(merge_config.prefix)),
                &merge_config.suffix,
            )
            .await?;
            let merger = SchemaMerger::new(merge_config.org_id, merge_config.bucket_id, schemas);
            let merged = merger.merge().map_err(SchemaCommandError::Merging)?;
            // just print the merged schema for now; we'll do more with this in future PRs
            println!("Merged schema:\n{:?}", merged);

            if let Err(errors) = validate_schema(&merged) {
                eprintln!("Schema conflicts:");
                for e in errors {
                    eprintln!("- {}", e);
                }
            }
            Ok(())
        }
    }
}
