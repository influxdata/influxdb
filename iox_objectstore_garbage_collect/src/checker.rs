use chrono::{DateTime, Utc};
use iox_catalog::interface::ParquetFileRepo;
use object_store::ObjectMeta;
use snafu::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug, Snafu)]
pub(crate) enum Error {
    #[snafu(display("Could not create the catalog"))]
    CreatingCatalog {
        source: clap_blocks::catalog_dsn::Error,
    },

    #[snafu(display("Expected a file name"))]
    FileNameMissing,

    #[snafu(display(r#""{uuid}" is not a valid ID"#))]
    MalformedId { source: uuid::Error, uuid: String },

    #[snafu(display("The catalog could not be queried for {object_store_id}"))]
    GetFile {
        source: iox_catalog::interface::Error,
        object_store_id: uuid::Uuid,
    },

    #[snafu(display("The deleter task exited unexpectedly"))]
    DeleterExited {
        source: tokio::sync::mpsc::error::SendError<ObjectMeta>,
    },
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) async fn perform(
    args: Arc<crate::Args>,
    mut items: mpsc::Receiver<ObjectMeta>,
    deleter: mpsc::Sender<ObjectMeta>,
) -> Result<()> {
    let catalog = args.catalog().await.context(CreatingCatalogSnafu)?;
    let cutoff = args.cutoff();

    let mut repositories = catalog.repositories().await;
    let parquet_files = repositories.parquet_files();

    while let Some(item) = items.recv().await {
        if should_delete(&item, cutoff, parquet_files).await? {
            deleter.send(item).await.context(DeleterExitedSnafu)?;
        }
    }

    Ok(())
}

async fn should_delete(
    item: &ObjectMeta,
    cutoff: DateTime<Utc>,
    parquet_files: &mut dyn ParquetFileRepo,
) -> Result<bool> {
    if item.last_modified < cutoff {
        // Not old enough; do not delete
        return Ok(false);
    }

    let file_name = item.location.parts().last().context(FileNameMissingSnafu)?;
    let file_name = file_name.to_string(); // TODO: Hmmmmmm; can we avoid allocation?

    if let Some(uuid) = file_name.strip_suffix(".parquet") {
        let object_store_id = uuid.parse().context(MalformedIdSnafu { uuid })?;
        let parquet_file = parquet_files
            .get_by_object_store_id(object_store_id)
            .await
            .context(GetFileSnafu { object_store_id })?;

        if parquet_file.is_some() {
            // We have a reference to this file; do not delete
            return Ok(false);
        }
    }

    Ok(true)
}
