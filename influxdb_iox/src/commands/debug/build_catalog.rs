//! This module implements the `build_catalog` CLI command
use import_export::file::{ExportedContents, RemoteImporter};
use iox_catalog::interface::Catalog;
use metric::Registry;
use object_store::ObjectStore;
use observability_deps::tracing::info;
use snafu::{ResultExt, Snafu};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Not yet implemented"))]
    NotYetImplemented,

    #[snafu(display("Catalog error:: {}", source))]
    #[snafu(context(false))]
    Catalog {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Object store error:: {}", source))]
    #[snafu(context(false))]
    ObjectStore { source: object_store::Error },

    #[snafu(display("Import error:: {}", source))]
    #[snafu(context(false))]
    Import { source: import_export::file::Error },

    #[snafu(display("Cannot {} output file '{:?}': {}", operation, path, source))]
    File {
        operation: String,
        path: PathBuf,
        source: std::io::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, clap::Parser)]
pub struct Config {
    /// Directory containing the output of running `influxdb_iox remote store get-table`
    #[clap(value_parser)]
    input_dir: PathBuf,

    /// Target data directory to create a sqlite catalog and file
    /// object_store.
    ///
    /// After a successful rebuild, you can examine the catalog locally using
    /// `influxdb_iox --data-dir <dir>`.
    #[clap(value_parser)]
    pub data_dir: PathBuf,
}

pub async fn command(config: Config) -> Result<(), Error> {
    let Config {
        input_dir,
        data_dir,
    } = config;

    let exported_contents = ExportedContents::try_new(&input_dir)?;

    // create a catalog / object store
    let catalog = get_catalog(&data_dir).await?;
    catalog.setup().await?;

    let object_store = get_object_store(&data_dir)?;

    info!("Initialized catalog, object store, and input path ...");

    let importer = RemoteImporter::new(exported_contents, catalog, object_store);

    info!(
        ?input_dir,
        ?data_dir,
        "Beginning catalog / object_store build"
    );

    Ok(importer.import().await?)
}

async fn get_catalog(data_dir: &Path) -> Result<Arc<dyn Catalog>> {
    std::fs::create_dir_all(data_dir).context(FileSnafu {
        operation: "create data directory",
        path: data_dir,
    })?;

    let file_path = data_dir.join("catalog.sqlite");
    let metrics = Arc::new(Registry::default());
    let options = iox_catalog::sqlite::SqliteConnectionOptions {
        file_path: file_path.display().to_string(),
    };

    info!(?file_path, "Using sqlite local catalog");
    let catalog = iox_catalog::sqlite::SqliteCatalog::connect(options, metrics).await?;
    Ok(Arc::new(catalog))
}

fn get_object_store(data_dir: &Path) -> Result<Arc<dyn ObjectStore>> {
    let os_dir = data_dir.join("object_store");
    std::fs::create_dir_all(&os_dir).context(FileSnafu {
        operation: "create object_store directory",
        path: &os_dir,
    })?;

    info!(?os_dir, "Using local object store");
    let object_store = object_store::local::LocalFileSystem::new_with_prefix(os_dir)?;

    Ok(Arc::new(object_store))
}
