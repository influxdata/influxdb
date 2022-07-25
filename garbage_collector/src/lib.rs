//! Tool to clean up old object store files that don't appear in the catalog.

#![deny(
    rustdoc::broken_intra_doc_links,
    rust_2018_idioms,
    missing_debug_implementations,
    unreachable_pub
)]
#![warn(
    missing_docs,
    clippy::todo,
    clippy::dbg_macro,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send
)]
#![allow(clippy::missing_docs_in_private_items)]

use chrono::{DateTime, Utc};
use chrono_english::{parse_date_string, Dialect};
use clap::Parser;
use iox_catalog::interface::Catalog;
use object_store::DynObjectStore;
use observability_deps::tracing::*;
use snafu::prelude::*;
use std::{fmt::Debug, sync::Arc};
use tokio::sync::{broadcast, mpsc};

/// Logic for checking if a file in object storage should be deleted or not.
mod checker;
/// Logic for deleting a file from object storage.
mod deleter;
/// Logic for listing all files in object storage.
mod lister;

const BATCH_SIZE: usize = 1000;

/// Run the tasks that clean up old object store files that don't appear in the catalog.
pub async fn main(config: Config) -> Result<()> {
    GarbageCollector::start(config)?.join().await
}

/// The tasks that clean up old object store files that don't appear in the catalog.
pub struct GarbageCollector {
    shutdown_tx: broadcast::Sender<()>,
    lister: tokio::task::JoinHandle<Result<(), lister::Error>>,
    checker: tokio::task::JoinHandle<Result<(), checker::Error>>,
    deleter: tokio::task::JoinHandle<Result<(), deleter::Error>>,
}

impl Debug for GarbageCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GarbageCollector").finish_non_exhaustive()
    }
}

impl GarbageCollector {
    /// Construct the garbage collector and start it
    pub fn start(config: Config) -> Result<Self> {
        let Config {
            object_store,
            sub_config,
            catalog,
        } = config;

        let dry_run = sub_config.dry_run;
        let cutoff = sub_config.cutoff()?;
        info!(
            cutoff_arg = %sub_config.cutoff,
            cutoff_parsed = %cutoff,
            "GarbageCollector starting"
        );

        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        let (tx1, rx1) = mpsc::channel(BATCH_SIZE);
        let (tx2, rx2) = mpsc::channel(BATCH_SIZE);

        let lister = tokio::spawn(lister::perform(shutdown_rx, Arc::clone(&object_store), tx1));
        let checker = tokio::spawn(checker::perform(catalog, cutoff, rx1, tx2));
        let deleter = tokio::spawn(deleter::perform(object_store, dry_run, rx2));

        Ok(Self {
            shutdown_tx,
            lister,
            checker,
            deleter,
        })
    }

    /// A handle to gracefully shutdown the garbage collector when invoked
    pub fn shutdown_handle(&self) -> impl Fn() {
        let shutdown_tx = self.shutdown_tx.clone();
        move || {
            shutdown_tx.send(()).ok();
        }
    }

    /// Wait for the garbage collector to finish work
    pub async fn join(self) -> Result<()> {
        let Self {
            lister,
            checker,
            deleter,
            ..
        } = self;

        let (lister, checker, deleter) = futures::join!(lister, checker, deleter);

        deleter.context(DeleterPanicSnafu)??;
        checker.context(CheckerPanicSnafu)??;
        lister.context(ListerPanicSnafu)??;

        Ok(())
    }
}

/// Configuration to run the object store garbage collector
#[derive(Clone)]
pub struct Config {
    /// The object store to garbage collect
    pub object_store: Arc<DynObjectStore>,

    /// The catalog to check if an object is garbage
    pub catalog: Arc<dyn Catalog>,

    /// The garbage collector specific configuration
    pub sub_config: SubConfig,
}

impl Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config (GarbageCollector")
            .field("sub_config", &self.sub_config)
            .finish_non_exhaustive()
    }
}

/// Configuration specific to the object store garbage collector
#[derive(Debug, Clone, Parser)]
pub struct SubConfig {
    /// If this flag is specified, don't delete the files in object storage. Only print the files
    /// that would be deleted if this flag wasn't specified.
    #[clap(long)]
    dry_run: bool,

    /// Items in the object store that are older than this timestamp and also unreferenced in the
    /// catalog will be deleted.
    ///
    /// Can be an exact datetime like `2020-01-01T01:23:45-05:00` or a fuzzy
    /// specification like `1 hour ago`. If not specified, defaults to 14 days ago.
    #[clap(long, default_value_t = String::from("14 days ago"))]
    cutoff: String,
}

impl SubConfig {
    fn cutoff(&self) -> Result<DateTime<Utc>> {
        let argument = &self.cutoff;
        parse_date_string(argument, Utc::now(), Dialect::Us)
            .context(ParsingCutoffSnafu { argument })
    }
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display(r#"Could not parse the cutoff "{argument}""#))]
    ParsingCutoff {
        source: chrono_english::DateError,
        argument: String,
    },

    #[snafu(display("The lister task failed"))]
    #[snafu(context(false))]
    Lister { source: lister::Error },
    #[snafu(display("The lister task panicked"))]
    ListerPanic { source: tokio::task::JoinError },

    #[snafu(display("The checker task failed"))]
    #[snafu(context(false))]
    Checker { source: checker::Error },
    #[snafu(display("The checker task panicked"))]
    CheckerPanic { source: tokio::task::JoinError },

    #[snafu(display("The deleter task failed"))]
    #[snafu(context(false))]
    Deleter { source: deleter::Error },
    #[snafu(display("The deleter task panicked"))]
    DeleterPanic { source: tokio::task::JoinError },
}

#[allow(missing_docs)]
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use clap_blocks::{
        catalog_dsn::CatalogDsnConfig,
        object_store::{make_object_store, ObjectStoreConfig},
    };
    use filetime::FileTime;
    use std::{fs, iter, path::PathBuf};
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn deletes_untracked_files_older_than_the_cutoff() {
        let setup = OldFileSetup::new();

        let config = build_config(setup.data_dir_arg(), []).await;
        main(config).await.unwrap();

        assert!(
            !setup.file_path.exists(),
            "The path {} should have been deleted",
            setup.file_path.as_path().display(),
        );
    }

    #[tokio::test]
    async fn preserves_untracked_files_newer_than_the_cutoff() {
        let setup = OldFileSetup::new();

        #[rustfmt::skip]
        let config = build_config(setup.data_dir_arg(), [
            "--cutoff", "10 years ago",
        ]).await;
        main(config).await.unwrap();

        assert!(
            setup.file_path.exists(),
            "The path {} should not have been deleted",
            setup.file_path.as_path().display(),
        );
    }

    async fn build_config(data_dir: &str, args: impl IntoIterator<Item = &str> + Send) -> Config {
        let sub_config = SubConfig::parse_from(iter::once("dummy-program-name").chain(args));
        let object_store = object_store(data_dir);
        let catalog = catalog().await;

        Config {
            object_store,
            catalog,
            sub_config,
        }
    }

    fn object_store(data_dir: &str) -> Arc<DynObjectStore> {
        #[rustfmt::skip]
        let cfg = ObjectStoreConfig::parse_from([
            "dummy-program-name",
            "--object-store", "file",
            "--data-dir", data_dir,
        ]);
        make_object_store(&cfg).unwrap()
    }

    async fn catalog() -> Arc<dyn Catalog> {
        #[rustfmt::skip]
        let cfg = CatalogDsnConfig::parse_from([
            "dummy-program-name",
            "--catalog", "memory",
        ]);

        let metrics = metric::Registry::default().into();

        cfg.get_catalog("garbage_collector", metrics).await.unwrap()
    }

    struct OldFileSetup {
        data_dir: TempDir,
        file_path: PathBuf,
    }

    impl OldFileSetup {
        const APRIL_9_2018: FileTime = FileTime::from_unix_time(1523308536, 0);

        fn new() -> Self {
            let data_dir = TempDir::new().unwrap();

            let file_path = data_dir.path().join("some-old-file");
            fs::write(&file_path, "dummy content").unwrap();
            filetime::set_file_mtime(&file_path, Self::APRIL_9_2018).unwrap();

            Self {
                data_dir,
                file_path,
            }
        }

        fn data_dir_arg(&self) -> &str {
            self.data_dir.path().to_str().unwrap()
        }
    }
}
