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
use clap::Parser;
use clap_blocks::{
    catalog_dsn::CatalogDsnConfig,
    object_store::{make_object_store, ObjectStoreConfig},
};
use iox_catalog::interface::Catalog;
use object_store::DynObjectStore;
use snafu::prelude::*;
use std::{process::ExitCode, sync::Arc};
use tokio::sync::mpsc;

const BATCH_SIZE: usize = 1000;

fn main() -> ExitCode {
    if let Err(e) = inner_main() {
        use snafu::ErrorCompat;

        eprintln!("{e}");

        for cause in ErrorCompat::iter_chain(&e).skip(1) {
            eprintln!("Caused by: {cause}");
        }

        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

#[tokio::main(flavor = "current_thread")]
async fn inner_main() -> Result<()> {
    let args = Arc::new(Args::parse());

    let (tx1, rx1) = mpsc::channel(BATCH_SIZE);
    let (tx2, rx2) = mpsc::channel(BATCH_SIZE);

    let lister = tokio::spawn(lister::perform(args.clone(), tx1));
    let checker = tokio::spawn(checker::perform(args.clone(), rx1, tx2));
    let deleter = tokio::spawn(deleter::perform(args.clone(), rx2));

    let (lister, checker, deleter) = futures::join!(lister, checker, deleter);

    deleter.context(DeleterPanicSnafu)??;
    checker.context(CheckerPanicSnafu)??;
    lister.context(ListerPanicSnafu)??;

    Ok(())
}

/// Command-line arguments
#[derive(Debug, Parser)]
pub struct Args {
    #[clap(flatten)]
    object_store: ObjectStoreConfig,

    #[clap(flatten)]
    catalog_dsn: CatalogDsnConfig,

    #[clap(long, default_value_t = false)]
    dry_run: bool,
}

impl Args {
    async fn object_store(
        &self,
    ) -> Result<Arc<DynObjectStore>, clap_blocks::object_store::ParseError> {
        make_object_store(&self.object_store)
    }

    async fn catalog(&self) -> Result<Arc<dyn Catalog>, clap_blocks::catalog_dsn::Error> {
        let metrics = metric::Registry::default().into();

        self.catalog_dsn
            .get_catalog("iox_objectstore_garbage_collect", metrics)
            .await
    }

    fn cutoff(&self) -> DateTime<Utc> {
        // TODO: parameterize the days
        Utc::now() - chrono::Duration::days(14)
    }
}

#[derive(Debug, Snafu)]
enum Error {
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

type Result<T, E = Error> = std::result::Result<T, E>;

mod lister {
    use futures::prelude::*;
    use object_store::ObjectMeta;
    use snafu::prelude::*;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    pub(crate) async fn perform(
        args: Arc<crate::Args>,
        checker: mpsc::Sender<ObjectMeta>,
    ) -> Result<()> {
        let object_store = args
            .object_store()
            .await
            .context(CreatingObjectStoreSnafu)?;

        let mut items = object_store.list(None).await.context(ListingSnafu)?;

        while let Some(item) = items.next().await {
            let item = item.context(MalformedSnafu)?;
            checker.send(item).await?;
        }

        Ok(())
    }

    #[derive(Debug, Snafu)]
    pub(crate) enum Error {
        #[snafu(display("Could not create the object store"))]
        CreatingObjectStore {
            source: clap_blocks::object_store::ParseError,
        },

        #[snafu(display("The prefix could not be listed"))]
        Listing { source: object_store::Error },

        #[snafu(display("The object could not be listed"))]
        Malformed { source: object_store::Error },

        #[snafu(display("The checker task exited unexpectedly"))]
        #[snafu(context(false))]
        CheckerExited {
            source: tokio::sync::mpsc::error::SendError<ObjectMeta>,
        },
    }

    pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;
}

mod checker {
    use object_store::ObjectMeta;
    use snafu::prelude::*;
    use std::sync::Arc;
    use tokio::sync::mpsc;

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
            if item.last_modified < cutoff {
                // Not old enough; do not delete
                continue;
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
                    continue;
                }
            }

            deleter.send(item).await.context(DeleterExitedSnafu)?;
        }

        Ok(())
    }

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
}

mod deleter {
    use object_store::ObjectMeta;
    use snafu::prelude::*;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    pub(crate) async fn perform(
        args: Arc<crate::Args>,
        mut items: mpsc::Receiver<ObjectMeta>,
    ) -> Result<()> {
        let object_store = args
            .object_store()
            .await
            .context(CreatingObjectStoreSnafu)?;
        let dry_run = args.dry_run;

        while let Some(item) = items.recv().await {
            let path = item.location;
            if dry_run {
                eprintln!("Not deleting {path} due to dry run");
            } else {
                object_store
                    .delete(&path)
                    .await
                    .context(DeletingSnafu { path })?;
            }
        }

        Ok(())
    }

    #[derive(Debug, Snafu)]
    pub(crate) enum Error {
        #[snafu(display("Could not create the object store"))]
        CreatingObjectStore {
            source: clap_blocks::object_store::ParseError,
        },

        #[snafu(display("{path} could not be deleted"))]
        Deleting {
            source: object_store::Error,
            path: object_store::path::Path,
        },
    }

    pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;
}
