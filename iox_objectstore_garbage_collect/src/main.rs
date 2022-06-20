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

mod checker;
mod deleter;
mod lister;

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
