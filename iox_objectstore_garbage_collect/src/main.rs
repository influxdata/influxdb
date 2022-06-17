use chrono::{DateTime, Utc};
use clap::Parser;
use core::fmt;
use iox_catalog::{
    interface::Catalog,
    mem::MemCatalog,
    postgres::{PostgresCatalog, PostgresConnectionOptions},
};
use object_store::ObjectStore;
use snafu::prelude::*;
use std::{
    num::{NonZeroUsize, ParseIntError},
    process::ExitCode,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
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

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(flatten)]
    aws: AwsArgs,

    #[clap(flatten)]
    postgres: PostgresArgs,

    #[clap(long, default_value_t = false)]
    dry_run: bool,
}

impl Args {
    async fn object_store(&self) -> object_store::Result<impl ObjectStore> {
        Ok({
            let x = object_store::memory::InMemory::new();
            use object_store::path::Path;
            x.put(&Path::from_raw("/gravy"), b"boat"[..].into())
                .await
                .unwrap();
            x
        })

        // TODO: use the real Object Store
        // self.aws.object_store()
    }

    async fn catalog(&self) -> iox_catalog::interface::Result<impl Catalog> {
        let metrics = metric::Registry::default().into();

        Ok(MemCatalog::new(metrics))

        // TODO: use the real Catalog
        // self.postgres.catalog(metrics).await
    }

    fn cutoff(&self) -> DateTime<Utc> {
        // TODO: parameterize the days
        Utc::now() - chrono::Duration::days(14)
    }
}

// TODO: All these fields were copy-pasted and I don't know if all of
// them are needed. We should pick defaults when possible.
//
// TODO: The default values I picked haven't had deep thought.
#[derive(Debug, Clone, clap::Args)]
struct AwsArgs {
    #[clap(long)]
    access_key_id: Option<String>,

    #[clap(long)]
    secret_access_key: Option<String>,

    #[clap(long)]
    region: String,

    #[clap(long)]
    bucket_name: String,

    #[clap(long)]
    endpoint: Option<String>,

    #[clap(long)]
    session_token: Option<String>,

    #[clap(long, default_value_t = NonZeroUsize::new(2).unwrap())]
    max_connections: NonZeroUsize,

    #[clap(long, default_value_t = false)]
    allow_http: bool,
}

impl AwsArgs {
    fn object_store(&self) -> object_store::Result<impl ObjectStore> {
        let this = self.clone();

        object_store::aws::new_s3(
            this.access_key_id,
            this.secret_access_key,
            this.region,
            this.bucket_name,
            this.endpoint,
            this.session_token,
            this.max_connections,
            this.allow_http,
        )
    }
}

// TODO: All these fields were copy-pasted and I don't know if all of
// them are needed. We should pick defaults when possible.
//
// TODO: The default values I picked haven't had deep thought.
#[derive(Debug, Clone, clap::Args)]
struct PostgresArgs {
    #[clap(long)]
    pub app_name: String,

    #[clap(long)]
    pub schema_name: String,

    #[clap(long)]
    pub dsn: String,

    #[clap(long)]
    pub max_conns: u32,

    #[clap(long, default_value_t = PrettyDuration::from_secs(10))]
    pub connect_timeout: PrettyDuration,

    #[clap(long, default_value_t = PrettyDuration::from_secs(60))]
    pub idle_timeout: PrettyDuration,

    #[clap(long, default_value_t = PrettyDuration::from_secs(60))]
    pub hotswap_poll_interval: PrettyDuration,
}

#[derive(Debug, Clone)]
struct PrettyDuration(Duration);

impl FromStr for PrettyDuration {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s
            .trim_end_matches('s')
            .trim_end_matches("second")
            .trim_end();
        s.parse().map(Duration::from_secs).map(Self)
    }
}

impl PrettyDuration {
    fn from_secs(secs: u64) -> Self {
        Self(Duration::from_secs(secs))
    }
}

impl fmt::Display for PrettyDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} seconds", self.0.as_secs())
    }
}

impl PostgresArgs {
    async fn catalog(
        &self,
        metrics: Arc<metric::Registry>,
    ) -> iox_catalog::interface::Result<impl Catalog> {
        let this = self.clone();

        let options = PostgresConnectionOptions {
            app_name: this.app_name,
            schema_name: this.schema_name,
            dsn: this.dsn,
            max_conns: this.max_conns,
            connect_timeout: this.connect_timeout.0,
            idle_timeout: this.idle_timeout.0,
            hotswap_poll_interval: this.hotswap_poll_interval.0,
        };
        PostgresCatalog::connect(options, metrics).await
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
    use object_store::{ObjectMeta, ObjectStore};
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
        CreatingObjectStore { source: object_store::Error },

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
    use iox_catalog::interface::Catalog;
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
            source: iox_catalog::interface::Error,
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
    use object_store::{ObjectMeta, ObjectStore};
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
        CreatingObjectStore { source: object_store::Error },

        #[snafu(display("{path} could not be deleted"))]
        Deleting {
            source: object_store::Error,
            path: object_store::path::Path,
        },
    }

    pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;
}
