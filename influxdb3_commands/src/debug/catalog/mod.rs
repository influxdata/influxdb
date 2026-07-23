use std::error::Error;
use std::sync::Arc;

use influxdb3_catalog::catalog::CatalogSequenceNumber;
use influxdb3_catalog::format::CatalogFile;
use influxdb3_catalog::format::Header;
use influxdb3_catalog::object_store::CatalogFilePath;
use influxdb3_clap_blocks::object_store::ObjectStoreConfig;
use object_store::{ObjectStore, path::Path as ObjPath};

pub mod list;
pub mod render;
pub mod sequence;
pub mod snapshot;
#[cfg(test)]
mod tests;

/// `influxdb3 debug catalog` — inspect catalog files.
#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: CatalogCommand,
}

#[derive(Debug, clap::Subcommand)]
enum CatalogCommand {
    /// List catalog files in the object store
    #[clap(visible_alias = "ls")]
    List(list::Args),
    /// Inspect the catalog snapshot
    Snapshot(snapshot::Args),
    /// Inspect a single catalog log file by sequence number
    Sequence(sequence::Args),
}

pub async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    match config.cmd {
        CatalogCommand::List(args) => list::run(args).await,
        CatalogCommand::Snapshot(args) => snapshot::run(args).await,
        CatalogCommand::Sequence(args) => sequence::run(args).await,
    }
}

/// Output format shared by all catalog debug verbs.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
#[clap(rename_all = "snake_case")]
pub enum CatalogFormat {
    Pretty,
    Json,
}

/// Arguments shared by every catalog debug verb.
#[derive(Debug, clap::Parser)]
pub struct CommonArgs {
    /// Catalog object-store prefix. Edition-neutral; takes precedence over --cluster-id and --node-id.
    #[clap(long = "catalog-prefix", env = "INFLUXDB3_CATALOG_PREFIX")]
    pub catalog_prefix: Option<String>,

    /// Cluster-id prefix (Enterprise). Lets you reuse a running server's environment.
    #[clap(long = "cluster-id", env = "INFLUXDB3_CLUSTER_ID")]
    pub cluster_id: Option<String>,

    /// Node-id prefix (Core).
    #[clap(long = "node-id", env = "INFLUXDB3_NODE_ID")]
    pub node_id: Option<String>,

    /// Output format.
    #[clap(value_enum, long = "format", default_value = "pretty")]
    pub format: CatalogFormat,

    #[clap(flatten)]
    #[command(next_help_heading = "Object Store Options")]
    pub object_store_config: ObjectStoreConfig,
}

impl CommonArgs {
    pub fn store(&self) -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
        Ok(self
            .object_store_config
            .make_object_store()
            .map_err(|e| format!("object store config error: {e}"))?)
    }

    /// Resolve the catalog object-store prefix: --catalog-prefix, then --cluster-id, then --node-id.
    pub fn prefix(&self) -> Result<&str, Box<dyn Error>> {
        self.catalog_prefix
            .as_deref()
            .or(self.cluster_id.as_deref())
            .or(self.node_id.as_deref())
            .ok_or_else(|| {
                "a catalog prefix is required: pass --catalog-prefix, --cluster-id, or --node-id \
                 (or set INFLUXDB3_CATALOG_PREFIX / INFLUXDB3_CLUSTER_ID / \
                 INFLUXDB3_NODE_ID)"
                    .into()
            })
    }
}

/// Read-only access to catalog files in an object store under a cluster prefix.
#[derive(Debug)]
pub struct CatalogStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

/// One catalog file located in the object store.
#[derive(Debug)]
pub struct CatalogFileEntry {
    pub kind: CatalogFileKind,
    pub sequence: Option<u64>,
    pub path: ObjPath,
    pub size_bytes: u64,
    pub last_modified: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogFileKind {
    Snapshot,
    Log,
}

impl CatalogStore {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into(),
        }
    }

    fn snapshot_path(&self) -> ObjPath {
        CatalogFilePath::snapshot(&self.prefix).into()
    }

    fn log_path(&self, sequence: u64) -> ObjPath {
        CatalogFilePath::log(&self.prefix, CatalogSequenceNumber::new(sequence)).into()
    }

    fn logs_dir(&self) -> ObjPath {
        CatalogFilePath::logs_dir(&self.prefix).into()
    }

    /// Parse the sequence number out of a `logs/NN...N.catalog` path.
    fn sequence_from_path(path: &ObjPath) -> Option<u64> {
        let name = path.filename()?;
        name.strip_suffix(".catalog")?.parse::<u64>().ok()
    }

    /// The snapshot's catalog sequence number (read from its 64-byte header
    /// via a ranged GET), or `None` if no snapshot exists.
    async fn snapshot_sequence(&self) -> Result<Option<u64>, Box<dyn Error>> {
        match self
            .store
            .get_range(&self.snapshot_path(), 0..Header::SIZE as u64)
            .await
        {
            Ok(bytes) => {
                let mut cursor = std::io::Cursor::new(bytes.as_ref());
                let header = Header::read_from(&mut cursor)
                    .map_err(|e| format!("failed to read snapshot header: {e}"))?;
                Ok(Some(header.sequence_number))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// List the snapshot (if present) and log files. By default only logs
    /// after the snapshot are returned; set `include_pre_snapshot` to also
    /// include logs written before it.
    pub async fn list(
        &self,
        include_pre_snapshot: bool,
    ) -> Result<Vec<CatalogFileEntry>, Box<dyn Error>> {
        use futures::StreamExt;

        let mut entries = Vec::new();
        let mut snapshot_seq = None;

        if let Ok(meta) = self.store.head(&self.snapshot_path()).await {
            snapshot_seq = self.snapshot_sequence().await?;
            entries.push(CatalogFileEntry {
                kind: CatalogFileKind::Snapshot,
                sequence: snapshot_seq,
                path: meta.location.clone(),
                size_bytes: meta.size,
                last_modified: meta.last_modified,
            });
        }

        let logs_dir = self.logs_dir();
        let mut stream = self.store.list(Some(&logs_dir));
        let mut logs = Vec::new();
        while let Some(item) = stream.next().await {
            let meta = item?;
            let sequence = Self::sequence_from_path(&meta.location);
            logs.push(CatalogFileEntry {
                kind: CatalogFileKind::Log,
                sequence,
                path: meta.location.clone(),
                size_bytes: meta.size,
                last_modified: meta.last_modified,
            });
        }
        logs.sort_by_key(|e| e.sequence.unwrap_or(u64::MAX));

        if !include_pre_snapshot && let Some(cp) = snapshot_seq {
            // keep only logs strictly after the snapshot (and any whose
            // sequence couldn't be parsed, to avoid hiding unexpected files)
            logs.retain(|e| e.sequence.is_none_or(|s| s > cp));
        }

        entries.extend(logs);
        Ok(entries)
    }

    /// Fetch and parse a file at the given object path.
    pub async fn read_file(
        &self,
        path: &ObjPath,
        skip_crc: bool,
    ) -> Result<CatalogFile, Box<dyn Error>> {
        let bytes = self
            .store
            .get(path)
            .await
            .map_err(|e| format!("failed to read {path}: {e}"))?
            .bytes()
            .await?;
        let file = if skip_crc {
            // Best-effort: attempt a strict read first so we can surface a
            // warning when the payload CRC is actually bad, then fall back to a
            // lenient read that skips the check. Non-CRC errors still fail.
            let mut strict = std::io::Cursor::new(bytes.as_ref());
            match CatalogFile::read_from(&mut strict) {
                Ok(file) => file,
                Err(influxdb3_catalog::format::FormatError::Crc32Mismatch {
                    expected,
                    computed,
                }) => {
                    eprintln!(
                        "warning: payload CRC mismatch in {path} \
                         (expected {expected:#010x}, computed {computed:#010x}); \
                         continuing with --skip-crc"
                    );
                    let mut lenient = std::io::Cursor::new(bytes.as_ref());
                    CatalogFile::read_from_lenient(&mut lenient)
                        .map_err(|e| format!("failed to parse {path}: {e}"))?
                }
                Err(e) => return Err(format!("failed to parse {path}: {e}").into()),
            }
        } else {
            let mut cursor = std::io::Cursor::new(bytes.as_ref());
            CatalogFile::read_from(&mut cursor)
                .map_err(|e| format!("failed to parse {path}: {e}"))?
        };
        Ok(file)
    }

    pub async fn read_snapshot(&self, skip_crc: bool) -> Result<CatalogFile, Box<dyn Error>> {
        self.read_file(&self.snapshot_path(), skip_crc).await
    }

    pub async fn read_log(
        &self,
        sequence: u64,
        skip_crc: bool,
    ) -> Result<CatalogFile, Box<dyn Error>> {
        self.read_file(&self.log_path(sequence), skip_crc).await
    }

    /// The highest log sequence present, if any.
    pub async fn latest_log_sequence(&self) -> Result<Option<u64>, Box<dyn Error>> {
        Ok(self
            .list(true)
            .await?
            .into_iter()
            .filter(|e| e.kind == CatalogFileKind::Log)
            .filter_map(|e| e.sequence)
            .max())
    }
}
