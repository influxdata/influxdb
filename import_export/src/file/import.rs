//! Utilities for importing catalog and data from files
//! MORE COMING SOON: <https://github.com/influxdata/influxdb_iox/issues/7744>

use bytes::Bytes;
use data_types::{
    partition_template::{
        NamespacePartitionTemplateOverride, TablePartitionTemplateOverride, PARTITION_BY_DAY_PROTO,
    },
    ColumnSet, ColumnType, CompactionLevel, Namespace, NamespaceName, NamespaceNameError,
    ParquetFileParams, Partition, Statistics, Table, TableId, Timestamp,
};
use generated_types::influxdata::iox::catalog::v1 as proto;
//    ParquetFile as ProtoParquetFile, Partition as ProtoPartition,
use iox_catalog::interface::{CasFailure, Catalog, RepoCollection, SoftDeletedRows};
use object_store::ObjectStore;
use observability_deps::tracing::{debug, info, warn};
use parquet_file::{
    metadata::{DecodedIoxParquetMetaData, IoxMetadata, IoxParquetMetaData},
    ParquetFilePath,
};
use std::{
    borrow::Cow,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Reading {path:?}: {e}")]
    Reading { path: PathBuf, e: std::io::Error },

    #[error("Not a directory: {0:?}")]
    NotDirectory(PathBuf),

    #[error("Error setting sort key: {0}")]
    SetSortKey(iox_catalog::interface::Error),

    #[error("Error decoding json in {path:?}: {e}")]
    Json { path: PathBuf, e: serde_json::Error },

    #[error("Parquet Metadata Not Found in {path:?}")]
    ParquetMetadataNotFound { path: PathBuf },

    #[error("Invalid Parquet Metadata: {0}")]
    ParquetMetadata(#[from] parquet_file::metadata::Error),

    #[error("Error creating default partition template override: {0}")]
    PartitionOveride(#[from] data_types::partition_template::ValidationError),

    #[error("Expected timestamp stats to be i64, but got: {stats:?}")]
    BadStats { stats: Option<Statistics> },

    #[error("Expected timestamp to have both min and max stats, had min={min:?}, max={max:?}")]
    NoMinMax { min: Option<i64>, max: Option<i64> },

    #[error("Mismatched sort key. Exported sort key is {exported}, existing is {existing}")]
    MismatchedSortKey { exported: String, existing: String },

    #[error("Unexpected parquet filename. Expected a name like <id>.<partition_id>.parquet, got {path:?}")]
    UnexpectedFileName { path: PathBuf },

    #[error("Invalid Namespace: {0}")]
    NamespaceName(#[from] NamespaceNameError),

    #[error(
        "Unexpected error: cound not find sort key in catalog export or embedded parquet metadata"
    )]
    NoSortKey,

    #[error("Unknown compaction level in encoded metadata: {0}")]
    UnknownCompactionLevel(Box<dyn std::error::Error + std::marker::Send + Sync>),

    #[error("Catalog error: {0}")]
    Catalog(#[from] iox_catalog::interface::Error),

    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
}

impl Error {
    fn reading(path: impl Into<PathBuf>, e: std::io::Error) -> Self {
        let path = path.into();
        Self::Reading { path, e }
    }
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Represents the contents of a directory exported using
/// [`RemoteExporter`]. This is a partial catalog snapshot.
///
/// [`RemoteExporter`]: crate::file::RemoteExporter
#[derive(Debug, Default)]
pub struct ExportedContents {
    /// .parquet files
    parquet_files: Vec<PathBuf>,

    /// .parquet.json files (json that correspond to the parquet files)
    parquet_json_files: Vec<PathBuf>,

    /// table .json files
    table_json_files: Vec<PathBuf>,

    /// partition .json files
    partition_json_files: Vec<PathBuf>,

    /// Decoded partition metadata,  found in the export
    partition_metadata: Vec<proto::Partition>,

    /// Decoded parquet metata found in the export
    /// Key is object_store_id, value is decoded metadata
    parquet_metadata: Vec<proto::ParquetFile>,
}

impl ExportedContents {
    /// Read the contents of the directory in `dir_path`, categorizing
    /// files in that directory.
    pub fn try_new(dir_path: &Path) -> Result<Self> {
        info!(?dir_path, "Reading exported catalog contents");

        if !dir_path.is_dir() {
            return Err(Error::NotDirectory(dir_path.into()));
        };

        let entries: Vec<_> = dir_path
            .read_dir()
            .map_err(|e| Error::reading(dir_path, e))?
            .flatten()
            .collect();

        debug!(?entries, "Directory contents");

        let mut new_self = Self::default();

        for entry in entries {
            let path = entry.path();
            let extension = if let Some(extension) = path.extension() {
                extension
            } else {
                warn!(?path, "IGNORING file with no extension");
                continue;
            };

            if extension == "parquet" {
                // names like "<UUID>.parquet"
                new_self.parquet_files.push(path)
            } else if extension == "json" {
                let name = file_name(&path);
                if name.starts_with("table.") {
                    new_self.table_json_files.push(path);
                } else if name.starts_with("partition") {
                    // names like "partitition.<id>.json"
                    new_self.partition_json_files.push(path);
                } else if name.ends_with(".parquet.json") {
                    // names like  "<UUID>.parquet.json"
                    new_self.parquet_json_files.push(path);
                } else {
                    warn!(?path, "IGNORING unknown JSON file");
                }
            } else {
                warn!(?path, "IGNORING unknown file");
            }
        }

        new_self.try_decode_files()?;

        Ok(new_self)
    }

    /// tries to decode all the metadata files found in the export
    fn try_decode_files(&mut self) -> Result<()> {
        debug!("Decoding partition files");

        for path in &self.partition_json_files {
            debug!(?path, "Reading partition json file");
            let json = std::fs::read_to_string(path).map_err(|e| Error::Reading {
                path: path.clone(),
                e,
            })?;

            let partition: proto::Partition =
                serde_json::from_str(&json).map_err(|e| Error::Json {
                    path: path.clone(),
                    e,
                })?;

            self.partition_metadata.push(partition);
        }

        for path in &self.parquet_json_files {
            debug!(?path, "Reading parquet json file");
            let json = std::fs::read_to_string(path).map_err(|e| Error::Reading {
                path: path.clone(),
                e,
            })?;

            let parquet_file: proto::ParquetFile =
                serde_json::from_str(&json).map_err(|e| Error::Json {
                    path: path.clone(),
                    e,
                })?;

            self.parquet_metadata.push(parquet_file);
        }

        Ok(())
    }

    /// Returns the name of the i'th entry in `self.parquet_files`, if
    /// any
    pub fn parquet_file_name(&self, i: usize) -> Option<Cow<'_, str>> {
        self.parquet_files.get(i).map(|p| file_name(p))
    }

    pub fn parquet_files(&self) -> &[PathBuf] {
        self.parquet_files.as_ref()
    }

    pub fn parquet_json_files(&self) -> &[PathBuf] {
        self.parquet_json_files.as_ref()
    }

    pub fn table_json_files(&self) -> &[PathBuf] {
        self.table_json_files.as_ref()
    }

    pub fn partition_json_files(&self) -> &[PathBuf] {
        self.partition_json_files.as_ref()
    }

    /// Returns partition information retrieved from the exported
    /// catalog, if any, with the given table id and partition key
    pub fn partition_metadata(
        &self,
        table_id: i64,
        partition_key: &str,
    ) -> Option<proto::Partition> {
        self.partition_metadata
            .iter()
            .find(|p| p.table_id == table_id && p.key == partition_key)
            .cloned()
    }

    /// Returns parquet file metadata, for the given object_store id, if any
    pub fn parquet_metadata(&self, object_store_id: &str) -> Option<proto::ParquetFile> {
        self.parquet_metadata
            .iter()
            .find(|p| p.object_store_id == object_store_id)
            .cloned()
    }
}

/// Returns the name of the file
fn file_name(p: &Path) -> Cow<'_, str> {
    p.file_name()
        .map(|p| p.to_string_lossy())
        .unwrap_or_else(|| Cow::Borrowed(""))
}

/// Imports the contents of a [`ExportedContents`] into a catalog and
/// object_store instance
#[derive(Debug)]
pub struct RemoteImporter {
    exported_contents: ExportedContents,
    catalog: Arc<dyn Catalog>,
    object_store: Arc<dyn ObjectStore>,
}

impl RemoteImporter {
    pub fn new(
        exported_contents: ExportedContents,
        catalog: Arc<dyn Catalog>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            exported_contents,
            catalog,
            object_store,
        }
    }

    /// Performs the import, reporting status to observer and erroring
    /// if a failure occurs
    pub async fn import(&self) -> Result<()> {
        let parquet_files = self.exported_contents.parquet_files();

        let total_files = parquet_files.len();
        info!(%total_files, "Begin importing files");
        for (files_done, file) in parquet_files.iter().enumerate() {
            self.import_parquet(file).await?;

            // print a log message every 50 files
            if files_done % 50 == 0 {
                let pct = (files_done as f64 / total_files as f64).floor() * 100.0;
                info!(%files_done, %total_files, %pct, "Import running");
            }
        }

        info!(%total_files, "Completed importing files");
        Ok(())
    }

    // tries to import the specified parquet file into the catalog
    async fn import_parquet(&self, file_path: &Path) -> Result<()> {
        info!(?file_path, "Beginning Import");

        // step 1: figure out the location to write the parquet file in object store and do so
        let mut in_file =
            std::fs::File::open(file_path).map_err(|e| Error::reading(file_path, e))?;

        let mut file_bytes = vec![];
        in_file
            .read_to_end(&mut file_bytes)
            .map_err(|e| Error::reading(file_path, e))?;
        let bytes = Bytes::from(file_bytes);
        let file_size_bytes = bytes.len();

        let Some(iox_parquet_metadata) = IoxParquetMetaData::from_file_bytes(bytes.clone())?  else {
            return Err(Error::ParquetMetadataNotFound {
                path: PathBuf::from(file_path)
            });
        };

        let decoded_iox_parquet_metadata = iox_parquet_metadata.decode()?;

        let iox_metadata = decoded_iox_parquet_metadata.read_iox_metadata_new()?;

        debug!(?iox_metadata, "read metadata");

        // step 2: Add the appropriate entry to the catalog
        let namespace_name = iox_metadata.namespace_name.as_ref();
        let mut repos = self.catalog.repositories().await;

        let namespace = repos
            .namespaces()
            .get_by_name(namespace_name, SoftDeletedRows::ExcludeDeleted)
            .await?;

        // create it if it doesn't exist
        let namespace = match namespace {
            Some(namespace) => {
                debug!(%namespace_name, "Found existing namespace");
                namespace
            }
            None => {
                let namespace_name = NamespaceName::try_from(namespace_name)?;
                let partition_template = None;
                let retention_period_ns = None;
                let service_protection_limits = None;

                info!(%namespace_name, "Namespace found, creating new namespace");
                repos
                    .namespaces()
                    .create(
                        &namespace_name,
                        partition_template,
                        retention_period_ns,
                        service_protection_limits,
                    )
                    .await?
            }
        };

        let table = self
            .table_for_parquet_file(repos.as_mut(), &namespace, &iox_metadata)
            .await?;
        let table_id = table.id;
        debug!(%table_id, "Inserting catalog records into table");

        let partition = self
            .partition_for_parquet_file(repos.as_mut(), &table, &iox_metadata)
            .await?;

        // Note that for some reason, the object_store_id that is
        // actually used in object_storage from the source system is
        // different than what is stored in the metadata embedded in
        // the parquet file itself. Thus use the object_store_id
        // encoded into the parquet file name
        let object_store_id =
            object_store_id_from_parquet_filename(file_path).ok_or_else(|| {
                Error::UnexpectedFileName {
                    path: file_path.into(),
                }
            })?;
        debug!(partition_id=%partition.id, %object_store_id, "Inserting into partition");

        let parquet_metadata = self.exported_contents.parquet_metadata(&object_store_id);

        let parquet_params = self
            .parquet_file_params(
                repos.as_mut(),
                &namespace,
                &table,
                &partition,
                parquet_metadata,
                &iox_metadata,
                &decoded_iox_parquet_metadata,
                file_size_bytes,
            )
            .await?;

        let object_store_id = parquet_params.object_store_id;
        let parquet_file = repos.parquet_files().create(parquet_params).await;

        match parquet_file {
            Ok(parquet_file) => {
                debug!(parquet_file_id=?parquet_file.id, "  Created parquet file entry {}", parquet_file.id);
            }
            Err(iox_catalog::interface::Error::FileExists { .. }) => {
                warn!(%object_store_id, "parquet file already exists, skipping");
            }
            Err(e) => {
                return Err(Error::Catalog(e));
            }
        };

        // Now copy the parquet files into the object store
        //let partition_id = TransitionPartitionId::Deprecated(partition.id);
        let transition_partition_id = partition.transition_partition_id();

        let parquet_path = ParquetFilePath::new(
            namespace.id,
            table_id,
            &transition_partition_id,
            object_store_id,
        );
        let object_store_path = parquet_path.object_store_path();
        debug!(?object_store_path, "copying data to object store");
        self.object_store.put(&object_store_path, bytes).await?;

        info!(?file_path, %namespace_name, %object_store_path, %transition_partition_id, %table_id, "Successfully imported file");
        Ok(())
    }

    /// Return the relevant Catlog [`Table`] for the specified parquet
    /// file.
    ///
    /// If the table does not yet exist, it is created, using any
    /// available catalog metadata and falling back to what is in the
    /// iox metadata if needed
    async fn table_for_parquet_file(
        &self,
        repos: &mut dyn RepoCollection,
        namespace: &Namespace,
        iox_metadata: &IoxMetadata,
    ) -> Result<Table> {
        let tables = repos.tables();

        // Note the export format doesn't currently have any table level information
        let table_name = iox_metadata.table_name.as_ref();

        if let Some(table) = tables
            .get_by_namespace_and_name(namespace.id, table_name)
            .await?
        {
            return Ok(table);
        }

        // need to make a new table, create the default partitioning scheme...
        let partition_template = PARTITION_BY_DAY_PROTO.as_ref().clone();
        let namespace_template = NamespacePartitionTemplateOverride::try_from(partition_template)?;
        let custom_table_template = None;
        let partition_template =
            TablePartitionTemplateOverride::try_new(custom_table_template, &namespace_template)?;

        let table = tables
            .create(table_name, partition_template, namespace.id)
            .await?;
        Ok(table)
    }

    /// Return the catalog [`Partition`] into which the specified parquet
    /// file shoudl be inserted.
    ///
    /// First attempts to use any available metadata from the
    /// catalog export, and falls back to what is in the iox
    /// metadata stored in the parquet file, if needed
    async fn partition_for_parquet_file(
        &self,
        repos: &mut dyn RepoCollection,
        table: &Table,
        iox_metadata: &IoxMetadata,
    ) -> Result<Partition> {
        let partition_key = iox_metadata.partition_key.clone();

        let partition = repos
            .partitions()
            .create_or_get(partition_key.clone(), table.id)
            .await?;

        // Note we use the table_id embedded in the file's metadata
        // from the source catalog to match the exported catlog (which
        // is dfferent than the new table we just created in the
        // target catalog);
        let proto_partition = self
            .exported_contents
            .partition_metadata(iox_metadata.table_id.get(), partition_key.inner());

        let new_sort_key: Vec<&str> = if let Some(proto_partition) = proto_partition.as_ref() {
            // Use the sort key from the source catalog
            debug!(array_sort_key=?proto_partition.array_sort_key, "Using sort key from catalog export");
            proto_partition
                .array_sort_key
                .iter()
                .map(|s| s.as_str())
                .collect()
        } else {
            warn!("Could not find sort key in catalog metadata export, falling back to embedded metadata");
            let sort_key = iox_metadata
                .sort_key
                .as_ref()
                .ok_or_else(|| Error::NoSortKey)?;

            sort_key.to_columns().collect()
        };

        if !partition.sort_key.is_empty() && partition.sort_key != new_sort_key {
            let exported = new_sort_key.join(",");
            let existing = partition.sort_key.join(",");
            return Err(Error::MismatchedSortKey { exported, existing });
        }

        loop {
            let res = repos
                .partitions()
                .cas_sort_key(
                    &partition.transition_partition_id(),
                    Some(partition.sort_key.clone()),
                    &new_sort_key,
                )
                .await;

            match res {
                Ok(partition) => return Ok(partition),
                Err(CasFailure::ValueMismatch(_)) => {
                    debug!("Value mismatch when setting sort key, retrying...");
                    continue;
                }
                Err(CasFailure::QueryError(e)) => return Err(Error::SetSortKey(e)),
            }
        }
    }

    /// Return a [`ParquetFileParams`] (information needed to insert
    /// the data into the target catalog).
    ///
    /// First attempts to use any available metadata from the
    /// catalog export, and falls back to what is in the iox
    /// metadata stored in the parquet file, if needed
    #[allow(clippy::too_many_arguments)]
    async fn parquet_file_params(
        &self,
        repos: &mut dyn RepoCollection,
        namespace: &Namespace,
        table: &Table,
        partition: &Partition,
        // parquet metadata, if known
        parquet_metadata: Option<proto::ParquetFile>,
        iox_metadata: &IoxMetadata,
        decoded_iox_parquet_metadata: &DecodedIoxParquetMetaData,
        file_size_bytes: usize,
    ) -> Result<ParquetFileParams> {
        let object_store_id = iox_metadata.object_store_id;

        // need to make columns in the target catalog
        let column_set = insert_columns(table.id, decoded_iox_parquet_metadata, repos).await?;

        let params = if let Some(proto_parquet_file) = &parquet_metadata {
            let compaction_level = proto_parquet_file
                .compaction_level
                .try_into()
                .map_err(Error::UnknownCompactionLevel)?;

            ParquetFileParams {
                namespace_id: namespace.id,
                table_id: table.id,
                partition_id: partition.transition_partition_id(),
                object_store_id,
                min_time: Timestamp::new(proto_parquet_file.min_time),
                max_time: Timestamp::new(proto_parquet_file.max_time),
                file_size_bytes: proto_parquet_file.file_size_bytes,
                row_count: proto_parquet_file.row_count,
                compaction_level,
                created_at: Timestamp::new(proto_parquet_file.created_at),
                column_set,
                max_l0_created_at: Timestamp::new(proto_parquet_file.max_l0_created_at),
            }
        } else {
            warn!("Could not read parquet file metadata, reconstructing based on encoded metadata");

            let (min_time, max_time) = get_min_max_times(decoded_iox_parquet_metadata)?;
            let created_at = Timestamp::new(iox_metadata.creation_timestamp.timestamp_nanos());
            ParquetFileParams {
                namespace_id: namespace.id,
                table_id: table.id,
                partition_id: partition.transition_partition_id(),
                object_store_id,
                min_time,
                max_time,
                // use unwrap: if we can't fit the file size or row
                // counts into usize, something is very wrong and we
                // should stop immediately (and get an exact stack trace)
                file_size_bytes: file_size_bytes.try_into().unwrap(),
                row_count: decoded_iox_parquet_metadata.row_count().try_into().unwrap(),
                //compaction_level: CompactionLevel::Final,
                compaction_level: CompactionLevel::Initial,
                created_at,
                column_set,
                max_l0_created_at: created_at,
            }
        };
        debug!(?params, "Created ParquetFileParams");
        Ok(params)
    }
}
/// Returns a `ColumnSet` that represents all the columns specified in
/// `decoded_iox_parquet_metadata`.
///
/// Insert the appropriate column entries in the catalog they are not
/// already present.
async fn insert_columns(
    table_id: TableId,
    decoded_iox_parquet_metadata: &DecodedIoxParquetMetaData,
    repos: &mut dyn RepoCollection,
) -> Result<ColumnSet> {
    let schema = decoded_iox_parquet_metadata.read_schema()?;

    let mut column_ids = vec![];

    for (iox_column_type, field) in schema.iter() {
        let column_name = field.name();
        let column_type = ColumnType::from(iox_column_type);

        let column = repos
            .columns()
            .create_or_get(column_name, table_id, column_type)
            .await?;
        column_ids.push(column.id);
    }

    Ok(ColumnSet::new(column_ids))
}

/// Reads out the min and max value for the decoded_iox_parquet_metadata column
fn get_min_max_times(
    decoded_iox_parquet_metadata: &DecodedIoxParquetMetaData,
) -> Result<(Timestamp, Timestamp)> {
    let schema = decoded_iox_parquet_metadata.read_schema()?;
    let stats = decoded_iox_parquet_metadata.read_statistics(&schema)?;

    let Some(summary) = stats
        .iter()
        .find(|s| s.name == schema::TIME_COLUMN_NAME) else {
            return Err(Error::BadStats { stats: None });
        };

    let Statistics::I64(stats) = &summary.stats else {
        return Err(Error::BadStats { stats: Some(summary.stats.clone()) });
    };

    let (Some(min), Some(max)) = (stats.min, stats.max) else {
        return Err(Error::NoMinMax  {
            min: stats.min,
            max: stats.max,
        })
    };

    Ok((Timestamp::new(min), Timestamp::new(max)))
}

/// Given a filename of the store parquet metadata, returns the object_store_id
///
/// For example, `e65790df-3e42-0094-048f-0b69a7ee402c.13180488.parquet`,
/// returns `e65790df-3e42-0094-048f-0b69a7ee402c`
///
/// For some reason the object store id embedded in the parquet file's
/// [`IoxMetadata`] and the of the actual file in object storage are
/// different, so we need to use the object_store_id actually used in
/// the source system, which is embedded in the filename
fn object_store_id_from_parquet_filename(path: &Path) -> Option<String> {
    let stem = path
        // <uuid>.partition_id.parquet --> <uuid>.partition_id
        .file_stem()?
        .to_string_lossy();

    // <uuid>.partition_id --> (<uuid>, partition_id)
    let (object_store_id, _partition_id) = stem.split_once('.')?;

    Some(object_store_id.to_string())
}
