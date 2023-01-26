//! This module implements the `remote partition` CLI subcommand

use bytes::Bytes;
use clap_blocks::object_store::{make_object_store, ObjectStoreType};
use clap_blocks::{catalog_dsn::CatalogDsnConfig, object_store::ObjectStoreConfig};
use data_types::{
    ColumnId, ColumnSet, ColumnType, NamespaceId, NamespaceSchema as CatalogNamespaceSchema,
    ParquetFile as CatalogParquetFile, ParquetFileParams, PartitionId, SequenceNumber, ShardId,
    ShardIndex, TableId, Timestamp,
};
use futures::future::join_all;
use influxdb_iox_client::{
    catalog::{
        self,
        generated_types::{ParquetFile, Partition},
    },
    connection::Connection,
    schema::{self, generated_types::NamespaceSchema},
    store,
};
use iox_catalog::interface::{get_schema_by_name, Catalog};
use parquet_file::ParquetFilePath;
use std::sync::Arc;
use thiserror::Error;
use tokio_stream::StreamExt;
use uuid::Uuid;

use crate::process_info::setup_metric_registry;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("JSON Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Client error: {0}")]
    ClientError(#[from] influxdb_iox_client::error::Error),

    #[error("Catalog DSN error: {0}")]
    CatalogDsn(#[from] clap_blocks::catalog_dsn::Error),

    #[error("Cannot parse object store config: {0}")]
    ObjectStoreParsing(#[from] clap_blocks::object_store::ParseError),

    #[error("Catalog error: {0}")]
    Catalog(#[from] iox_catalog::interface::Error),

    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("Namespace not found")]
    NamespaceNotFound,

    #[error("Table not found")]
    TableNotFound,

    #[error("Partition not found")]
    PartitionNotFound,

    #[error(
        "The object store is configured to store files in memory which is \
        unlikely to be useful - try passing --object-store=file"
    )]
    SillyObjectStoreConfig,
}

/// Manage IOx chunks
#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

/// Show the parqet_files of a partition
#[derive(Debug, clap::Parser)]
struct Show {
    /// The id of the partition
    #[clap(action)]
    id: i64,
}

/// Pull the schema and partition parquet files into the local catalog and object store
#[derive(Debug, clap::Parser)]
struct Pull {
    #[clap(flatten)]
    catalog_dsn: CatalogDsnConfig,

    #[clap(flatten)]
    object_store: ObjectStoreConfig,

    /// The namespace we're getting the partition from
    #[clap(action)]
    namespace: String,

    /// The table name
    #[clap(action)]
    table: String,

    /// The partition key
    #[clap(action)]
    partition_key: String,
}

/// All possible subcommands for partition
#[derive(Debug, clap::Parser)]
enum Command {
    Show(Show),
    Pull(Box<Pull>),
}

pub async fn command(connection: Connection, config: Config) -> Result<(), Error> {
    match config.command {
        Command::Show(show) => {
            let mut client = catalog::Client::new(connection);
            let files = client.get_parquet_files_by_partition_id(show.id).await?;
            println!("{}", serde_json::to_string_pretty(&files)?);

            Ok(())
        }
        Command::Pull(pull) => {
            let metrics = setup_metric_registry();
            let catalog = pull.catalog_dsn.get_catalog("cli", metrics).await?;
            let mut schema_client = schema::Client::new(connection.clone());
            println!(
                "getting schema from remote for namespace {}",
                pull.namespace
            );
            let schema = schema_client.get_schema(&pull.namespace).await?;
            let table_id = schema
                .tables
                .iter()
                .find(|(table_name, _)| *table_name == &pull.table)
                .map(|(_, t)| t.id)
                .ok_or(Error::TableNotFound)?;
            let schema = load_schema(&catalog, &pull.namespace, &schema).await?;

            println!("getting partitions from remote for table {}", pull.table);
            let mut catalog_client = catalog::Client::new(connection.clone());
            let partitions = catalog_client.get_partitions_by_table_id(table_id).await?;
            let partition = partitions
                .into_iter()
                .find(|p| p.key == pull.partition_key)
                .ok_or(Error::PartitionNotFound)?;

            let partition_mapping =
                load_partition(&catalog, &schema, &pull.table, &partition).await?;

            match &pull.object_store.object_store {
                None | Some(ObjectStoreType::Memory | ObjectStoreType::MemoryThrottled) => {
                    return Err(Error::SillyObjectStoreConfig);
                }
                _ => {}
            }

            let object_store =
                make_object_store(&pull.object_store).map_err(Error::ObjectStoreParsing)?;

            println!(
                "getting parquet files from remote for partiton {}",
                partition.key
            );
            let parquet_files = catalog_client
                .get_parquet_files_by_partition_id(partition_mapping.remote_partition_id)
                .await?;

            let parquet_files =
                load_parquet_files(&catalog, schema.id, partition_mapping, parquet_files).await?;

            let mut handles = vec![];
            let store_client = store::Client::new(connection);
            for parquet_file in parquet_files {
                let path = ParquetFilePath::new(
                    parquet_file.namespace_id,
                    parquet_file.table_id,
                    parquet_file.shard_id,
                    parquet_file.partition_id,
                    parquet_file.object_store_id,
                );
                let path = path.object_store_path();
                match object_store.get(&path).await {
                    Ok(_) => {
                        println!(
                            "skipping file {} already in the local object store",
                            parquet_file.object_store_id
                        );
                    }
                    Err(object_store::Error::NotFound { .. }) => {
                        println!("getting file {} from remote", parquet_file.object_store_id);
                        let object_store = Arc::clone(&object_store);
                        let mut store_client = store_client.clone();
                        let task = tokio::task::spawn(async move {
                            let mut res = store_client
                                .get_parquet_file_by_object_store_id(
                                    parquet_file.object_store_id.to_string(),
                                )
                                .await
                                .expect("error getting file from remote");
                            let mut bytes = Vec::new();

                            while let Some(Ok(next)) = res.next().await {
                                bytes.extend_from_slice(next.data.as_ref())
                            }
                            let bytes = Bytes::from(bytes);
                            object_store
                                .put(&path, bytes)
                                .await
                                .expect("error putting file in object store");
                            println!(
                                "wrote file {} to object store",
                                parquet_file.object_store_id
                            );
                        });
                        handles.push(task);
                    }
                    e => return Err(Error::ObjectStore(e.unwrap_err())),
                }
            }

            join_all(handles)
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .expect("worker thread crashed");

            Ok(())
        }
    }
}

const TOPIC_NAME: &str = "iox-shared";
const SHARD_INDEX: ShardIndex = ShardIndex::new(0);
const QUERY_POOL: &str = "iox-shared";

// loads the protobuf namespace schema returned from a remote IOx server into the passed in
// catalog. It does this based on namespace, table, and column names, not IDs. It also inserts
// a topic and query pool for the namespace to use, which aren't for real use, but just
// to make the loaded schema work.
async fn load_schema(
    catalog: &Arc<dyn Catalog>,
    namespace: &str,
    schema: &NamespaceSchema,
) -> Result<CatalogNamespaceSchema, Error> {
    let mut repos = catalog.repositories().await;
    let topic = repos.topics().create_or_get(TOPIC_NAME).await?;
    let query_pool = repos.query_pools().create_or_get(QUERY_POOL).await?;
    // ensure there's a shard for this partition so it can be used later
    let _shard = repos.shards().create_or_get(&topic, SHARD_INDEX).await?;

    let namespace = match repos
        .namespaces()
        .create(namespace, None, topic.id, query_pool.id)
        .await
    {
        Ok(n) => n,
        Err(iox_catalog::interface::Error::NameExists { .. }) => repos
            .namespaces()
            .get_by_name(namespace)
            .await?
            .ok_or(Error::NamespaceNotFound)?,
        e => e?,
    };
    println!("namespace {} loaded into local catalog", &namespace.name);

    for (table_name, table_schema) in &schema.tables {
        let table = repos
            .tables()
            .create_or_get(table_name, namespace.id)
            .await?;
        for (column_name, column_schema) in &table_schema.columns {
            let column_type: ColumnType = column_schema
                .column_type()
                .try_into()
                .expect("column type from remote not valid");
            let _column = repos
                .columns()
                .create_or_get(column_name, table.id, column_type)
                .await?;
        }
        let column_names = &table_schema
            .columns
            .keys()
            .map(|k| k.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        println!(
            "table {} with columns {} loaded into local catalog",
            table_name, column_names
        );
    }

    let full_inserted_schema = get_schema_by_name(&namespace.name, repos.as_mut()).await?;

    Ok(full_inserted_schema)
}

// this function will get the table from the schema and insert a record in the catalog for the
// partition and return the mapping information that can be used to get parquet file records and
// the files.
async fn load_partition(
    catalog: &Arc<dyn Catalog>,
    schema: &CatalogNamespaceSchema,
    table_name: &str,
    remote_partition: &Partition,
) -> Result<PartitionMapping, Error> {
    let mut repos = catalog.repositories().await;
    let topic = repos
        .topics()
        .get_by_name(TOPIC_NAME)
        .await?
        .expect("topic should have been inserted earlier");
    let shard = repos
        .shards()
        .get_by_topic_id_and_shard_index(topic.id, SHARD_INDEX)
        .await?
        .expect("shard should have been inserted earlier");
    let table = schema
        .tables
        .get(table_name)
        .expect("table should have been loaded");
    let partition = repos
        .partitions()
        .create_or_get(remote_partition.key.clone().into(), shard.id, table.id)
        .await?;

    Ok(PartitionMapping {
        shard_id: shard.id,
        table_id: table.id,
        partition_id: partition.id,
        remote_partition_id: remote_partition.id,
    })
}

async fn load_parquet_files(
    catalog: &Arc<dyn Catalog>,
    namespace_id: NamespaceId,
    partition_mapping: PartitionMapping,
    parquet_files: Vec<ParquetFile>,
) -> Result<Vec<CatalogParquetFile>, Error> {
    let mut repos = catalog.repositories().await;

    let mut files = Vec::with_capacity(parquet_files.len());
    for p in parquet_files {
        let uuid = Uuid::parse_str(&p.object_store_id).expect("object store id should be valid");
        let parquet_file = match repos.parquet_files().get_by_object_store_id(uuid).await? {
            Some(f) => {
                println!("found file {} in catalog", uuid);
                f
            }
            None => {
                println!("creating file {} in catalog", uuid);
                let params = ParquetFileParams {
                    shard_id: partition_mapping.shard_id,
                    namespace_id,
                    table_id: partition_mapping.table_id,
                    partition_id: partition_mapping.partition_id,
                    object_store_id: uuid,
                    max_sequence_number: SequenceNumber::new(p.max_sequence_number),
                    min_time: Timestamp::new(p.min_time),
                    max_time: Timestamp::new(p.max_time),
                    file_size_bytes: p.file_size_bytes,
                    row_count: p.row_count,
                    compaction_level: p
                        .compaction_level
                        .try_into()
                        .expect("compaction level should be valid"),
                    created_at: Timestamp::new(p.created_at),
                    column_set: ColumnSet::new(p.column_set.into_iter().map(ColumnId::new)),
                    max_l0_created_at: Timestamp::new(p.max_l0_created_at),
                };

                repos.parquet_files().create(params).await?
            }
        };

        files.push(parquet_file);
    }

    Ok(files)
}

// keeps a mapping of the locally created partition and shard to the remote partition id
struct PartitionMapping {
    shard_id: ShardId,
    table_id: TableId,
    partition_id: PartitionId,
    remote_partition_id: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::{ColumnType, CompactionLevel, ParquetFileId};
    use influxdb_iox_client::schema::generated_types::*;
    use iox_catalog::mem::MemCatalog;
    use std::collections::HashMap;

    #[tokio::test]
    async fn load_schema_fresh() {
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));

        let schema = NamespaceSchema {
            id: 1,
            topic_id: 1,
            query_pool_id: 1,
            tables: HashMap::from([(
                "table1".to_string(),
                TableSchema {
                    id: 1,
                    columns: HashMap::from([(
                        "col1".to_string(),
                        ColumnSchema {
                            id: 1,
                            column_type: 1,
                        },
                    )]),
                },
            )]),
        };
        let res = load_schema(&catalog, "foo", &schema).await.unwrap();

        assert_eq!(res.tables.len(), 1);
        let table = res.tables.get("table1").unwrap();
        assert_eq!(table.columns.len(), 1);
        let column = table.columns.get("col1").unwrap();
        let column_type: ColumnType = 1.try_into().unwrap();
        assert_eq!(column.column_type, column_type);
    }

    #[tokio::test]
    async fn load_schema_update() {
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));

        let schema = NamespaceSchema {
            id: 1,
            topic_id: 1,
            query_pool_id: 1,
            tables: HashMap::from([(
                "table1".to_string(),
                TableSchema {
                    id: 1,
                    columns: HashMap::from([(
                        "col1".to_string(),
                        ColumnSchema {
                            id: 1,
                            column_type: 1,
                        },
                    )]),
                },
            )]),
        };
        assert!(load_schema(&catalog, "foo", &schema).await.is_ok());

        let schema = NamespaceSchema {
            id: 1,
            topic_id: 1,
            query_pool_id: 1,
            tables: HashMap::from([
                (
                    "newtable".to_string(),
                    TableSchema {
                        id: 2,
                        columns: HashMap::from([(
                            "col1".to_string(),
                            ColumnSchema {
                                id: 3,
                                column_type: 1,
                            },
                        )]),
                    },
                ),
                (
                    "table1".to_string(),
                    TableSchema {
                        id: 1,
                        columns: HashMap::from([
                            (
                                "col1".to_string(),
                                ColumnSchema {
                                    id: 1,
                                    column_type: 1,
                                },
                            ),
                            (
                                "newcol".to_string(),
                                ColumnSchema {
                                    id: 2,
                                    column_type: 2,
                                },
                            ),
                        ]),
                    },
                ),
            ]),
        };

        let res = load_schema(&catalog, "foo", &schema).await.unwrap();

        assert_eq!(res.tables.len(), 2);
        let table = res.tables.get("newtable").unwrap();
        assert_eq!(table.columns.len(), 1);
        let column = table.columns.get("col1").unwrap();
        let column_type: ColumnType = 1.try_into().unwrap();
        assert_eq!(column.column_type, column_type);
        let table = res.tables.get("table1").unwrap();
        assert_eq!(table.columns.len(), 2);
        let column = table.columns.get("newcol").unwrap();
        let column_type: ColumnType = 2.try_into().unwrap();
        assert_eq!(column.column_type, column_type);
    }

    #[tokio::test]
    async fn load_parquet_files() {
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let shard;
        let namespace;
        let table;
        let partition;

        {
            let mut repos = catalog.repositories().await;
            let topic = repos.topics().create_or_get(TOPIC_NAME).await.unwrap();
            let query_pool = repos.query_pools().create_or_get(QUERY_POOL).await.unwrap();
            shard = repos
                .shards()
                .create_or_get(&topic, SHARD_INDEX)
                .await
                .unwrap();
            namespace = repos
                .namespaces()
                .create("load_parquet_files", None, topic.id, query_pool.id)
                .await
                .unwrap();
            table = repos
                .tables()
                .create_or_get("pftable", namespace.id)
                .await
                .unwrap();
            partition = repos
                .partitions()
                .create_or_get("1970-01-01".into(), shard.id, table.id)
                .await
                .unwrap();
        }

        let partition_mapping = PartitionMapping {
            shard_id: shard.id,
            table_id: table.id,
            partition_id: partition.id,
            remote_partition_id: 4,
        };

        let object_store_id = uuid::Uuid::new_v4();
        let max_sequence_number = SequenceNumber::new(23);
        let min_time = Timestamp::new(123);
        let max_time = Timestamp::new(456);
        let file_size_bytes = 10;
        let row_count = 51;
        let created_at = Timestamp::new(555);

        let files = super::load_parquet_files(
            &catalog,
            namespace.id,
            partition_mapping,
            vec![ParquetFile {
                id: 98,
                shard_id: 100,
                namespace_id: 100,
                table_id: 100,
                partition_id: 100,
                object_store_id: object_store_id.to_string(),
                max_sequence_number: max_sequence_number.get(),
                min_time: min_time.get(),
                max_time: max_time.get(),
                to_delete: 0,
                file_size_bytes,
                row_count,
                compaction_level: CompactionLevel::Initial as i32,
                created_at: created_at.get(),
                column_set: vec![1, 2],
                max_l0_created_at: created_at.get(),
            }],
        )
        .await
        .unwrap();

        // the inserted parquet file should have shard, namespace, table, and partition ids
        // that match with the ones in the catalog, not the remote. The other values should
        // match those of the remote.
        let expected = vec![CatalogParquetFile {
            id: ParquetFileId::new(1),
            shard_id: shard.id,
            namespace_id: namespace.id,
            table_id: table.id,
            partition_id: partition.id,
            object_store_id,
            max_sequence_number,
            min_time,
            max_time,
            to_delete: None,
            file_size_bytes,
            row_count,
            compaction_level: CompactionLevel::Initial,
            created_at,
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
            max_l0_created_at: created_at,
        }];
        assert_eq!(expected, files);
    }
}
