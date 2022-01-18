//! The IOx catalog which keeps track of what namespaces, tables, columns, parquet files,
//! and deletes are in the system. Configuration information for distributing ingest, query
//! and compaction is also stored here.
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use crate::interface::{
    column_type_from_field, ColumnSchema, ColumnType, Error, KafkaPartition, KafkaTopic,
    NamespaceSchema, QueryPool, RepoCollection, Result, Sequencer, SequencerId, TableId,
};
use futures::{stream::FuturesOrdered, StreamExt};
use influxdb_line_protocol::ParsedLine;
use std::collections::BTreeMap;

#[allow(dead_code)]
const SHARED_KAFKA_TOPIC: &str = "iox_shared";
const SHARED_QUERY_POOL: &str = SHARED_KAFKA_TOPIC;
const TIME_COLUMN: &str = "time";

pub mod interface;
pub mod mem;
pub mod postgres;

/// Given the lines of a write request and an in memory schema, this will validate the write
/// against the schema, or if new schema is defined, attempt to insert it into the Postgres
/// catalog. If any new schema is created or found, this function will return a new
/// `NamespaceSchema` struct which can replace the passed in one in cache.
///
/// If another writer attempts to create a column of the same name with a different
/// type at the same time and beats this caller to it, an error will be returned. If another
/// writer adds the same schema before this one, then this will load that schema here.
pub async fn validate_or_insert_schema<T: RepoCollection + Sync + Send>(
    lines: Vec<ParsedLine<'_>>,
    schema: &NamespaceSchema,
    repo: &T,
) -> Result<Option<NamespaceSchema>> {
    // table name to table_id
    let mut new_tables: BTreeMap<String, TableId> = BTreeMap::new();
    // table_id to map of column name to column
    let mut new_columns: BTreeMap<TableId, BTreeMap<String, ColumnSchema>> = BTreeMap::new();

    for line in &lines {
        let table_name = line.series.measurement.as_str();
        match schema.tables.get(table_name) {
            Some(table) => {
                // validate existing tags or insert in new
                if let Some(tagset) = &line.series.tag_set {
                    for (key, _) in tagset {
                        match table.columns.get(key.as_str()) {
                            Some(c) => {
                                if !c.is_tag() {
                                    return Err(Error::ColumnTypeMismatch {
                                        name: key.to_string(),
                                        existing: c.column_type.to_string(),
                                        new: ColumnType::Tag.to_string(),
                                    });
                                };
                            }
                            None => {
                                let entry = new_columns.entry(table.id).or_default();
                                if entry.get(key.as_str()).is_none() {
                                    let column_repo = repo.column();
                                    let column = column_repo
                                        .create_or_get(key.as_str(), table.id, ColumnType::Tag)
                                        .await?;
                                    entry.insert(
                                        column.name,
                                        ColumnSchema {
                                            id: column.id,
                                            column_type: ColumnType::Tag,
                                        },
                                    );
                                }
                            }
                        }
                    }
                }

                // validate existing fields or insert
                for (key, value) in &line.field_set {
                    if let Some(column) = table.columns.get(key.as_str()) {
                        if !column.matches_field_type(value) {
                            return Err(Error::ColumnTypeMismatch {
                                name: key.to_string(),
                                existing: column.column_type.to_string(),
                                new: column_type_from_field(value).to_string(),
                            });
                        }
                    } else {
                        let entry = new_columns.entry(table.id).or_default();
                        if entry.get(key.as_str()).is_none() {
                            let data_type = column_type_from_field(value);
                            let column_repo = repo.column();
                            let column = column_repo
                                .create_or_get(key.as_str(), table.id, data_type)
                                .await?;
                            entry.insert(
                                column.name,
                                ColumnSchema {
                                    id: column.id,
                                    column_type: data_type,
                                },
                            );
                        }
                    }
                }
            }
            None => {
                let table_repo = repo.table();
                let new_table = table_repo.create_or_get(table_name, schema.id).await?;
                let new_table_columns = new_columns.entry(new_table.id).or_default();

                let column_repo = repo.column();

                if let Some(tagset) = &line.series.tag_set {
                    for (key, _) in tagset {
                        let new_column = column_repo
                            .create_or_get(key.as_str(), new_table.id, ColumnType::Tag)
                            .await?;
                        new_table_columns.insert(
                            new_column.name,
                            ColumnSchema {
                                id: new_column.id,
                                column_type: ColumnType::Tag,
                            },
                        );
                    }
                }
                for (key, value) in &line.field_set {
                    let data_type = column_type_from_field(value);
                    let new_column = column_repo
                        .create_or_get(key.as_str(), new_table.id, data_type)
                        .await?;
                    new_table_columns.insert(
                        new_column.name,
                        ColumnSchema {
                            id: new_column.id,
                            column_type: data_type,
                        },
                    );
                }
                let time_column = column_repo
                    .create_or_get(TIME_COLUMN, new_table.id, ColumnType::Time)
                    .await?;
                new_table_columns.insert(
                    time_column.name,
                    ColumnSchema {
                        id: time_column.id,
                        column_type: ColumnType::Time,
                    },
                );

                new_tables.insert(new_table.name, new_table.id);
            }
        };
    }

    if !new_tables.is_empty() || !new_columns.is_empty() {
        let mut new_schema = schema.clone();
        new_schema.add_tables_and_columns(new_tables, new_columns);
        return Ok(Some(new_schema));
    }

    Ok(None)
}

/// Creates or gets records in the catalog for the shared kafka topic, query pool, and sequencers for
/// each of the partitions.
pub async fn create_or_get_default_records<T: RepoCollection + Sync + Send>(
    kafka_partition_count: i32,
    repo: &T,
) -> Result<(KafkaTopic, QueryPool, BTreeMap<SequencerId, Sequencer>)> {
    let kafka_repo = repo.kafka_topic();
    let query_repo = repo.query_pool();
    let sequencer_repo = repo.sequencer();

    let kafka_topic = kafka_repo.create_or_get(SHARED_KAFKA_TOPIC).await?;
    let query_pool = query_repo.create_or_get(SHARED_QUERY_POOL).await?;

    let sequencers = (1..=kafka_partition_count)
        .map(|partition| sequencer_repo.create_or_get(&kafka_topic, KafkaPartition::new(partition)))
        .collect::<FuturesOrdered<_>>()
        .map(|v| {
            let v = v.expect("failed to create sequencer");
            (v.id, v)
        })
        .collect::<BTreeMap<_, _>>()
        .await;

    Ok((kafka_topic, query_pool, sequencers))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interface::get_schema_by_name;
    use crate::mem::MemCatalog;
    use influxdb_line_protocol::parse_lines;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_validate_or_insert_schema() {
        let repo = Arc::new(MemCatalog::new());
        let (kafka_topic, query_pool, _) = create_or_get_default_records(2, &repo).await.unwrap();

        let namespace_name = "validate_schema";
        // now test with a new namespace
        let namespace = repo
            .namespace()
            .create(namespace_name, "inf", kafka_topic.id, query_pool.id)
            .await
            .unwrap();
        let data = r#"
m1,t1=a,t2=b f1=2i,f2=2.0 1
m1,t1=a f1=3i 2
m2,t3=b f1=true 1
        "#;

        // test that new schema gets returned
        let lines: Vec<_> = parse_lines(data).map(|l| l.unwrap()).collect();
        let schema = Arc::new(NamespaceSchema::new(
            namespace.id,
            namespace.kafka_topic_id,
            namespace.query_pool_id,
        ));
        let new_schema = validate_or_insert_schema(lines, &schema, &repo)
            .await
            .unwrap();
        let new_schema = new_schema.unwrap();

        // ensure new schema is in the db
        let schema_from_db = get_schema_by_name(namespace_name, &repo)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(new_schema, schema_from_db);

        // test that a new table will be created
        let data = r#"
m1,t1=c f1=1i 2
new_measurement,t9=a f10=true 1
        "#;
        let lines: Vec<_> = parse_lines(data).map(|l| l.unwrap()).collect();
        let new_schema = validate_or_insert_schema(lines, &schema_from_db, &repo)
            .await
            .unwrap()
            .unwrap();
        let new_table = new_schema.tables.get("new_measurement").unwrap();
        assert_eq!(
            ColumnType::Bool,
            new_table.columns.get("f10").unwrap().column_type
        );
        assert_eq!(
            ColumnType::Tag,
            new_table.columns.get("t9").unwrap().column_type
        );
        let schema = get_schema_by_name(namespace_name, &repo)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(new_schema, schema);

        // test that a new column for an existing table will be created
        // test that a new table will be created
        let data = r#"
m1,new_tag=c new_field=1i 2
        "#;
        let lines: Vec<_> = parse_lines(data).map(|l| l.unwrap()).collect();
        let new_schema = validate_or_insert_schema(lines, &schema, &repo)
            .await
            .unwrap()
            .unwrap();
        let table = new_schema.tables.get("m1").unwrap();
        assert_eq!(
            ColumnType::I64,
            table.columns.get("new_field").unwrap().column_type
        );
        assert_eq!(
            ColumnType::Tag,
            table.columns.get("new_tag").unwrap().column_type
        );
        let schema = get_schema_by_name(namespace_name, &repo)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(new_schema, schema);
    }
}
