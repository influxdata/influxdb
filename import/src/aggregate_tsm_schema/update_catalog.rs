use std::{ops::DerefMut, sync::Arc};

use data_types::{
    org_and_bucket_to_database, ColumnType, KafkaTopicId, Namespace, NamespaceSchema,
    OrgBucketMappingError, QueryPoolId, TableSchema,
};
use iox_catalog::interface::{get_schema_by_name, Catalog, ColumnUpsertRequest, RepoCollection};
use schema::{InfluxColumnType, InfluxFieldType};
use thiserror::Error;

use crate::AggregateTSMSchema;

#[derive(Debug, Error)]
pub enum UpdateCatalogError {
    #[error("Error returned from the Catalog: {0}")]
    CatalogError(#[from] iox_catalog::interface::Error),

    #[error("Couldn't construct namespace from org and bucket: {0}")]
    InvalidOrgBucket(#[from] OrgBucketMappingError),

    #[error("No kafka topic named {0} in Catalog")]
    KafkaTopicNotFound(String),

    #[error("No namespace named {0} in Catalog")]
    NamespaceNotFound(String),

    #[error("Failed to update schema in IOx Catalog: {0}")]
    SchemaUpdateError(String),

    #[error("Error creating namespace: {0}")]
    NamespaceCreationError(String),
}

/// Given a merged schema, update the IOx catalog to either merge that schema into the existing one
/// for the namespace, or create the namespace and schema using the merged schema.
/// Will error if the namespace needs to be created but the user hasn't explicitly set the query
/// pool name and retention setting, allowing the user to not provide them if they're not needed.
/// Would have done the same for `kafka_topic` but that comes from the shared clap block and isn't
/// an `Option`.
pub async fn update_iox_catalog<'a>(
    merged_tsm_schema: &'a AggregateTSMSchema,
    kafka_topic: &'a str,
    query_pool_name: Option<&'a str>,
    retention: Option<&'a str>,
    catalog: Arc<dyn Catalog>,
) -> Result<(), UpdateCatalogError> {
    let namespace_name =
        org_and_bucket_to_database(&merged_tsm_schema.org_id, &merged_tsm_schema.bucket_id)
            .map_err(UpdateCatalogError::InvalidOrgBucket)?;
    let mut repos = catalog.repositories().await;
    let iox_schema = match get_schema_by_name(namespace_name.as_str(), repos.deref_mut()).await {
        Ok(iox_schema) => iox_schema,
        Err(iox_catalog::interface::Error::NamespaceNotFoundByName { .. }) => {
            // Namespace has to be created; ensure the user provided the required parameters
            let (query_pool_name, retention) = match (query_pool_name, retention) {
                (Some(query_pool_name), Some(retention)) => (query_pool_name, retention),
                _ => {
                    return Err(UpdateCatalogError::NamespaceCreationError("in order to create the namespace you must provide query_pool_name and retention args".to_string()));
                }
            };
            // create the namespace
            let (topic_id, query_id) =
                get_topic_id_and_query_id(repos.deref_mut(), kafka_topic, query_pool_name).await?;
            let _namespace = create_namespace(
                namespace_name.as_str(),
                retention,
                topic_id,
                query_id,
                repos.deref_mut(),
            )
            .await?;
            // fetch the newly-created schema (which will be empty except for the time column,
            // which won't impact the merge we're about to do)
            match get_schema_by_name(namespace_name.as_str(), repos.deref_mut()).await {
                Ok(iox_schema) => iox_schema,
                Err(e) => return Err(UpdateCatalogError::CatalogError(e)),
            }
        }
        Err(e) => {
            return Err(UpdateCatalogError::CatalogError(e));
        }
    };
    update_catalog_schema_with_merged(iox_schema, merged_tsm_schema, repos.deref_mut()).await?;
    Ok(())
}

async fn get_topic_id_and_query_id<'a, R>(
    repos: &mut R,
    kafka_topic: &'a str,
    query_pool_name: &'a str,
) -> Result<(KafkaTopicId, QueryPoolId), UpdateCatalogError>
where
    R: RepoCollection + ?Sized,
{
    let topic_id = repos
        .kafka_topics()
        .get_by_name(kafka_topic)
        .await
        .map_err(UpdateCatalogError::CatalogError)?
        .map(|v| v.id)
        .ok_or_else(|| UpdateCatalogError::KafkaTopicNotFound(kafka_topic.to_string()))?;
    let query_id = repos
        .query_pools()
        .create_or_get(query_pool_name)
        .await
        .map(|v| v.id)
        .map_err(UpdateCatalogError::CatalogError)?;
    Ok((topic_id, query_id))
}

async fn create_namespace<R>(
    name: &str,
    retention: &str,
    topic_id: KafkaTopicId,
    query_id: QueryPoolId,
    repos: &mut R,
) -> Result<Namespace, UpdateCatalogError>
where
    R: RepoCollection + ?Sized,
{
    match repos
        .namespaces()
        .create(name, retention, topic_id, query_id)
        .await
    {
        Ok(ns) => Ok(ns),
        Err(iox_catalog::interface::Error::NameExists { .. }) => {
            // presumably it got created in the meantime?
            repos
                .namespaces()
                .get_by_name(name)
                .await
                .map_err(UpdateCatalogError::CatalogError)?
                .ok_or_else(|| UpdateCatalogError::NamespaceNotFound(name.to_string()))
        }
        Err(e) => {
            eprintln!("Failed to create namespace");
            Err(UpdateCatalogError::CatalogError(e))
        }
    }
}

/// Merge our aggregate TSM schema into the IOx schema for the namespace in the catalog.
/// This is basically the same as iox_catalog::validate_mutable_batch() but operates on
/// AggregateTSMSchema instead of a MutableBatch (we don't have any data, only a schema)
async fn update_catalog_schema_with_merged<R>(
    iox_schema: NamespaceSchema,
    merged_tsm_schema: &AggregateTSMSchema,
    repos: &mut R,
) -> Result<(), UpdateCatalogError>
where
    R: RepoCollection + ?Sized,
{
    for (measurement_name, measurement) in &merged_tsm_schema.measurements {
        // measurement name -> table name - does it exist in the schema?
        let table = match iox_schema.tables.get(measurement_name) {
            Some(t) => t.clone(),
            None => {
                // it doesn't; create it and add a time column
                let mut table = repos
                    .tables()
                    .create_or_get(measurement_name, iox_schema.id)
                    .await
                    .map(|t| TableSchema::new(t.id))?;
                let time_col = repos
                    .columns()
                    .create_or_get("time", table.id, ColumnType::Time)
                    .await?;
                table.add_column(&time_col);
                table
            }
        };
        // batch of columns to add into the schema at the end
        let mut column_batch = Vec::default();
        // fields and tags are both columns; tag is a special type of column.
        // check that the schema has all these columns or update accordingly.
        for tag in measurement.tags.values() {
            match table.columns.get(tag.name.as_str()) {
                Some(c) if c.is_tag() => {
                    // nothing to do, all good
                }
                Some(_) => {
                    // a column that isn't a tag exists; not good
                    return Err(UpdateCatalogError::SchemaUpdateError(format!(
                        "a non-tag column with name {} already exists in the schema",
                        tag.name.clone()
                    )));
                }
                None => {
                    // column doesn't exist; add it
                    column_batch.push(ColumnUpsertRequest {
                        name: tag.name.as_str(),
                        table_id: table.id,
                        column_type: ColumnType::Tag,
                    });
                }
            }
        }
        for field in measurement.fields.values() {
            // we validated this in the Command with types_are_valid() so these two error checks
            // should never fire
            let field_type = field.types.iter().next().ok_or_else(|| {
                UpdateCatalogError::SchemaUpdateError(format!(
                    "field with no type cannot be converted into an IOx field: {}",
                    field.name
                ))
            })?;
            let influx_column_type =
                InfluxColumnType::Field(InfluxFieldType::try_from(field_type).map_err(|e| {
                    UpdateCatalogError::SchemaUpdateError(format!(
                        "error converting field {} with type {} to an IOx field: {}",
                        field.name, field_type, e,
                    ))
                })?);
            match table.columns.get(field.name.as_str()) {
                Some(c) if c.matches_type(influx_column_type) => {
                    // nothing to do, all good
                }
                Some(_) => {
                    // a column that isn't a tag exists with that name; not good
                    return Err(UpdateCatalogError::SchemaUpdateError(format!(
                        "a column with name {} already exists in the schema with a different type",
                        field.name
                    )));
                }
                None => {
                    // column doesn't exist; add it
                    column_batch.push(ColumnUpsertRequest {
                        name: field.name.as_str(),
                        table_id: table.id,
                        column_type: ColumnType::from(influx_column_type),
                    });
                }
            }
        }
        if !column_batch.is_empty() {
            // add all the new columns we have to create for this table in one batch.
            // it would have been nice to call this once outside the loop and thus put less
            // pressure on the catalog, but because ColumnUpsertRequest takes a slice i can't do
            // that with short-lived loop variables.
            // since this is a CLI tool rather than something called a lot on the write path, i
            // figure it's okay.
            repos.columns().create_or_get_many(&column_batch).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use assert_matches::assert_matches;
    use iox_catalog::mem::MemCatalog;

    #[tokio::test]
    async fn needs_creating() {
        // init a test catalog stack
        let metrics = Arc::new(metric::Registry::default());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        catalog
            .repositories()
            .await
            .kafka_topics()
            .create_or_get("iox_shared")
            .await
            .expect("topic created");

        let json = r#"
        {
          "org_id": "1234",
          "bucket_id": "5678",
          "measurements": {
            "cpu": {
              "tags": [
                { "name": "host", "values": ["server", "desktop"] }
              ],
             "fields": [
                { "name": "usage", "types": ["Float"] }
              ]
            }
          }
        }
        "#;
        let agg_schema: AggregateTSMSchema = json.try_into().unwrap();
        update_iox_catalog(
            &agg_schema,
            "iox_shared",
            Some("iox_shared"),
            Some("inf"),
            Arc::clone(&catalog),
        )
        .await
        .expect("schema update worked");
        let mut repos = catalog.repositories().await;
        let iox_schema = get_schema_by_name("1234_5678", repos.deref_mut())
            .await
            .expect("got schema");
        assert_eq!(iox_schema.tables.len(), 1);
        let table = iox_schema.tables.get("cpu").expect("got table");
        assert_eq!(table.columns.len(), 3); // one tag & one field, plus time
        let tag = table.columns.get("host").expect("got tag");
        assert!(tag.is_tag());
        let field = table.columns.get("usage").expect("got field");
        assert_eq!(
            field.column_type,
            InfluxColumnType::Field(InfluxFieldType::Float)
        );
    }

    #[tokio::test]
    async fn needs_merging() {
        // init a test catalog stack
        let metrics = Arc::new(metric::Registry::default());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let mut txn = catalog
            .start_transaction()
            .await
            .expect("started transaction");
        txn.kafka_topics()
            .create_or_get("iox_shared")
            .await
            .expect("topic created");

        // create namespace, table and columns for weather measurement
        let namespace = txn
            .namespaces()
            .create(
                "1234_5678",
                "inf",
                KafkaTopicId::new(1),
                QueryPoolId::new(1),
            )
            .await
            .expect("namespace created");
        let mut table = txn
            .tables()
            .create_or_get("weather", namespace.id)
            .await
            .map(|t| TableSchema::new(t.id))
            .expect("table created");
        let time_col = txn
            .columns()
            .create_or_get("time", table.id, ColumnType::Time)
            .await
            .expect("column created");
        table.add_column(&time_col);
        let location_col = txn
            .columns()
            .create_or_get("city", table.id, ColumnType::Tag)
            .await
            .expect("column created");
        let temperature_col = txn
            .columns()
            .create_or_get("temperature", table.id, ColumnType::F64)
            .await
            .expect("column created");
        table.add_column(&location_col);
        table.add_column(&temperature_col);
        txn.commit().await.unwrap();

        // merge with aggregate schema that has some overlap
        let json = r#"
        {
          "org_id": "1234",
          "bucket_id": "5678",
          "measurements": {
            "weather": {
              "tags": [
                { "name": "country", "values": ["United Kingdom"] }
              ],
             "fields": [
                { "name": "temperature", "types": ["Float"] },
                { "name": "humidity", "types": ["Float"] }
              ]
            }
          }
        }
        "#;
        let agg_schema: AggregateTSMSchema = json.try_into().unwrap();
        update_iox_catalog(
            &agg_schema,
            "iox_shared",
            Some("iox_shared"),
            Some("inf"),
            Arc::clone(&catalog),
        )
        .await
        .expect("schema update worked");
        let mut repos = catalog.repositories().await;
        let iox_schema = get_schema_by_name("1234_5678", repos.deref_mut())
            .await
            .expect("got schema");
        assert_eq!(iox_schema.tables.len(), 1);
        let table = iox_schema.tables.get("weather").expect("got table");
        assert_eq!(table.columns.len(), 5); // two tags, two fields, plus time
        let tag1 = table.columns.get("city").expect("got tag");
        assert!(tag1.is_tag());
        let tag2 = table.columns.get("country").expect("got tag");
        assert!(tag2.is_tag());
        let field1 = table.columns.get("temperature").expect("got field");
        assert_eq!(
            field1.column_type,
            InfluxColumnType::Field(InfluxFieldType::Float)
        );
        let field2 = table.columns.get("humidity").expect("got field");
        assert_eq!(
            field2.column_type,
            InfluxColumnType::Field(InfluxFieldType::Float)
        );
    }

    #[tokio::test]
    async fn needs_merging_duplicate_tag_field_name() {
        // init a test catalog stack
        let metrics = Arc::new(metric::Registry::default());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let mut txn = catalog
            .start_transaction()
            .await
            .expect("started transaction");
        txn.kafka_topics()
            .create_or_get("iox_shared")
            .await
            .expect("topic created");

        // create namespace, table and columns for weather measurement
        let namespace = txn
            .namespaces()
            .create(
                "1234_5678",
                "inf",
                KafkaTopicId::new(1),
                QueryPoolId::new(1),
            )
            .await
            .expect("namespace created");
        let mut table = txn
            .tables()
            .create_or_get("weather", namespace.id)
            .await
            .map(|t| TableSchema::new(t.id))
            .expect("table created");
        let time_col = txn
            .columns()
            .create_or_get("time", table.id, ColumnType::Time)
            .await
            .expect("column created");
        table.add_column(&time_col);
        let temperature_col = txn
            .columns()
            .create_or_get("temperature", table.id, ColumnType::F64)
            .await
            .expect("column created");
        table.add_column(&temperature_col);
        txn.commit().await.unwrap();

        // merge with aggregate schema that has some issue that will trip a catalog error
        let json = r#"
        {
          "org_id": "1234",
          "bucket_id": "5678",
          "measurements": {
            "weather": {
              "tags": [
                { "name": "temperature", "values": ["unseasonably_warm"] }
              ],
             "fields": [
                { "name": "temperature", "types": ["Float"] }
              ]
            }
          }
        }
        "#;
        let agg_schema: AggregateTSMSchema = json.try_into().unwrap();
        let err = update_iox_catalog(
            &agg_schema,
            "iox_shared",
            Some("iox_shared"),
            Some("inf"),
            Arc::clone(&catalog),
        )
        .await
        .expect_err("should fail catalog update");
        assert_matches!(err, UpdateCatalogError::SchemaUpdateError(_));
        assert!(err
            .to_string()
            .ends_with("a non-tag column with name temperature already exists in the schema"));
    }

    #[tokio::test]
    async fn needs_merging_column_exists_different_type() {
        // init a test catalog stack
        let metrics = Arc::new(metric::Registry::default());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let mut txn = catalog
            .start_transaction()
            .await
            .expect("started transaction");
        txn.kafka_topics()
            .create_or_get("iox_shared")
            .await
            .expect("topic created");

        // create namespace, table and columns for weather measurement
        let namespace = txn
            .namespaces()
            .create(
                "1234_5678",
                "inf",
                KafkaTopicId::new(1),
                QueryPoolId::new(1),
            )
            .await
            .expect("namespace created");
        let mut table = txn
            .tables()
            .create_or_get("weather", namespace.id)
            .await
            .map(|t| TableSchema::new(t.id))
            .expect("table created");
        let time_col = txn
            .columns()
            .create_or_get("time", table.id, ColumnType::Time)
            .await
            .expect("column created");
        table.add_column(&time_col);
        let temperature_col = txn
            .columns()
            .create_or_get("temperature", table.id, ColumnType::F64)
            .await
            .expect("column created");
        table.add_column(&temperature_col);
        txn.commit().await.unwrap();

        // merge with aggregate schema that has some issue that will trip a catalog error
        let json = r#"
        {
          "org_id": "1234",
          "bucket_id": "5678",
          "measurements": {
            "weather": {
              "tags": [
              ],
             "fields": [
                { "name": "temperature", "types": ["Integer"] }
              ]
            }
          }
        }
        "#;
        let agg_schema: AggregateTSMSchema = json.try_into().unwrap();
        let err = update_iox_catalog(
            &agg_schema,
            "iox_shared",
            Some("iox_shared"),
            Some("inf"),
            Arc::clone(&catalog),
        )
        .await
        .expect_err("should fail catalog update");
        assert_matches!(err, UpdateCatalogError::SchemaUpdateError(_));
        assert!(err.to_string().ends_with(
            "a column with name temperature already exists in the schema with a different type"
        ));
    }

    #[tokio::test]
    async fn needs_creating_but_missing_query_pool() {
        // init a test catalog stack
        let metrics = Arc::new(metric::Registry::default());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        catalog
            .repositories()
            .await
            .kafka_topics()
            .create_or_get("iox_shared")
            .await
            .expect("topic created");

        let json = r#"
        {
          "org_id": "1234",
          "bucket_id": "5678",
          "measurements": {
            "cpu": {
              "tags": [
                { "name": "host", "values": ["server", "desktop"] }
              ],
             "fields": [
                { "name": "usage", "types": ["Float"] }
              ]
            }
          }
        }
        "#;
        let agg_schema: AggregateTSMSchema = json.try_into().unwrap();
        let err = update_iox_catalog(
            &agg_schema,
            "iox_shared",
            None,
            Some("inf"),
            Arc::clone(&catalog),
        )
        .await
        .expect_err("should fail namespace creation");
        assert_matches!(err, UpdateCatalogError::NamespaceCreationError(_));
    }

    #[tokio::test]
    async fn needs_creating_but_missing_retention() {
        // init a test catalog stack
        let metrics = Arc::new(metric::Registry::default());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        catalog
            .repositories()
            .await
            .kafka_topics()
            .create_or_get("iox_shared")
            .await
            .expect("topic created");

        let json = r#"
        {
          "org_id": "1234",
          "bucket_id": "5678",
          "measurements": {
            "cpu": {
              "tags": [
                { "name": "host", "values": ["server", "desktop"] }
              ],
             "fields": [
                { "name": "usage", "types": ["Float"] }
              ]
            }
          }
        }
        "#;
        let agg_schema: AggregateTSMSchema = json.try_into().unwrap();
        let err = update_iox_catalog(
            &agg_schema,
            "iox_shared",
            Some("iox-shared"),
            None,
            Arc::clone(&catalog),
        )
        .await
        .expect_err("should fail namespace creation");
        assert_matches!(err, UpdateCatalogError::NamespaceCreationError(_));
    }
}
