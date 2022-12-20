use self::generated_types::{shard_service_client::ShardServiceClient, *};
use crate::{AggregateTSMMeasurement, AggregateTSMSchema};
use chrono::{format::StrftimeItems, offset::FixedOffset, DateTime, Duration};
use data_types::{
    org_and_bucket_to_namespace, ColumnType, Namespace, NamespaceSchema, OrgBucketMappingError,
    Partition, PartitionKey, QueryPoolId, ShardId, TableSchema, TopicId,
};
use influxdb_iox_client::connection::{Connection, GrpcConnection};
use iox_catalog::interface::{get_schema_by_name, CasFailure, Catalog, RepoCollection};
use schema::{
    sort::{adjust_sort_key_columns, SortKey, SortKeyBuilder},
    InfluxColumnType, InfluxFieldType, TIME_COLUMN_NAME,
};
use std::{collections::HashMap, fmt::Write, ops::DerefMut, sync::Arc};
use thiserror::Error;

pub mod generated_types {
    pub use generated_types::influxdata::iox::sharder::v1::*;
}

#[derive(Debug, Error)]
pub enum UpdateCatalogError {
    #[error("Error returned from the Catalog: {0}")]
    CatalogError(#[from] iox_catalog::interface::Error),

    #[error("Error returned from the Catalog: failed to cas sort key update")]
    SortKeyCasError,

    #[error("Couldn't construct namespace from org and bucket: {0}")]
    InvalidOrgBucket(#[from] OrgBucketMappingError),

    #[error("No topic named '{topic_name}' found in the catalog")]
    TopicCatalogLookup { topic_name: String },

    #[error("No namespace named {0} in Catalog")]
    NamespaceNotFound(String),

    #[error("Failed to update schema in IOx Catalog: {0}")]
    SchemaUpdateError(String),

    #[error("Error creating namespace: {0}")]
    NamespaceCreationError(String),

    #[error("Time calculation error when deriving partition key: {0}")]
    PartitionKeyCalculationError(String),

    #[error("Error fetching shard ID from shard service: {0}")]
    ShardServiceError(#[from] tonic::Status),
}

/// Given a merged schema, update the IOx catalog to either merge that schema into the existing one
/// for the namespace, or create the namespace and schema using the merged schema.
/// Will error if the namespace needs to be created but the user hasn't explicitly set the query
/// pool name and retention setting, allowing the user to not provide them if they're not needed.
/// Would have done the same for `topic` but that comes from the shared clap block and isn't
/// an `Option`.
pub async fn update_iox_catalog<'a>(
    merged_tsm_schema: &'a AggregateTSMSchema,
    topic: &'a str,
    query_pool_name: Option<&'a str>,
    catalog: Arc<dyn Catalog>,
    connection: Connection,
) -> Result<(), UpdateCatalogError> {
    let namespace_name =
        org_and_bucket_to_namespace(&merged_tsm_schema.org_id, &merged_tsm_schema.bucket_id)
            .map_err(UpdateCatalogError::InvalidOrgBucket)?;
    let mut repos = catalog.repositories().await;
    let iox_schema = match get_schema_by_name(namespace_name.as_str(), repos.deref_mut()).await {
        Ok(iox_schema) => iox_schema,
        Err(iox_catalog::interface::Error::NamespaceNotFoundByName { .. }) => {
            // Namespace has to be created; ensure the user provided the required parameters
            let query_pool_name = match query_pool_name {
                Some(query_pool_name) => query_pool_name,
                _ => {
                    return Err(UpdateCatalogError::NamespaceCreationError("in order to create the namespace you must provide query_pool_name and retention args".to_string()));
                }
            };
            // create the namespace
            let (topic_id, query_id) =
                get_topic_id_and_query_id(repos.deref_mut(), topic, query_pool_name).await?;
            let _namespace = create_namespace(
                namespace_name.as_str(),
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
    // initialise a client of the shard service in the router. we will use it to find out which
    // shard a table/namespace combo would shard to, without exposing the implementation
    // details of the sharding
    let mut shard_client = ShardServiceClient::new(connection.into_grpc_connection());
    update_catalog_schema_with_merged(
        namespace_name.as_str(),
        iox_schema,
        merged_tsm_schema,
        repos.deref_mut(),
        &mut shard_client,
    )
    .await?;
    Ok(())
}

async fn get_topic_id_and_query_id<'a, R>(
    repos: &mut R,
    topic_name: &'a str,
    query_pool_name: &'a str,
) -> Result<(TopicId, QueryPoolId), UpdateCatalogError>
where
    R: RepoCollection + ?Sized,
{
    let topic_id = repos
        .topics()
        .get_by_name(topic_name)
        .await
        .map_err(UpdateCatalogError::CatalogError)?
        .map(|v| v.id)
        .ok_or_else(|| UpdateCatalogError::TopicCatalogLookup {
            topic_name: topic_name.to_string(),
        })?;
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
    topic_id: TopicId,
    query_id: QueryPoolId,
    repos: &mut R,
) -> Result<Namespace, UpdateCatalogError>
where
    R: RepoCollection + ?Sized,
{
    match repos
        .namespaces()
        .create(name, None, topic_id, query_id)
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
    namespace_name: &str,
    iox_schema: NamespaceSchema,
    merged_tsm_schema: &AggregateTSMSchema,
    repos: &mut R,
    shard_client: &mut ShardServiceClient<GrpcConnection>,
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
        let mut column_batch = HashMap::new();
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
                    let old = column_batch.insert(tag.name.as_str(), ColumnType::Tag);
                    assert!(
                        old.is_none(),
                        "duplicate column name `{}` in new column batch shouldn't be possible",
                        tag.name
                    );
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
                    let old = column_batch
                        .insert(field.name.as_str(), ColumnType::from(influx_column_type));
                    assert!(
                        old.is_none(),
                        "duplicate column name `{}` in new column batch shouldn't be possible",
                        field.name
                    );
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
            repos
                .columns()
                .create_or_get_many_unchecked(table.id, column_batch)
                .await?;
        }
        // create a partition for every day in the date range.
        // N.B. this will need updating if we someday support partitioning by inputs other than
        // date, but this is what the router logic currently does so that would need to change too.
        let partition_keys =
            get_partition_keys_for_range(measurement.earliest_time, measurement.latest_time)?;
        let response = shard_client
            .map_to_shard(tonic::Request::new(MapToShardRequest {
                table_name: measurement_name.clone(),
                namespace_name: namespace_name.to_string(),
            }))
            .await?;
        let shard_id = ShardId::new(response.into_inner().shard_id);
        for partition_key in partition_keys {
            // create the partition if it doesn't exist; new partitions get an empty sort key which
            // gets matched as `None`` in the code below
            let partition = repos
                .partitions()
                .create_or_get(partition_key, shard_id, table.id)
                .await
                .map_err(UpdateCatalogError::CatalogError)?;
            // get the sort key from the partition, if it exists. create it or update it as
            // necessary
            if let (_metadata_sort_key, Some(sort_key)) = get_sort_key(&partition, measurement) {
                let sort_key = sort_key.to_columns().collect::<Vec<_>>();
                repos
                    .partitions()
                    .cas_sort_key(partition.id, Some(partition.sort_key), &sort_key)
                    .await
                    .map_err(|e| match e {
                        CasFailure::ValueMismatch(_) => UpdateCatalogError::SortKeyCasError,
                        CasFailure::QueryError(e) => UpdateCatalogError::CatalogError(e),
                    })?;
            }
        }
    }
    Ok(())
}

fn get_sort_key(
    partition: &Partition,
    measurement: &AggregateTSMMeasurement,
) -> (SortKey, Option<SortKey>) {
    let metadata_sort_key = partition.sort_key();
    match metadata_sort_key.as_ref() {
        Some(sk) => {
            // the partition already has a sort key; check if there are any modifications required
            // to it based on the primary key of this measurement. the second term of the tuple
            // returned contains the updated sort key for the catalog.
            let primary_key = compute_measurement_primary_key(measurement);
            adjust_sort_key_columns(sk, &primary_key)
        }
        None => {
            // the partition doesn't yet have a sort key (because it's newly created and set to
            // empty), so compute the optimal sort key based on the measurement schema. this will
            // use the cardinality of the tags in the aggregate TSM schema to set the sort key. IOx
            // makes the gamble that the first data written will be representative and that the
            // sort key calculated here will remain sufficiently optimal.
            // second term of the tuple is the same as the first because we are creating the sort
            // key.
            let sort_key = compute_measurement_sort_key(measurement);
            (sort_key.clone(), Some(sort_key))
        }
    }
}

fn compute_measurement_primary_key(measurement: &AggregateTSMMeasurement) -> Vec<&str> {
    let mut primary_keys: Vec<_> = measurement.tags.keys().map(|k| k.as_str()).collect();
    primary_keys.sort();
    primary_keys.push("time");
    primary_keys
}

// based on schema::sort::compute_sort_key but works with AggregateTSMMeasurement rather than
// MutableBatch, which has done the tag value collation work for us already
fn compute_measurement_sort_key(measurement: &AggregateTSMMeasurement) -> SortKey {
    // use the number of tag values from the measurement to compute cardinality; then sort
    let mut cardinalities: HashMap<String, u64> = HashMap::new();
    measurement.tags.values().for_each(|tag| {
        cardinalities.insert(
            tag.name.clone(),
            tag.values.len().try_into().expect("usize -> u64 overflow"),
        );
    });
    let mut cardinalities: Vec<_> = cardinalities.into_iter().collect();
    cardinalities.sort_by_cached_key(|x| (x.1, x.0.clone()));

    // build a sort key from the tag cardinality data
    let mut builder = SortKeyBuilder::with_capacity(cardinalities.len() + 1);
    for (col, _) in cardinalities {
        builder = builder.with_col(col)
    }
    builder = builder.with_col(TIME_COLUMN_NAME);
    builder.build()
}

fn get_partition_keys_for_range(
    earliest_time: DateTime<FixedOffset>,
    latest_time: DateTime<FixedOffset>,
) -> Result<Vec<PartitionKey>, UpdateCatalogError> {
    if latest_time.lt(&earliest_time) {
        // we have checked this elsewhere but just guarding against refactors!
        return Err(UpdateCatalogError::PartitionKeyCalculationError(
            "latest time earlier than earliest time".to_string(),
        ));
    }
    let mut keys = vec![];
    let mut d = earliest_time;
    while d <= latest_time {
        keys.push(datetime_to_partition_key(&d)?);
        d += Duration::days(1);
    }
    // the above while loop logic will miss the end date for a range that is less than a day but
    // crosses a date boundary, so...
    keys.push(datetime_to_partition_key(&latest_time)?);
    keys.dedup();
    Ok(keys)
}

fn datetime_to_partition_key(
    datetime: &DateTime<FixedOffset>,
) -> Result<PartitionKey, UpdateCatalogError> {
    let mut partition_key = String::new();
    write!(
        partition_key,
        "{}",
        datetime.format_with_items(StrftimeItems::new("%Y-%m-%d")),
    )
    .map_err(|e| UpdateCatalogError::PartitionKeyCalculationError(e.to_string()))?;
    Ok(partition_key.into())
}

#[cfg(test)]
mod tests {
    use super::{generated_types::shard_service_server::ShardService, *};
    use crate::{AggregateTSMField, AggregateTSMTag};
    use assert_matches::assert_matches;
    use client_util::connection::Builder;
    use data_types::{PartitionId, TableId};
    use iox_catalog::mem::MemCatalog;
    use parking_lot::RwLock;
    use std::{collections::HashSet, net::SocketAddr};
    use tokio::task::JoinHandle;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    struct MockShardService {
        requests: Arc<RwLock<Vec<MapToShardRequest>>>,
        reply_with: MapToShardResponse,
    }

    impl MockShardService {
        pub fn new(response: MapToShardResponse) -> Self {
            MockShardService {
                requests: Arc::new(RwLock::new(vec![])),
                reply_with: response,
            }
        }

        /// Use to replace the next reply with the given response (not currently used but would be
        /// handy for expanded tests)
        #[allow(dead_code)]
        pub fn with_reply(mut self, response: MapToShardResponse) -> Self {
            self.reply_with = response;
            self
        }

        /// Get all the requests that were made to the mock (not currently used but would be handy
        /// for expanded tests)
        #[allow(dead_code)]
        pub fn get_requests(&self) -> Arc<RwLock<Vec<MapToShardRequest>>> {
            Arc::clone(&self.requests)
        }
    }

    #[tonic::async_trait]
    impl ShardService for MockShardService {
        async fn map_to_shard(
            &self,
            request: tonic::Request<MapToShardRequest>,
        ) -> Result<tonic::Response<MapToShardResponse>, tonic::Status> {
            self.requests.write().push(request.into_inner());
            Ok(tonic::Response::new(self.reply_with.clone()))
        }
    }

    async fn create_test_shard_service(
        response: MapToShardResponse,
    ) -> (
        Connection,
        JoinHandle<()>,
        Arc<RwLock<Vec<MapToShardRequest>>>,
    ) {
        let bind_addr = SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            0,
        );
        let socket = tokio::net::TcpListener::bind(bind_addr)
            .await
            .expect("failed to bind to socket in test");
        let bind_addr = socket.local_addr().unwrap();
        let sharder = MockShardService::new(response);
        let requests = Arc::clone(&sharder.get_requests());
        let server =
            Server::builder().add_service(shard_service_server::ShardServiceServer::new(sharder));
        let server = async move {
            let stream = TcpListenerStream::new(socket);
            server.serve_with_incoming(stream).await.ok();
        };
        let join_handle = tokio::task::spawn(server);
        let connection = Builder::default()
            .build(format!("http://{}", bind_addr))
            .await
            .expect("failed to connect to server");
        (connection, join_handle, requests)
    }

    #[tokio::test]
    async fn needs_creating() {
        // init a test catalog stack
        let metrics = Arc::new(metric::Registry::default());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        catalog
            .repositories()
            .await
            .topics()
            .create_or_get("iox-shared")
            .await
            .expect("topic created");
        let (connection, _join_handle, _requests) = create_test_shard_service(MapToShardResponse {
            shard_id: 0,
            shard_index: 0,
        })
        .await;

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
              ],
              "earliest_time": "2022-01-01T00:00:00.00Z",
              "latest_time": "2022-07-07T06:00:00.00Z"
            }
          }
        }
        "#;
        let agg_schema: AggregateTSMSchema = json.try_into().unwrap();
        update_iox_catalog(
            &agg_schema,
            "iox-shared",
            Some("iox-shared"),
            Arc::clone(&catalog),
            connection,
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
        // check that the partitions were created and the sort keys are correct
        let partitions = repos
            .partitions()
            .list_by_table_id(table.id)
            .await
            .expect("got partitions");
        // number of days in the date range of the schema
        assert_eq!(partitions.len(), 188);
        // check the sort keys of the first and last as a sanity check (that code is tested more
        // thoroughly below)
        let (first_partition, last_partition) = {
            let mut partitions_iter = partitions.into_iter();
            (
                partitions_iter.next().expect("partitions not empty"),
                partitions_iter.last().expect("partitions not empty"),
            )
        };
        let first_sort_key = first_partition.sort_key;
        let last_sort_key = last_partition.sort_key;
        // ensure sort key is updated; new columns get appended after existing ones only
        assert_eq!(first_sort_key, vec!["host", "time"]);
        assert_eq!(last_sort_key, vec!["host", "time"]);
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
        txn.topics()
            .create_or_get("iox-shared")
            .await
            .expect("topic created");
        let (connection, _join_handle, _requests) = create_test_shard_service(MapToShardResponse {
            shard_id: 0,
            shard_index: 0,
        })
        .await;

        // create namespace, table and columns for weather measurement
        let namespace = txn
            .namespaces()
            .create("1234_5678", None, TopicId::new(1), QueryPoolId::new(1))
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
              ],
              "earliest_time": "2022-01-01T00:00:00.00Z",
              "latest_time": "2022-07-07T06:00:00.00Z"
            }
          }
        }
        "#;
        let agg_schema: AggregateTSMSchema = json.try_into().unwrap();
        update_iox_catalog(
            &agg_schema,
            "iox-shared",
            Some("iox-shared"),
            Arc::clone(&catalog),
            connection,
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
        txn.topics()
            .create_or_get("iox-shared")
            .await
            .expect("topic created");
        let (connection, _join_handle, _requests) = create_test_shard_service(MapToShardResponse {
            shard_id: 0,
            shard_index: 0,
        })
        .await;

        // create namespace, table and columns for weather measurement
        let namespace = txn
            .namespaces()
            .create("1234_5678", None, TopicId::new(1), QueryPoolId::new(1))
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
              ],
              "earliest_time": "2022-01-01T00:00:00.00Z",
              "latest_time": "2022-07-07T06:00:00.00Z"
            }
          }
        }
        "#;
        let agg_schema: AggregateTSMSchema = json.try_into().unwrap();
        let err = update_iox_catalog(
            &agg_schema,
            "iox-shared",
            Some("iox-shared"),
            Arc::clone(&catalog),
            connection,
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
        txn.topics()
            .create_or_get("iox-shared")
            .await
            .expect("topic created");
        let (connection, _join_handle, _requests) = create_test_shard_service(MapToShardResponse {
            shard_id: 0,
            shard_index: 0,
        })
        .await;

        // create namespace, table and columns for weather measurement
        let namespace = txn
            .namespaces()
            .create("1234_5678", None, TopicId::new(1), QueryPoolId::new(1))
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
              ],
              "earliest_time": "2022-01-01T00:00:00.00Z",
              "latest_time": "2022-07-07T06:00:00.00Z"
            }
          }
        }
        "#;
        let agg_schema: AggregateTSMSchema = json.try_into().unwrap();
        let err = update_iox_catalog(
            &agg_schema,
            "iox-shared",
            Some("iox-shared"),
            Arc::clone(&catalog),
            connection,
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
            .topics()
            .create_or_get("iox-shared")
            .await
            .expect("topic created");
        let (connection, _join_handle, _requests) = create_test_shard_service(MapToShardResponse {
            shard_id: 0,
            shard_index: 0,
        })
        .await;

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
              ],
              "earliest_time": "2022-01-01T00:00:00.00Z",
              "latest_time": "2022-07-07T06:00:00.00Z"
            }
          }
        }
        "#;
        let agg_schema: AggregateTSMSchema = json.try_into().unwrap();
        let err = update_iox_catalog(
            &agg_schema,
            "iox-shared",
            None,
            Arc::clone(&catalog),
            connection,
        )
        .await
        .expect_err("should fail namespace creation");
        assert_matches!(err, UpdateCatalogError::NamespaceCreationError(_));
    }

    #[tokio::test]
    async fn shard_lookup() {
        // init a test catalog stack
        let metrics = Arc::new(metric::Registry::default());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        catalog
            .repositories()
            .await
            .topics()
            .create_or_get("iox-shared")
            .await
            .expect("topic created");
        let (connection, _join_handle, requests) = create_test_shard_service(MapToShardResponse {
            shard_id: 0,
            shard_index: 0,
        })
        .await;

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
              ],
              "earliest_time": "2022-01-01T00:00:00.00Z",
              "latest_time": "2022-07-07T06:00:00.00Z"
            },
            "weather": {
              "tags": [
              ],
             "fields": [
                { "name": "temperature", "types": ["Integer"] }
              ],
              "earliest_time": "2022-01-01T00:00:00.00Z",
              "latest_time": "2022-07-07T06:00:00.00Z"
            }
          }
        }
        "#;
        let agg_schema: AggregateTSMSchema = json.try_into().unwrap();
        update_iox_catalog(
            &agg_schema,
            "iox-shared",
            Some("iox-shared"),
            Arc::clone(&catalog),
            connection,
        )
        .await
        .expect("schema update worked");
        // check that a request was made for the two shard lookups for the tables
        let requests = requests.read();
        assert_eq!(requests.len(), 2);
        let cpu_req = requests
            .iter()
            .find(|r| r.table_name == "cpu")
            .expect("cpu request missing from mock");
        assert_eq!(
            (cpu_req.namespace_name.as_str(), cpu_req.table_name.as_str()),
            ("1234_5678", "cpu"),
        );
        let weather_req = requests
            .iter()
            .find(|r| r.table_name == "weather")
            .expect("weather request missing from mock");
        assert_eq!(
            (
                weather_req.namespace_name.as_str(),
                weather_req.table_name.as_str()
            ),
            ("1234_5678", "weather"),
        );
    }

    #[tokio::test]
    async fn partition_keys_from_datetime_range_midday_to_midday() {
        let earliest_time = DateTime::parse_from_rfc3339("2022-10-30T12:00:00+00:00")
            .expect("rfc3339 date parsing failed");
        let latest_time = DateTime::parse_from_rfc3339("2022-11-01T12:00:00+00:00")
            .expect("rfc3339 date parsing failed");
        let keys = get_partition_keys_for_range(earliest_time, latest_time)
            .expect("error creating partition keys in test");
        assert_eq!(
            keys,
            vec![
                "2022-10-30".into(),
                "2022-10-31".into(),
                "2022-11-01".into()
            ]
        );
    }

    #[tokio::test]
    async fn partition_keys_from_datetime_range_within_a_day() {
        let earliest_time = DateTime::parse_from_rfc3339("2022-10-31T00:00:00+00:00")
            .expect("rfc3339 date parsing failed");
        let latest_time = DateTime::parse_from_rfc3339("2022-10-31T12:00:00+00:00")
            .expect("rfc3339 date parsing failed");
        let keys = get_partition_keys_for_range(earliest_time, latest_time)
            .expect("error creating partition keys in test");
        assert_eq!(keys, vec!["2022-10-31".into(),]);
    }

    #[tokio::test]
    async fn partition_keys_from_datetime_range_equal() {
        let earliest_time = DateTime::parse_from_rfc3339("2022-10-31T23:59:59+00:00")
            .expect("rfc3339 date parsing failed");
        let latest_time = DateTime::parse_from_rfc3339("2022-10-31T23:59:59+00:00")
            .expect("rfc3339 date parsing failed");
        let keys = get_partition_keys_for_range(earliest_time, latest_time)
            .expect("error creating partition keys in test");
        assert_eq!(keys, vec!["2022-10-31".into(),]);
    }

    #[tokio::test]
    async fn partition_keys_from_datetime_range_across_day_boundary() {
        let earliest_time = DateTime::parse_from_rfc3339("2022-10-31T23:59:59+00:00")
            .expect("rfc3339 date parsing failed");
        let latest_time = DateTime::parse_from_rfc3339("2022-11-01T00:00:01+00:00")
            .expect("rfc3339 date parsing failed");
        let keys = get_partition_keys_for_range(earliest_time, latest_time)
            .expect("error creating partition keys in test");
        assert_eq!(keys, vec!["2022-10-31".into(), "2022-11-01".into()]);
    }

    #[tokio::test]
    async fn compute_primary_key_not_sorted() {
        let m = AggregateTSMMeasurement {
            tags: HashMap::from([
                (
                    "host".to_string(),
                    AggregateTSMTag {
                        name: "host".to_string(),
                        values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                    },
                ),
                (
                    "arch".to_string(),
                    AggregateTSMTag {
                        name: "arch".to_string(),
                        values: HashSet::from([
                            "amd64".to_string(),
                            "x86".to_string(),
                            "i386".to_string(),
                        ]),
                    },
                ),
                // add something sorted after time lexicographically to tickle a bug as i write a
                // failing test
                (
                    "zazzle".to_string(),
                    AggregateTSMTag {
                        name: "zazzle".to_string(),
                        values: HashSet::from(["true".to_string()]),
                    },
                ),
            ]),
            fields: HashMap::from([(
                "usage".to_string(),
                AggregateTSMField {
                    name: "usage".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        let pk = compute_measurement_primary_key(&m);
        assert_eq!(pk, vec!["arch", "host", "zazzle", "time"]);
    }

    #[tokio::test]
    async fn compute_primary_key_already_sorted() {
        let m = AggregateTSMMeasurement {
            tags: HashMap::from([
                (
                    "arch".to_string(),
                    AggregateTSMTag {
                        name: "arch".to_string(),
                        values: HashSet::from([
                            "amd64".to_string(),
                            "x86".to_string(),
                            "i386".to_string(),
                        ]),
                    },
                ),
                (
                    "host".to_string(),
                    AggregateTSMTag {
                        name: "host".to_string(),
                        values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                    },
                ),
            ]),
            fields: HashMap::from([(
                "usage".to_string(),
                AggregateTSMField {
                    name: "usage".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        let pk = compute_measurement_primary_key(&m);
        assert_eq!(pk, vec!["arch", "host", "time"]);
    }

    #[tokio::test]
    async fn compute_sort_key_not_sorted() {
        let m = AggregateTSMMeasurement {
            tags: HashMap::from([
                (
                    "host".to_string(),
                    AggregateTSMTag {
                        name: "host".to_string(),
                        values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                    },
                ),
                (
                    "arch".to_string(),
                    AggregateTSMTag {
                        name: "arch".to_string(),
                        values: HashSet::from([
                            "amd64".to_string(),
                            "x86".to_string(),
                            "i386".to_string(),
                        ]),
                    },
                ),
            ]),
            fields: HashMap::from([(
                "usage".to_string(),
                AggregateTSMField {
                    name: "usage".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        let sk = compute_measurement_sort_key(&m);
        let sk = sk.to_columns().collect::<Vec<_>>();
        assert_eq!(sk, vec!["host", "arch", "time"]);
    }

    #[tokio::test]
    async fn compute_sort_key_already_sorted() {
        let m = AggregateTSMMeasurement {
            tags: HashMap::from([
                (
                    "arch".to_string(),
                    AggregateTSMTag {
                        name: "arch".to_string(),
                        values: HashSet::from([
                            "amd64".to_string(),
                            "x86".to_string(),
                            "i386".to_string(),
                        ]),
                    },
                ),
                (
                    "host".to_string(),
                    AggregateTSMTag {
                        name: "host".to_string(),
                        values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                    },
                ),
            ]),
            fields: HashMap::from([(
                "usage".to_string(),
                AggregateTSMField {
                    name: "usage".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        let sk = compute_measurement_sort_key(&m);
        let sk = sk.to_columns().collect::<Vec<_>>();
        assert_eq!(sk, vec!["host", "arch", "time"]);
    }

    #[tokio::test]
    async fn compute_sort_key_already_sorted_more_tags() {
        let m = AggregateTSMMeasurement {
            tags: HashMap::from([
                (
                    "arch".to_string(),
                    AggregateTSMTag {
                        name: "arch".to_string(),
                        values: HashSet::from([
                            "amd64".to_string(),
                            "x86".to_string(),
                            "i386".to_string(),
                        ]),
                    },
                ),
                (
                    "host".to_string(),
                    AggregateTSMTag {
                        name: "host".to_string(),
                        values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                    },
                ),
                (
                    "os".to_string(),
                    AggregateTSMTag {
                        name: "os".to_string(),
                        values: HashSet::from([
                            "linux".to_string(),
                            "windows".to_string(),
                            "osx".to_string(),
                            "freebsd".to_string(),
                        ]),
                    },
                ),
            ]),
            fields: HashMap::from([(
                "usage".to_string(),
                AggregateTSMField {
                    name: "usage".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        let sk = compute_measurement_sort_key(&m);
        let sk = sk.to_columns().collect::<Vec<_>>();
        assert_eq!(sk, vec!["host", "arch", "os", "time"]);
    }

    #[tokio::test]
    async fn get_sort_key_was_empty() {
        let m = AggregateTSMMeasurement {
            tags: HashMap::from([
                (
                    "arch".to_string(),
                    AggregateTSMTag {
                        name: "arch".to_string(),
                        values: HashSet::from([
                            "amd64".to_string(),
                            "x86".to_string(),
                            "i386".to_string(),
                        ]),
                    },
                ),
                (
                    "host".to_string(),
                    AggregateTSMTag {
                        name: "host".to_string(),
                        values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                    },
                ),
            ]),
            fields: HashMap::from([(
                "usage".to_string(),
                AggregateTSMField {
                    name: "usage".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        let partition = Partition {
            id: PartitionId::new(1),
            shard_id: ShardId::new(1),
            table_id: TableId::new(1),
            persisted_sequence_number: None,
            partition_key: PartitionKey::from("2022-06-21"),
            sort_key: Vec::new(),
        };
        let sort_key = get_sort_key(&partition, &m).1.unwrap();
        let sort_key = sort_key.to_columns().collect::<Vec<_>>();
        // ensure sort key is updated with the computed one
        assert_eq!(sort_key, vec!["host", "arch", "time"]);
    }

    #[tokio::test]
    async fn get_sort_key_no_change() {
        let m = AggregateTSMMeasurement {
            tags: HashMap::from([
                (
                    "arch".to_string(),
                    AggregateTSMTag {
                        name: "arch".to_string(),
                        values: HashSet::from([
                            "amd64".to_string(),
                            "x86".to_string(),
                            "i386".to_string(),
                        ]),
                    },
                ),
                (
                    "host".to_string(),
                    AggregateTSMTag {
                        name: "host".to_string(),
                        values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                    },
                ),
            ]),
            fields: HashMap::from([(
                "usage".to_string(),
                AggregateTSMField {
                    name: "usage".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        let partition = Partition {
            id: PartitionId::new(1),
            shard_id: ShardId::new(1),
            table_id: TableId::new(1),
            persisted_sequence_number: None,
            partition_key: PartitionKey::from("2022-06-21"),
            // N.B. sort key is already what it will computed to; here we're testing the `adjust_sort_key_columns` code path
            sort_key: vec!["host".to_string(), "arch".to_string(), "time".to_string()],
        };
        // ensure sort key is unchanged
        let _maybe_updated_sk = get_sort_key(&partition, &m).1;
        assert_matches!(None::<SortKey>, _maybe_updated_sk);
    }

    #[tokio::test]
    async fn get_sort_key_with_changes_1() {
        let m = AggregateTSMMeasurement {
            tags: HashMap::from([
                (
                    "arch".to_string(),
                    AggregateTSMTag {
                        name: "arch".to_string(),
                        values: HashSet::from([
                            "amd64".to_string(),
                            "x86".to_string(),
                            "i386".to_string(),
                        ]),
                    },
                ),
                (
                    "host".to_string(),
                    AggregateTSMTag {
                        name: "host".to_string(),
                        values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                    },
                ),
            ]),
            fields: HashMap::from([(
                "usage".to_string(),
                AggregateTSMField {
                    name: "usage".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        let partition = Partition {
            id: PartitionId::new(1),
            shard_id: ShardId::new(1),
            table_id: TableId::new(1),
            persisted_sequence_number: None,
            partition_key: PartitionKey::from("2022-06-21"),
            // N.B. is missing host so will need updating
            sort_key: vec!["arch".to_string(), "time".to_string()],
        };
        let sort_key = get_sort_key(&partition, &m).1.unwrap();
        let sort_key = sort_key.to_columns().collect::<Vec<_>>();
        // ensure sort key is updated; host would have been sorted first but it got added later so
        // it won't be
        assert_eq!(sort_key, vec!["arch", "host", "time"]);
    }

    #[tokio::test]
    async fn get_sort_key_with_changes_2() {
        let m = AggregateTSMMeasurement {
            tags: HashMap::from([
                (
                    "arch".to_string(),
                    AggregateTSMTag {
                        name: "arch".to_string(),
                        values: HashSet::from([
                            "amd64".to_string(),
                            "x86".to_string(),
                            "i386".to_string(),
                        ]),
                    },
                ),
                (
                    "host".to_string(),
                    AggregateTSMTag {
                        name: "host".to_string(),
                        values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                    },
                ),
            ]),
            fields: HashMap::from([(
                "usage".to_string(),
                AggregateTSMField {
                    name: "usage".to_string(),
                    types: HashSet::from(["Float".to_string()]),
                },
            )]),
            earliest_time: DateTime::parse_from_rfc3339("2022-01-01T00:00:00+00:00").unwrap(),
            latest_time: DateTime::parse_from_rfc3339("2022-07-07T06:00:00+00:00").unwrap(),
        };
        let partition = Partition {
            id: PartitionId::new(1),
            shard_id: ShardId::new(1),
            table_id: TableId::new(1),
            persisted_sequence_number: None,
            partition_key: PartitionKey::from("2022-06-21"),
            // N.B. is missing arch so will need updating
            sort_key: vec!["host".to_string(), "time".to_string()],
        };
        let sort_key = get_sort_key(&partition, &m).1.unwrap();
        let sort_key = sort_key.to_columns().collect::<Vec<_>>();
        // ensure sort key is updated; new columns get appended after existing ones only
        assert_eq!(sort_key, vec!["host", "arch", "time"]);
    }
}
