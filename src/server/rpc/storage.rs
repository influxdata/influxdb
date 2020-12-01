//! This module contains implementations for the storage gRPC service
//! implemented in terms of the `query::Database` and
//! `query::DatabaseStore`

use std::{collections::HashMap, sync::Arc};

use generated_types::{
    i_ox_server::{IOx, IOxServer},
    storage_server::{Storage, StorageServer},
    CapabilitiesResponse, Capability, CreateBucketRequest, CreateBucketResponse,
    DeleteBucketRequest, DeleteBucketResponse, GetBucketsResponse, Int64ValuesResponse,
    MeasurementFieldsRequest, MeasurementFieldsResponse, MeasurementNamesRequest,
    MeasurementTagKeysRequest, MeasurementTagValuesRequest, Organization, Predicate,
    ReadFilterRequest, ReadGroupRequest, ReadResponse, ReadSeriesCardinalityRequest,
    ReadWindowAggregateRequest, StringValuesResponse, TagKeysRequest, TagValuesRequest,
    TestErrorRequest, TestErrorResponse, TimestampRange,
};

use data_types::error::ErrorLogger;

#[allow(unused_imports)]
// For some reason rust thinks these imports are unused, but then
// complains of unresolved imports if they are not imported.
use generated_types::{node, Node};
use query::group_by::GroupByAndAggregate;

use crate::server::rpc::expr::{self, AddRPCNode, Loggable, SpecialTagKeys};
use crate::server::rpc::input::GrpcInputs;

use query::{
    exec::{
        seriesset::{Error as SeriesSetError, SeriesSetItem},
        Executor as QueryExecutor,
    },
    org_and_bucket_to_database,
    predicate::PredicateBuilder,
    DatabaseStore, TSDatabase,
};

use snafu::{OptionExt, ResultExt, Snafu};

use tokio::{net::TcpListener, sync::mpsc};
use tonic::Status;
use tracing::{error, info, warn};

use super::data::{
    fieldlist_to_measurement_fields_response, series_set_item_to_read_response,
    tag_keys_to_byte_vecs,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("gRPC server error:  {}", source))]
    ServerError { source: tonic::transport::Error },

    #[snafu(display("Database not found: {}", db_name))]
    DatabaseNotFound { db_name: String },

    #[snafu(display("Error listing tables in database '{}': {}", db_name, source))]
    ListingTables {
        db_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Error listing columns in database '{}': {}", db_name, source))]
    ListingColumns {
        db_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Error listing fields in database '{}': {}", db_name, source))]
    ListingFields {
        db_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Error creating series plans for database '{}': {}", db_name, source))]
    PlanningFilteringSeries {
        db_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Error creating group plans for database '{}': {}", db_name, source))]
    PlanningGroupSeries {
        db_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Error running series plans for database '{}': {}", db_name, source))]
    FilteringSeries {
        db_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Error running grouping plans for database '{}': {}", db_name, source))]
    GroupingSeries {
        db_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Can not retrieve tag values for '{}' in database '{}': {}",
        tag_name,
        db_name,
        source
    ))]
    ListingTagValues {
        db_name: String,
        tag_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Error converting Predicate '{}: {}", rpc_predicate_string, source))]
    ConvertingPredicate {
        rpc_predicate_string: String,
        source: crate::server::rpc::expr::Error,
    },

    #[snafu(display("Error converting group type '{}':  {}", aggregate_string, source))]
    ConvertingReadGroupType {
        aggregate_string: String,
        source: crate::server::rpc::expr::Error,
    },

    #[snafu(display(
        "Error converting read_group aggregate '{}':  {}",
        aggregate_string,
        source
    ))]
    ConvertingReadGroupAggregate {
        aggregate_string: String,
        source: crate::server::rpc::expr::Error,
    },

    #[snafu(display(
        "Error converting read_aggregate_window aggregate definition '{}':  {}",
        aggregate_string,
        source
    ))]
    ConvertingWindowAggregate {
        aggregate_string: String,
        source: crate::server::rpc::expr::Error,
    },

    #[snafu(display("Computing series: {}", source))]
    ComputingSeriesSet { source: SeriesSetError },

    #[snafu(display("Computing groups series: {}", source))]
    ComputingGroupedSeriesSet { source: SeriesSetError },

    #[snafu(display("Converting time series into gRPC response:  {}", source))]
    ConvertingSeriesSet {
        source: crate::server::rpc::data::Error,
    },

    #[snafu(display("Converting field information series into gRPC response:  {}", source))]
    ConvertingFieldList {
        source: crate::server::rpc::data::Error,
    },

    #[snafu(display("Error sending results via channel:  {}", source))]
    SendingResults {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Operation not yet implemented:  {}", operation))]
    NotYetImplemented { operation: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for tonic::Status {
    /// Converts a result from the business logic into the appropriate tonic status
    fn from(err: Error) -> Self {
        error!("Error handling gRPC request: {}", err);
        err.to_status()
    }
}

impl Error {
    /// Converts a result from the business logic into the appropriate tonic status
    fn to_status(&self) -> tonic::Status {
        match &self {
            Self::ServerError { .. } => Status::internal(self.to_string()),
            Self::DatabaseNotFound { .. } => Status::not_found(self.to_string()),
            Self::ListingTables { .. } => Status::internal(self.to_string()),
            Self::ListingColumns { .. } => {
                // TODO: distinguish between input errors and internal errors
                Status::invalid_argument(self.to_string())
            }
            Self::ListingFields { .. } => {
                // TODO: distinguish between input errors and internal errors
                Status::invalid_argument(self.to_string())
            }
            Self::PlanningFilteringSeries { .. } => Status::invalid_argument(self.to_string()),
            Self::PlanningGroupSeries { .. } => Status::invalid_argument(self.to_string()),
            Self::FilteringSeries { .. } => Status::invalid_argument(self.to_string()),
            Self::GroupingSeries { .. } => Status::invalid_argument(self.to_string()),
            Self::ListingTagValues { .. } => Status::invalid_argument(self.to_string()),
            Self::ConvertingPredicate { .. } => Status::invalid_argument(self.to_string()),
            Self::ConvertingReadGroupAggregate { .. } => Status::invalid_argument(self.to_string()),
            Self::ConvertingReadGroupType { .. } => Status::invalid_argument(self.to_string()),
            Self::ConvertingWindowAggregate { .. } => Status::invalid_argument(self.to_string()),
            Self::ComputingSeriesSet { .. } => Status::invalid_argument(self.to_string()),
            Self::ComputingGroupedSeriesSet { .. } => Status::invalid_argument(self.to_string()),
            Self::ConvertingSeriesSet { .. } => Status::invalid_argument(self.to_string()),
            Self::ConvertingFieldList { .. } => Status::invalid_argument(self.to_string()),
            Self::SendingResults { .. } => Status::internal(self.to_string()),
            Self::NotYetImplemented { .. } => Status::internal(self.to_string()),
        }
    }
}

#[derive(Debug)]
pub struct GrpcService<T: DatabaseStore> {
    db_store: Arc<T>,
    executor: Arc<QueryExecutor>,
}

impl<T> GrpcService<T>
where
    T: DatabaseStore + 'static,
{
    /// Create a new GrpcService connected to `db_store`
    pub fn new(db_store: Arc<T>, executor: Arc<QueryExecutor>) -> Self {
        Self { db_store, executor }
    }
}

#[tonic::async_trait]
/// Implements the protobuf defined IOx rpc service for a DatabaseStore
impl<T> IOx for GrpcService<T>
where
    T: DatabaseStore + 'static,
{
    // TODO: Do we want to keep this gRPC request?
    async fn create_bucket(
        &self,
        _req: tonic::Request<CreateBucketRequest>,
    ) -> Result<tonic::Response<CreateBucketResponse>, Status> {
        Err(Status::unimplemented("create_bucket"))
    }

    async fn delete_bucket(
        &self,
        _req: tonic::Request<DeleteBucketRequest>,
    ) -> Result<tonic::Response<DeleteBucketResponse>, Status> {
        Err(Status::unimplemented("delete_bucket"))
    }

    async fn get_buckets(
        &self,
        _req: tonic::Request<Organization>,
    ) -> Result<tonic::Response<GetBucketsResponse>, Status> {
        Err(Status::unimplemented("get_buckets"))
    }

    async fn test_error(
        &self,
        _req: tonic::Request<TestErrorRequest>,
    ) -> Result<tonic::Response<TestErrorResponse>, Status> {
        warn!("Got a test_error request. About to panic");
        panic!("This is a test panic");
    }
}

/// Implementes the protobuf defined Storage service for a DatabaseStore
#[tonic::async_trait]
impl<T> Storage for GrpcService<T>
where
    T: DatabaseStore + 'static,
{
    type ReadFilterStream = mpsc::Receiver<Result<ReadResponse, Status>>;

    async fn read_filter(
        &self,
        req: tonic::Request<ReadFilterRequest>,
    ) -> Result<tonic::Response<Self::ReadFilterStream>, Status> {
        let (tx, rx) = mpsc::channel(4);

        let read_filter_request = req.into_inner();

        let db_name = get_database_name(&read_filter_request)?;

        let ReadFilterRequest {
            read_source: _read_source,
            range,
            predicate,
        } = read_filter_request;

        info!(
            "read_filter for database {}, range: {:?}, predicate: {}",
            db_name,
            range,
            predicate.loggable()
        );

        read_filter_impl(
            tx.clone(),
            self.db_store.clone(),
            self.executor.clone(),
            db_name,
            range,
            predicate,
        )
        .await
        .map_err(|e| e.to_status())?;

        Ok(tonic::Response::new(rx))
    }

    type ReadGroupStream = mpsc::Receiver<Result<ReadResponse, Status>>;

    async fn read_group(
        &self,
        req: tonic::Request<ReadGroupRequest>,
    ) -> Result<tonic::Response<Self::ReadGroupStream>, Status> {
        let (tx, rx) = mpsc::channel(4);

        let read_group_request = req.into_inner();

        let db_name = get_database_name(&read_group_request)?;

        let ReadGroupRequest {
            read_source: _read_source,
            range,
            predicate,
            group_keys,
            group,
            aggregate,
            hints: _,
        } = read_group_request;

        info!(
            "read_group for database {}, range: {:?}, group_keys: {:?}, group: {:?}, aggregate: {:?}, predicate: {}",
            db_name, range, group_keys, group, aggregate,
              predicate.loggable()
        );

        warn!("read_group implementation not yet complete: https://github.com/influxdata/influxdb_iox/issues/448");

        let aggregate_string = format!(
            "aggregate: {:?}, group: {:?}, group_keys: {:?}",
            aggregate, group, group_keys
        );

        let group = expr::convert_group_type(group).context(ConvertingReadGroupType {
            aggregate_string: &aggregate_string,
        })?;

        let gby_agg = expr::make_read_group_aggregate(aggregate, group, group_keys)
            .context(ConvertingReadGroupAggregate { aggregate_string })?;

        query_group_impl(
            tx.clone(),
            self.db_store.clone(),
            self.executor.clone(),
            db_name,
            range,
            predicate,
            gby_agg,
        )
        .await
        .map_err(|e| e.to_status())?;

        Ok(tonic::Response::new(rx))
    }

    type ReadWindowAggregateStream = mpsc::Receiver<Result<ReadResponse, Status>>;

    async fn read_window_aggregate(
        &self,
        req: tonic::Request<ReadWindowAggregateRequest>,
    ) -> Result<tonic::Response<Self::ReadGroupStream>, Status> {
        let (tx, rx) = mpsc::channel(4);

        let read_window_aggregate_request = req.into_inner();

        let db_name = get_database_name(&read_window_aggregate_request)?;

        let ReadWindowAggregateRequest {
            read_source: _read_source,
            range,
            predicate,
            window_every,
            offset,
            aggregate,
            window,
        } = read_window_aggregate_request;

        info!(
            "read_window_aggregate for database {}, range: {:?}, window_every: {:?}, offset: {:?}, aggregate: {:?}, window: {:?}, predicate: {}",
            db_name, range, window_every, offset, aggregate, window,
              predicate.loggable()
        );

        let aggregate_string = format!(
            "aggregate: {:?}, window_every: {:?}, offset: {:?}, window: {:?}",
            aggregate, window_every, offset, window
        );

        let gby_agg = expr::make_read_window_aggregate(aggregate, window_every, offset, window)
            .context(ConvertingWindowAggregate { aggregate_string })?;

        query_group_impl(
            tx.clone(),
            self.db_store.clone(),
            self.executor.clone(),
            db_name,
            range,
            predicate,
            gby_agg,
        )
        .await
        .map_err(|e| e.to_status())?;

        Ok(tonic::Response::new(rx))
    }

    type TagKeysStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    async fn tag_keys(
        &self,
        req: tonic::Request<TagKeysRequest>,
    ) -> Result<tonic::Response<Self::TagKeysStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);

        let tag_keys_request = req.into_inner();

        let db_name = get_database_name(&tag_keys_request)?;

        let TagKeysRequest {
            tags_source: _tag_source,
            range,
            predicate,
        } = tag_keys_request;

        info!(
            "tag_keys for database {}, range: {:?}, predicate: {}",
            db_name,
            range,
            predicate.loggable()
        );

        let measurement = None;

        let response = tag_keys_impl(
            self.db_store.clone(),
            self.executor.clone(),
            db_name,
            measurement,
            range,
            predicate,
        )
        .await
        .map_err(|e| e.to_status());

        tx.send(response)
            .await
            .expect("sending tag_keys response to server");

        Ok(tonic::Response::new(rx))
    }

    type TagValuesStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    async fn tag_values(
        &self,
        req: tonic::Request<TagValuesRequest>,
    ) -> Result<tonic::Response<Self::TagValuesStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);

        let tag_values_request = req.into_inner();

        let db_name = get_database_name(&tag_values_request)?;

        let TagValuesRequest {
            tags_source: _tag_source,
            range,
            predicate,
            tag_key,
        } = tag_values_request;

        let measurement = None;

        // Special case a request for 'tag_key=_measurement" means to list all measurements
        let response = if tag_key.is_measurement() {
            info!(
                "tag_values with tag_key=[x00] for database {}, range: {:?}, predicate: {} --> returning measurement_names",
                db_name, range,
                    predicate.loggable()
            );

            if predicate.is_some() {
                unimplemented!("tag_value for a measurement, with general predicate");
            }

            measurement_name_impl(self.db_store.clone(), self.executor.clone(), db_name, range)
                .await
        } else {
            info!(
                "tag_values for database {}, range: {:?}, tag_key: {}, predicate: {}",
                db_name,
                range,
                tag_key,
                predicate.loggable()
            );

            tag_values_impl(
                self.db_store.clone(),
                self.executor.clone(),
                db_name,
                tag_key,
                measurement,
                range,
                predicate,
            )
            .await
        };

        let response = response.map_err(|e| e.to_status());

        tx.send(response)
            .await
            .expect("sending tag_values response to server");

        Ok(tonic::Response::new(rx))
    }

    type ReadSeriesCardinalityStream = mpsc::Receiver<Result<Int64ValuesResponse, Status>>;

    async fn read_series_cardinality(
        &self,
        _req: tonic::Request<ReadSeriesCardinalityRequest>,
    ) -> Result<tonic::Response<Self::ReadSeriesCardinalityStream>, Status> {
        unimplemented!("read_series_cardinality not yet implemented");
    }

    async fn capabilities(
        &self,
        _req: tonic::Request<()>,
    ) -> Result<tonic::Response<CapabilitiesResponse>, Status> {
        // Full list of go capabilities in
        // idpe/storage/read/capabilities.go (aka window aggregate /
        // pushdown)
        //

        // For now, hard code our list of support
        let caps = [(
            "WindowAggregate",
            vec![
                "Count", "Sum", // "First"
                // "Last",
                "Min", "Max", "Mean",
                // "Offset"
            ],
        )];

        // Turn it into the HashMap -> Capabiltity
        let caps = caps
            .iter()
            .map(|(cap_name, features)| {
                let features = features.iter().map(|f| f.to_string()).collect::<Vec<_>>();
                (cap_name.to_string(), Capability { features })
            })
            .collect::<HashMap<String, Capability>>();

        let caps = CapabilitiesResponse { caps };
        Ok(tonic::Response::new(caps))
    }

    type MeasurementNamesStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    async fn measurement_names(
        &self,
        req: tonic::Request<MeasurementNamesRequest>,
    ) -> Result<tonic::Response<Self::MeasurementNamesStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);

        let measurement_names_request = req.into_inner();

        let db_name = get_database_name(&measurement_names_request)?;

        let MeasurementNamesRequest {
            source: _source,
            range,
            predicate,
        } = measurement_names_request;

        if let Some(predicate) = predicate {
            return NotYetImplemented {
                operation: format!(
                    "measurement_names request with a predicate: {:?}",
                    predicate
                ),
            }
            .fail()
            .map_err(|e| e.to_status());
        }

        info!(
            "measurement_names for database {}, range: {:?}, predicate: {}",
            db_name,
            range,
            predicate.loggable()
        );

        let response =
            measurement_name_impl(self.db_store.clone(), self.executor.clone(), db_name, range)
                .await
                .map_err(|e| e.to_status());

        tx.send(response)
            .await
            .expect("sending measurement names response to server");

        Ok(tonic::Response::new(rx))
    }

    type MeasurementTagKeysStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    async fn measurement_tag_keys(
        &self,
        req: tonic::Request<MeasurementTagKeysRequest>,
    ) -> Result<tonic::Response<Self::MeasurementTagKeysStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);

        let measurement_tag_keys_request = req.into_inner();

        let db_name = get_database_name(&measurement_tag_keys_request)?;

        let MeasurementTagKeysRequest {
            source: _source,
            measurement,
            range,
            predicate,
        } = measurement_tag_keys_request;

        info!(
            "measurement_tag_keys for database {}, range: {:?}, measurement: {}, predicate: {}",
            db_name,
            range,
            measurement,
            predicate.loggable()
        );

        let measurement = Some(measurement);

        let response = tag_keys_impl(
            self.db_store.clone(),
            self.executor.clone(),
            db_name,
            measurement,
            range,
            predicate,
        )
        .await
        .map_err(|e| e.to_status());

        tx.send(response)
            .await
            .expect("sending measurement_tag_keys response to server");

        Ok(tonic::Response::new(rx))
    }

    type MeasurementTagValuesStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    async fn measurement_tag_values(
        &self,
        req: tonic::Request<MeasurementTagValuesRequest>,
    ) -> Result<tonic::Response<Self::MeasurementTagValuesStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);

        let measurement_tag_values_request = req.into_inner();

        let db_name = get_database_name(&measurement_tag_values_request)?;

        let MeasurementTagValuesRequest {
            source: _source,
            measurement,
            range,
            predicate,
            tag_key,
        } = measurement_tag_values_request;

        info!(
            "measurement_tag_values for database {}, range: {:?}, measurement: {}, tag_key: {}, predicate: {}",
            db_name, range, measurement, tag_key,
                    predicate.loggable()
        );

        let measurement = Some(measurement);

        let response = tag_values_impl(
            self.db_store.clone(),
            self.executor.clone(),
            db_name,
            tag_key,
            measurement,
            range,
            predicate,
        )
        .await
        .map_err(|e| e.to_status());

        tx.send(response)
            .await
            .expect("sending measurement_tag_values response to server");

        Ok(tonic::Response::new(rx))
    }

    type MeasurementFieldsStream = mpsc::Receiver<Result<MeasurementFieldsResponse, Status>>;

    async fn measurement_fields(
        &self,
        req: tonic::Request<MeasurementFieldsRequest>,
    ) -> Result<tonic::Response<Self::MeasurementFieldsStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);

        let measurement_fields_request = req.into_inner();

        let db_name = get_database_name(&measurement_fields_request)?;

        let MeasurementFieldsRequest {
            source: _source,
            measurement,
            range,
            predicate,
        } = measurement_fields_request;

        info!(
            "measurement_fields for database {}, range: {:?}, predicate: {}",
            db_name,
            range,
            predicate.loggable()
        );

        let measurement = measurement;

        let response = measurement_fields_impl(
            self.db_store.clone(),
            self.executor.clone(),
            db_name,
            measurement,
            range,
            predicate,
        )
        .await
        .map_err(|e| e.to_status());

        tx.send(response)
            .await
            .expect("sending measurement_fields response to server");

        Ok(tonic::Response::new(rx))
    }
}

trait SetRange {
    /// sets the timestamp range to range, if present
    fn set_range(self, range: Option<TimestampRange>) -> Self;
}
impl SetRange for PredicateBuilder {
    fn set_range(self, range: Option<TimestampRange>) -> Self {
        if let Some(range) = range {
            self.timestamp_range(range.start, range.end)
        } else {
            self
        }
    }
}

fn get_database_name(input: &impl GrpcInputs) -> Result<String, Status> {
    Ok(org_and_bucket_to_database(
        input.org_id()?,
        &input.bucket_name()?,
    ))
}

// The following code implements the business logic of the requests as
// methods that return Results with module specific Errors (and thus
// can use ?, etc). The trait implemententations then handle mapping
// to the appropriate tonic Status

/// Gathers all measurement names that have data in the specified
/// (optional) range
async fn measurement_name_impl<T>(
    db_store: Arc<T>,
    executor: Arc<QueryExecutor>,
    db_name: String,
    range: Option<TimestampRange>,
) -> Result<StringValuesResponse>
where
    T: DatabaseStore,
{
    let predicate = PredicateBuilder::default().set_range(range).build();

    let plan = db_store
        .db(&db_name)
        .await
        .context(DatabaseNotFound { db_name: &db_name })?
        .table_names(predicate)
        .await
        .map_err(|e| Error::ListingTables {
            db_name: db_name.clone(),
            source: Box::new(e),
        })?;

    let table_names = executor
        .to_string_set(plan)
        .await
        .map_err(|e| Error::ListingTables {
            db_name: db_name.clone(),
            source: Box::new(e),
        })?;

    // Map the resulting collection of Strings into a Vec<Vec<u8>>for return
    let values = table_names
        .iter()
        .map(|name| name.bytes().collect())
        .collect::<Vec<_>>();

    Ok(StringValuesResponse { values })
}

/// Return tag keys with optional measurement, timestamp and arbitratry predicates
async fn tag_keys_impl<T>(
    db_store: Arc<T>,
    executor: Arc<QueryExecutor>,
    db_name: String,
    measurement: Option<String>,
    range: Option<TimestampRange>,
    rpc_predicate: Option<Predicate>,
) -> Result<StringValuesResponse>
where
    T: DatabaseStore,
{
    let rpc_predicate_string = format!("{:?}", rpc_predicate);

    let predicate = PredicateBuilder::default()
        .set_range(range)
        .table_option(measurement)
        .rpc_predicate(rpc_predicate)
        .context(ConvertingPredicate {
            rpc_predicate_string,
        })?
        .build();

    let db = db_store
        .db(&db_name)
        .await
        .context(DatabaseNotFound { db_name: &db_name })?;

    let tag_key_plan = db
        .tag_column_names(predicate)
        .await
        .map_err(|e| Error::ListingColumns {
            db_name: db_name.clone(),
            source: Box::new(e),
        })?;

    let tag_keys =
        executor
            .to_string_set(tag_key_plan)
            .await
            .map_err(|e| Error::ListingColumns {
                db_name: db_name.clone(),
                source: Box::new(e),
            })?;

    // Map the resulting collection of Strings into a Vec<Vec<u8>>for return
    let values = tag_keys_to_byte_vecs(tag_keys);

    Ok(StringValuesResponse { values })
}

/// Return tag values for tag_name, with optional measurement, timestamp and arbitratry predicates
async fn tag_values_impl<T>(
    db_store: Arc<T>,
    executor: Arc<QueryExecutor>,
    db_name: String,
    tag_name: String,
    measurement: Option<String>,
    range: Option<TimestampRange>,
    rpc_predicate: Option<Predicate>,
) -> Result<StringValuesResponse>
where
    T: DatabaseStore,
{
    let rpc_predicate_string = format!("{:?}", rpc_predicate);

    let predicate = PredicateBuilder::default()
        .set_range(range)
        .table_option(measurement)
        .rpc_predicate(rpc_predicate)
        .context(ConvertingPredicate {
            rpc_predicate_string,
        })?
        .build();

    let db = db_store
        .db(&db_name)
        .await
        .context(DatabaseNotFound { db_name: &db_name })?;

    let tag_value_plan =
        db.column_values(&tag_name, predicate)
            .await
            .map_err(|e| Error::ListingTagValues {
                db_name: db_name.clone(),
                tag_name: tag_name.clone(),
                source: Box::new(e),
            })?;

    let tag_values =
        executor
            .to_string_set(tag_value_plan)
            .await
            .map_err(|e| Error::ListingTagValues {
                db_name: db_name.clone(),
                tag_name: tag_name.clone(),
                source: Box::new(e),
            })?;

    // Map the resulting collection of Strings into a Vec<Vec<u8>>for return
    let values = tag_values
        .iter()
        .map(|name| name.bytes().collect())
        .collect::<Vec<_>>();

    Ok(StringValuesResponse { values })
}

/// Launch async tasks that send the result of executing read_filter to `tx`
async fn read_filter_impl<T>(
    tx: mpsc::Sender<Result<ReadResponse, Status>>,
    db_store: Arc<T>,
    executor: Arc<QueryExecutor>,
    db_name: String,
    range: Option<TimestampRange>,
    rpc_predicate: Option<Predicate>,
) -> Result<()>
where
    T: DatabaseStore,
{
    let rpc_predicate_string = format!("{:?}", rpc_predicate);

    let predicate = PredicateBuilder::default()
        .set_range(range)
        .rpc_predicate(rpc_predicate)
        .context(ConvertingPredicate {
            rpc_predicate_string,
        })?
        .build();

    let db = db_store
        .db(&db_name)
        .await
        .context(DatabaseNotFound { db_name: &db_name })?;

    let series_plan =
        db.query_series(predicate)
            .await
            .map_err(|e| Error::PlanningFilteringSeries {
                db_name: db_name.clone(),
                source: Box::new(e),
            })?;

    // Spawn task to convert between series sets and the gRPC results
    // and to run the actual plans (so we can return a result to the
    // client before we start sending result)
    let (tx_series, rx_series) = mpsc::channel(4);
    tokio::spawn(async move {
        convert_series_set(rx_series, tx)
            .await
            .log_if_error("Converting series set")
    });

    // fire up the plans and start the pipeline flowing
    tokio::spawn(async move {
        executor
            .to_series_set(series_plan, tx_series)
            .await
            .map_err(|e| Error::FilteringSeries {
                db_name: db_name.clone(),
                source: Box::new(e),
            })
            .log_if_error("Running series set plan")
    });

    Ok(())
}

/// Receives SeriesSets from rx, converts them to ReadResponse and
/// and sends them to tx
async fn convert_series_set(
    mut rx: mpsc::Receiver<Result<SeriesSetItem, SeriesSetError>>,
    mut tx: mpsc::Sender<Result<ReadResponse, Status>>,
) -> Result<()> {
    while let Some(series_set) = rx.recv().await {
        let response = series_set
            .context(ComputingSeriesSet)
            .and_then(|series_set| {
                series_set_item_to_read_response(series_set).context(ConvertingSeriesSet)
            })
            .map_err(|e| Status::internal(e.to_string()));

        tx.send(response)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            .context(SendingResults)?
    }
    Ok(())
}

/// Launch async tasks that send the result of executing read_group to `tx`
async fn query_group_impl<T>(
    tx: mpsc::Sender<Result<ReadResponse, Status>>,
    db_store: Arc<T>,
    executor: Arc<QueryExecutor>,
    db_name: String,
    range: Option<TimestampRange>,
    rpc_predicate: Option<Predicate>,
    gby_agg: GroupByAndAggregate,
) -> Result<()>
where
    T: DatabaseStore,
{
    let rpc_predicate_string = format!("{:?}", rpc_predicate);

    let predicate = PredicateBuilder::default()
        .set_range(range)
        .rpc_predicate(rpc_predicate)
        .context(ConvertingPredicate {
            rpc_predicate_string,
        })?
        .build();

    let db = db_store
        .db(&db_name)
        .await
        .context(DatabaseNotFound { db_name: &db_name })?;

    let grouped_series_set_plan =
        db.query_groups(predicate, gby_agg)
            .await
            .map_err(|e| Error::PlanningFilteringSeries {
                db_name: db_name.clone(),
                source: Box::new(e),
            })?;

    // Spawn task to convert between series sets and the gRPC results
    // and to run the actual plans (so we can return a result to the
    // client before we start sending result)
    let (tx_series, rx_series) = mpsc::channel(4);
    tokio::spawn(async move {
        convert_series_set(rx_series, tx)
            .await
            .log_if_error("Converting grouped series set")
    });

    // fire up the plans and start the pipeline flowing
    tokio::spawn(async move {
        executor
            .to_series_set(grouped_series_set_plan, tx_series)
            .await
            .map_err(|e| Error::GroupingSeries {
                db_name: db_name.clone(),
                source: Box::new(e),
            })
            .log_if_error("Running Grouped SeriesSet Plan")
    });

    Ok(())
}

/// Return fields with optional measurement, timestamp and arbitratry predicates
async fn measurement_fields_impl<T>(
    db_store: Arc<T>,
    executor: Arc<QueryExecutor>,
    db_name: String,
    measurement: String,
    range: Option<TimestampRange>,
    rpc_predicate: Option<Predicate>,
) -> Result<MeasurementFieldsResponse>
where
    T: DatabaseStore,
{
    let rpc_predicate_string = format!("{:?}", rpc_predicate);

    let predicate = PredicateBuilder::default()
        .set_range(range)
        .table_option(Some(measurement))
        .rpc_predicate(rpc_predicate)
        .context(ConvertingPredicate {
            rpc_predicate_string,
        })?
        .build();

    let db = db_store
        .db(&db_name)
        .await
        .context(DatabaseNotFound { db_name: &db_name })?;

    let fieldlist_plan =
        db.field_column_names(predicate)
            .await
            .map_err(|e| Error::ListingFields {
                db_name: db_name.clone(),
                source: Box::new(e),
            })?;

    let fieldlist =
        executor
            .to_fieldlist(fieldlist_plan)
            .await
            .map_err(|e| Error::ListingFields {
                db_name: db_name.clone(),
                source: Box::new(e),
            })?;

    // And convert the result
    fieldlist_to_measurement_fields_response(fieldlist).context(ConvertingFieldList)
}

/// Instantiate a server listening on the specified address
/// implementing the IOx and Storage gRPC interfaces, the
/// underlying hyper server instance. Resolves when the server has
/// shutdown.
pub async fn make_server<T>(
    socket: TcpListener,
    storage: Arc<T>,
    executor: Arc<QueryExecutor>,
) -> Result<()>
where
    T: DatabaseStore + 'static,
{
    tonic::transport::Server::builder()
        .add_service(IOxServer::new(GrpcService::new(
            storage.clone(),
            executor.clone(),
        )))
        .add_service(StorageServer::new(GrpcService::new(
            storage.clone(),
            executor.clone(),
        )))
        .serve_with_incoming(socket)
        .await
        .context(ServerError {})
        .log_if_error("Running Tonic Server")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::panic::SendPanicsToTracing;
    use arrow_deps::arrow::datatypes::DataType;
    use query::{
        exec::fieldlist::{Field, FieldList},
        exec::FieldListPlan,
        exec::SeriesSetPlans,
        group_by::{Aggregate as QueryAggregate, WindowDuration as QueryWindowDuration},
        id::Id,
        test::ColumnNamesRequest,
        test::FieldColumnsRequest,
        test::QueryGroupsRequest,
        test::TestDatabaseStore,
        test::{ColumnValuesRequest, QuerySeriesRequest},
    };
    use std::{
        convert::TryFrom,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::Duration,
    };
    use test_helpers::tracing::TracingCapture;
    use tonic::Code;

    use futures::prelude::*;

    use generated_types::{
        aggregate::AggregateType, i_ox_client, read_response::frame, storage_client,
        Aggregate as RPCAggregate, Duration as RPCDuration, ReadSource, Window as RPCWindow,
    };

    use prost::Message;

    type IOxClient = i_ox_client::IOxClient<tonic::transport::Channel>;
    type StorageClient = storage_client::StorageClient<tonic::transport::Channel>;

    #[tokio::test]
    async fn test_influxdb_iox_rpc() -> Result<()> {
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let org = Organization {
            id: 1337,
            name: "my non-existent-org".into(),
            buckets: Vec::new(),
        };

        // Test response from influxdb_iox server
        let res = fixture.iox_client.get_buckets(org).await;

        match res {
            Err(e) => {
                assert_eq!(e.code(), Code::Unimplemented);
                assert_eq!(e.message(), "get_buckets");
            }
            Ok(buckets) => {
                panic!("Unexpected iox_client success: {:?}", buckets);
            }
        };

        Ok(())
    }

    fn to_str_vec(s: &[&str]) -> Vec<String> {
        s.iter().map(|s| s.to_string()).collect()
    }

    #[tokio::test]
    async fn test_storage_rpc_capabilities() -> Result<(), tonic::Status> {
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        // Test response from storage server
        let mut expected_capabilities: HashMap<String, Vec<String>> = HashMap::new();
        expected_capabilities.insert(
            "WindowAggregate".into(),
            to_str_vec(&["Count", "Sum", "Min", "Max", "Mean"]),
        );

        assert_eq!(
            expected_capabilities,
            fixture.storage_client.capabilities().await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_storage_rpc_measurement_names() -> Result<(), tonic::Status> {
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let lp_data = "h2o,state=CA temp=50.4 100\n\
                       o2,state=MA temp=50.4 200";
        fixture
            .test_storage
            .add_lp_string(&db_info.db_name, lp_data)
            .await;

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        // --- No timestamps
        let request = MeasurementNamesRequest {
            source: source.clone(),
            range: None,
            predicate: None,
        };

        let actual_measurements = fixture.storage_client.measurement_names(request).await?;
        let expected_measurements = to_string_vec(&["h2o", "o2"]);
        assert_eq!(actual_measurements, expected_measurements);

        // --- Timestamp range
        let range = TimestampRange {
            start: 150,
            end: 200,
        };
        let request = MeasurementNamesRequest {
            source,
            range: Some(range),
            predicate: None,
        };

        let actual_measurements = fixture.storage_client.measurement_names(request).await?;
        let expected_measurements = to_string_vec(&["o2"]);
        assert_eq!(actual_measurements, expected_measurements);

        Ok(())
    }

    /// test the plumbing of the RPC layer for tag_keys -- specifically that
    /// the right parameters are passed into the Database interface
    /// and that the returned values are sent back via gRPC.
    #[tokio::test]
    async fn test_storage_rpc_tag_keys() -> Result<(), tonic::Status> {
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let test_db = fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .expect("creating test database");

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        let tag_keys = vec!["k1", "k2", "k3", "k4"];
        let request = TagKeysRequest {
            tags_source: source.clone(),
            range: make_timestamp_range(150, 200),
            predicate: make_state_ma_predicate(),
        };

        let expected_request =  ColumnNamesRequest {
            predicate: "Predicate { exprs: [#state Eq Utf8(\"MA\")] range: TimestampRange { start: 150, end: 200 }}".into()
        };

        test_db.set_column_names(to_string_vec(&tag_keys)).await;

        let actual_tag_keys = fixture.storage_client.tag_keys(request).await?;
        let mut expected_tag_keys = vec!["_field", "_measurement"];
        expected_tag_keys.extend(tag_keys.iter());

        assert_eq!(
            actual_tag_keys, expected_tag_keys,
            "unexpected tag keys while getting column names"
        );
        assert_eq!(
            test_db.get_column_names_request().await,
            Some(expected_request),
            "unexpected request while getting column names"
        );

        // ---
        // test error
        // ---
        let request = TagKeysRequest {
            tags_source: source.clone(),
            range: None,
            predicate: None,
        };

        // Note we don't set the column_names on the test database, so we expect an error
        let response = fixture.storage_client.tag_keys(request).await;
        assert!(response.is_err());
        let response_string = format!("{:?}", response);
        let expected_error = "No saved column_names in TestDatabase";
        assert!(
            response_string.contains(expected_error),
            "'{}' did not contain expected content '{}'",
            response_string,
            expected_error
        );

        let expected_request = Some(ColumnNamesRequest {
            predicate: "Predicate {}".into(),
        });
        assert_eq!(test_db.get_column_names_request().await, expected_request);

        Ok(())
    }

    /// test the plumbing of the RPC layer for measurement_tag_keys-- specifically that
    /// the right parameters are passed into the Database interface
    /// and that the returned values are sent back via gRPC.
    #[tokio::test]
    async fn test_storage_rpc_measurement_tag_keys() -> Result<(), tonic::Status> {
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let test_db = fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .expect("creating test database");

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        #[derive(Debug)]
        struct TestCase<'a> {
            /// The tag keys to load into the database
            tag_keys: Vec<&'a str>,
            request: MeasurementTagKeysRequest,
            expected_request: ColumnNamesRequest,
        }

        // ---
        // Timestamp + Predicate
        // ---
        let tag_keys = vec!["k1", "k2", "k3", "k4"];
        let request = MeasurementTagKeysRequest {
            measurement: "m4".into(),
            source: source.clone(),
            range: make_timestamp_range(150, 200),
            predicate: make_state_ma_predicate(),
        };

        let expected_request =  ColumnNamesRequest {
            predicate: "Predicate { table_names: m4 exprs: [#state Eq Utf8(\"MA\")] range: TimestampRange { start: 150, end: 200 }}".into()
        };

        test_db.set_column_names(to_string_vec(&tag_keys)).await;

        let actual_tag_keys = fixture.storage_client.measurement_tag_keys(request).await?;

        let mut expected_tag_keys = vec!["_field", "_measurement"];
        expected_tag_keys.extend(tag_keys.iter());

        assert_eq!(
            actual_tag_keys, expected_tag_keys,
            "unexpected tag keys while getting column names"
        );

        assert_eq!(
            test_db.get_column_names_request().await,
            Some(expected_request),
            "unexpected request while getting column names"
        );

        // ---
        // test error
        // ---
        let request = MeasurementTagKeysRequest {
            measurement: "m5".into(),
            source: source.clone(),
            range: None,
            predicate: None,
        };

        // Note we don't set the column_names on the test database, so we expect an error
        let response = fixture.storage_client.measurement_tag_keys(request).await;
        assert!(response.is_err());
        let response_string = format!("{:?}", response);
        let expected_error = "No saved column_names in TestDatabase";
        assert!(
            response_string.contains(expected_error),
            "'{}' did not contain expected content '{}'",
            response_string,
            expected_error
        );

        let expected_request = Some(ColumnNamesRequest {
            predicate: "Predicate { table_names: m5}".into(),
        });
        assert_eq!(test_db.get_column_names_request().await, expected_request);

        Ok(())
    }

    /// test the plumbing of the RPC layer for tag_keys -- specifically that
    /// the right parameters are passed into the Database interface
    /// and that the returned values are sent back via gRPC.
    #[tokio::test]
    async fn test_storage_rpc_tag_values() -> Result<(), tonic::Status> {
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let test_db = fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .expect("creating test database");

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        let tag_values = vec!["k1", "k2", "k3", "k4"];
        let request = TagValuesRequest {
            tags_source: source.clone(),
            range: make_timestamp_range(150, 200),
            predicate: make_state_ma_predicate(),
            tag_key: "the_tag_key".into(),
        };

        let expected_request = ColumnValuesRequest {
            predicate: "Predicate { exprs: [#state Eq Utf8(\"MA\")] range: TimestampRange { start: 150, end: 200 }}".into(),
            column_name: "the_tag_key".into(),
        };

        test_db.set_column_values(to_string_vec(&tag_values)).await;

        let actual_tag_values = fixture.storage_client.tag_values(request).await?;
        assert_eq!(
            actual_tag_values, tag_values,
            "unexpected tag values while getting tag values"
        );
        assert_eq!(
            test_db.get_column_values_request().await,
            Some(expected_request),
            "unexpected request while getting tag values"
        );

        // ---
        // test tag_key = _measurement means listing all measurement names
        // ---
        let request = TagValuesRequest {
            tags_source: source.clone(),
            range: make_timestamp_range(1000, 1500),
            predicate: None,
            tag_key: "\x00".into(),
        };

        let lp_data = "h2o,state=CA temp=50.4 1000\n\
                       o2,state=MA temp=50.4 2000";
        fixture
            .test_storage
            .add_lp_string(&db_info.db_name, lp_data)
            .await;

        let tag_values = vec!["h2o"];
        let actual_tag_values = fixture.storage_client.tag_values(request).await?;
        assert_eq!(
            actual_tag_values, tag_values,
            "unexpected tag values while getting tag values for measurement names"
        );

        // ---
        // test error
        // ---
        let request = TagValuesRequest {
            tags_source: source.clone(),
            range: None,
            predicate: None,
            tag_key: "the_tag_key".into(),
        };

        // Note we don't set the column_names on the test database, so we expect an error
        let response = fixture.storage_client.tag_values(request).await;
        assert!(response.is_err());
        let response_string = format!("{:?}", response);
        let expected_error = "No saved column_values in TestDatabase";
        assert!(
            response_string.contains(expected_error),
            "'{}' did not contain expected content '{}'",
            response_string,
            expected_error
        );

        let expected_request = Some(ColumnValuesRequest {
            predicate: "Predicate {}".into(),
            column_name: "the_tag_key".into(),
        });
        assert_eq!(test_db.get_column_values_request().await, expected_request);

        Ok(())
    }

    /// test the plumbing of the RPC layer for measurement_tag_values-- specifically that
    /// the right parameters are passed into the Database interface
    /// and that the returned values are sent back via gRPC.
    #[tokio::test]
    async fn test_storage_rpc_measurement_tag_values() -> Result<(), tonic::Status> {
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let test_db = fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .expect("creating test database");

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        let tag_values = vec!["k1", "k2", "k3", "k4"];
        let request = MeasurementTagValuesRequest {
            measurement: "m4".into(),
            source: source.clone(),
            range: make_timestamp_range(150, 200),
            predicate: make_state_ma_predicate(),
            tag_key: "the_tag_key".into(),
        };

        let expected_request = ColumnValuesRequest {
            predicate: "Predicate { table_names: m4 exprs: [#state Eq Utf8(\"MA\")] range: TimestampRange { start: 150, end: 200 }}".into(),
            column_name: "the_tag_key".into(),
        };

        test_db.set_column_values(to_string_vec(&tag_values)).await;

        let actual_tag_values = fixture
            .storage_client
            .measurement_tag_values(request)
            .await?;
        assert_eq!(
            actual_tag_values, tag_values,
            "unexpected tag values while getting tag values",
        );

        assert_eq!(
            test_db.get_column_values_request().await,
            Some(expected_request),
            "unexpected request while getting tag values",
        );

        // ---
        // test error
        // ---
        let request = MeasurementTagValuesRequest {
            measurement: "m5".into(),
            source: source.clone(),
            range: None,
            predicate: None,
            tag_key: "the_tag_key".into(),
        };

        // Note we don't set the column_names on the test database, so we expect an error
        let response = fixture.storage_client.measurement_tag_values(request).await;
        assert!(response.is_err());
        let response_string = format!("{:?}", response);
        let expected_error = "No saved column_values in TestDatabase";
        assert!(
            response_string.contains(expected_error),
            "'{}' did not contain expected content '{}'",
            response_string,
            expected_error
        );

        let expected_request = Some(ColumnValuesRequest {
            predicate: "Predicate { table_names: m5}".into(),
            column_name: "the_tag_key".into(),
        });
        assert_eq!(test_db.get_column_values_request().await, expected_request);

        Ok(())
    }

    #[tokio::test]
    async fn test_log_on_panic() -> Result<(), tonic::Status> {
        // Send a message to a route that causes a panic and ensure:
        // 1. We don't use up all executors 2. The panic message
        // message ends up in the log system

        // Normally, the global panic logger is set at program start
        let f = SendPanicsToTracing::new();

        // capture all tracing messages
        let tracing_capture = TracingCapture::new();

        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let request = TestErrorRequest {};

        // Test response from storage server
        let response = fixture.iox_client.test_error(request).await;

        match &response {
            Ok(_) => {
                panic!("Unexpected success: {:?}", response);
            }
            Err(status) => {
                assert_eq!(status.code(), Code::Unknown);
                assert!(
                    status.message().contains("transport error"),
                    "could not find 'transport error' in '{}'",
                    status.message()
                );
            }
        };

        // Ensure that the logs captured the panic (and drop f
        // beforehand -- if assert fails, it panics and during that
        // panic `f` gets dropped causing a nasty error message)
        drop(f);

        let captured_logs = tracing_capture.to_string();
        // Note we don't include the actual line / column in the
        // expected panic message to avoid needing to update the test
        // whenever the source code file changed.
        let expected_error = "panicked at 'This is a test panic', src/server/rpc/storage.rs";
        assert!(
            captured_logs.contains(expected_error),
            "Logs did not contain expected panic message '{}'. They were\n{}",
            expected_error,
            captured_logs
        );

        // Ensure that panics don't exhaust the tokio executor by
        // running 100 times (success is if we can make a successful
        // call after this)
        for _ in 0usize..100 {
            let request = TestErrorRequest {};

            // Test response from storage server
            let response = fixture.iox_client.test_error(request).await;
            assert!(response.is_err(), "Got an error response: {:?}", response);
        }

        // Ensure there are still threads to answer actual client queries
        let caps = fixture.storage_client.capabilities().await?;
        assert!(!caps.is_empty(), "Caps: {:?}", caps);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_filter() -> Result<(), tonic::Status> {
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let test_db = fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .expect("creating test database");

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        let request = ReadFilterRequest {
            read_source: source.clone(),
            range: make_timestamp_range(150, 200),
            predicate: make_state_ma_predicate(),
        };

        let expected_request = QuerySeriesRequest {
            predicate: "Predicate { exprs: [#state Eq Utf8(\"MA\")] range: TimestampRange { start: 150, end: 200 }}".into()
        };

        let dummy_series_set_plan = SeriesSetPlans::from(vec![]);
        test_db.set_query_series_values(dummy_series_set_plan).await;

        let actual_frames = fixture.storage_client.read_filter(request).await?;

        // TODO: encode this in the test case or something
        let expected_frames: Vec<String> = vec!["0 frames".into()];

        assert_eq!(
            actual_frames, expected_frames,
            "unexpected frames returned by query_series",
        );
        assert_eq!(
            test_db.get_query_series_request().await,
            Some(expected_request),
            "unexpected request to query_series",
        );

        // ---
        // test error
        // ---
        let request = ReadFilterRequest {
            read_source: source.clone(),
            range: None,
            predicate: None,
        };

        // Note we don't set the response on the test database, so we expect an error
        let response = fixture.storage_client.read_filter(request).await;
        assert!(response.is_err());
        let response_string = format!("{:?}", response);
        let expected_error = "No saved query_series in TestDatabase";
        assert!(
            response_string.contains(expected_error),
            "'{}' did not contain expected content '{}'",
            response_string,
            expected_error
        );

        let expected_request = Some(QuerySeriesRequest {
            predicate: "Predicate {}".into(),
        });
        assert_eq!(test_db.get_query_series_request().await, expected_request);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_group() -> Result<(), tonic::Status> {
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let test_db = fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .expect("creating test database");

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        let group = generated_types::read_group_request::Group::None as i32;

        let request = ReadGroupRequest {
            read_source: source.clone(),
            range: make_timestamp_range(150, 200),
            predicate: make_state_ma_predicate(),
            group_keys: vec![String::from("tag1")],
            group,
            aggregate: Some(RPCAggregate {
                r#type: AggregateType::Sum as i32,
            }),
            hints: 0,
        };

        let expected_request = QueryGroupsRequest {
            predicate: "Predicate { exprs: [#state Eq Utf8(\"MA\")] range: TimestampRange { start: 150, end: 200 }}".into(),
            gby_agg: GroupByAndAggregate::Columns {
                agg: QueryAggregate::Sum,
                group_columns: vec![String::from("tag1")],
            }
        };

        // TODO setup any expected results
        let dummy_groups_set_plan = SeriesSetPlans::from(vec![]);
        test_db.set_query_groups_values(dummy_groups_set_plan).await;

        let actual_frames = fixture.storage_client.read_group(request).await?;
        let expected_frames: Vec<String> = vec!["0 group frames".into()];

        assert_eq!(
            actual_frames, expected_frames,
            "unexpected frames returned by query_groups"
        );
        assert_eq!(
            test_db.get_query_groups_request().await,
            Some(expected_request),
            "unexpected request to query_groups"
        );

        // ---
        // test error
        // ---
        let request = ReadGroupRequest {
            read_source: source.clone(),
            range: None,
            predicate: None,
            group_keys: vec![],
            group,
            aggregate: Some(RPCAggregate {
                r#type: AggregateType::Sum as i32,
            }),
            hints: 0,
        };

        // Note we don't set the response on the test database, so we expect an error
        let response = fixture.storage_client.read_group(request).await;
        assert!(response.is_err());
        let response_string = format!("{:?}", response);
        let expected_error = "No saved query_groups in TestDatabase";
        assert!(
            response_string.contains(expected_error),
            "'{}' did not contain expected content '{}'",
            response_string,
            expected_error
        );

        let expected_request = Some(QueryGroupsRequest {
            predicate: "Predicate {}".into(),
            gby_agg: GroupByAndAggregate::Columns {
                agg: QueryAggregate::Sum,
                group_columns: vec![],
            },
        });
        assert_eq!(test_db.get_query_groups_request().await, expected_request);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_window_aggegate() -> Result<(), tonic::Status> {
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let test_db = fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .expect("creating test database");

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        // -----
        // Test with window_every/offset setup
        // -----

        let request_window_every = ReadWindowAggregateRequest {
            read_source: source.clone(),
            range: make_timestamp_range(150, 200),
            predicate: make_state_ma_predicate(),
            window_every: 1122,
            offset: 15,
            aggregate: vec![RPCAggregate {
                r#type: AggregateType::Sum as i32,
            }],
            // old skool window definition
            window: None,
        };

        let expected_request_window_every = QueryGroupsRequest {
            predicate: "Predicate { exprs: [#state Eq Utf8(\"MA\")] range: TimestampRange { start: 150, end: 200 }}".into(),
            gby_agg: GroupByAndAggregate::Window {
                agg: QueryAggregate::Sum,
                every: QueryWindowDuration::Fixed {
                    nanoseconds: 1122,
                },
                offset: QueryWindowDuration::Fixed {
                    nanoseconds: 15,
                }
            }
        };

        // setup expected results
        let dummy_groups_set_plan = SeriesSetPlans::from(vec![]);
        test_db.set_query_groups_values(dummy_groups_set_plan).await;

        let actual_frames = fixture
            .storage_client
            .read_window_aggregate(request_window_every)
            .await?;
        let expected_frames: Vec<String> = vec!["0 aggregate_frames".into()];

        assert_eq!(
            actual_frames, expected_frames,
            "unexpected frames returned by query_groups"
        );
        assert_eq!(
            test_db.get_query_groups_request().await,
            Some(expected_request_window_every),
            "unexpected request to query_groups"
        );

        // -----
        // Test with window.every and window.offset durations specified
        // -----

        let request_window = ReadWindowAggregateRequest {
            read_source: source.clone(),
            range: make_timestamp_range(150, 200),
            predicate: make_state_ma_predicate(),
            window_every: 0,
            offset: 0,
            aggregate: vec![RPCAggregate {
                r#type: AggregateType::Sum as i32,
            }],
            // old skool window definition
            window: Some(RPCWindow {
                every: Some(RPCDuration {
                    nsecs: 1122,
                    months: 0,
                    negative: false,
                }),
                offset: Some(RPCDuration {
                    nsecs: 0,
                    months: 4,
                    negative: true,
                }),
            }),
        };

        let expected_request_window = QueryGroupsRequest {
            predicate: "Predicate { exprs: [#state Eq Utf8(\"MA\")] range: TimestampRange { start: 150, end: 200 }}".into(),
            gby_agg : GroupByAndAggregate::Window {
                agg: QueryAggregate::Sum,
                every: QueryWindowDuration::Fixed {
                    nanoseconds: 1122,
                },
                offset: QueryWindowDuration::Variable {
                    months: 4,
                    negative: true,
                }
            }
        };

        // setup expected results
        let dummy_groups_set_plan = SeriesSetPlans::from(vec![]);
        test_db.set_query_groups_values(dummy_groups_set_plan).await;

        let actual_frames = fixture
            .storage_client
            .read_window_aggregate(request_window.clone())
            .await?;
        let expected_frames: Vec<String> = vec!["0 aggregate_frames".into()];

        assert_eq!(
            actual_frames, expected_frames,
            "unexpected frames returned by query_groups"
        );
        assert_eq!(
            test_db.get_query_groups_request().await,
            Some(expected_request_window.clone()),
            "unexpected request to query_groups"
        );

        // ---
        // test error
        // ---

        // Note we don't set the response on the test database, so we expect an error
        let response = fixture
            .storage_client
            .read_window_aggregate(request_window)
            .await;
        assert!(response.is_err());
        let response_string = format!("{:?}", response);
        let expected_error = "No saved query_groups in TestDatabase";
        assert!(
            response_string.contains(expected_error),
            "'{}' did not contain expected content '{}'",
            response_string,
            expected_error
        );

        assert_eq!(
            test_db.get_query_groups_request().await,
            Some(expected_request_window)
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_measurement_fields() -> Result<(), tonic::Status> {
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let test_db = fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .expect("creating test database");

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        let request = MeasurementFieldsRequest {
            source: source.clone(),
            measurement: String::from("TheMeasurement"),
            range: make_timestamp_range(150, 200),
            predicate: make_state_ma_predicate(),
        };

        let expected_request = FieldColumnsRequest {
            predicate: "Predicate { table_names: TheMeasurement exprs: [#state Eq Utf8(\"MA\")] range: TimestampRange { start: 150, end: 200 }}".into()
        };

        let fieldlist = FieldList {
            fields: vec![Field {
                name: "Field1".into(),
                data_type: DataType::Utf8,
                last_timestamp: 1000,
            }],
        };

        let fieldlist_plan = FieldListPlan::Known(Ok(fieldlist));
        test_db.set_field_colum_names_values(fieldlist_plan).await;

        let actual_fields = fixture.storage_client.measurement_fields(request).await?;
        let expected_fields: Vec<String> = vec!["key: Field1, type: 3, timestamp: 1000".into()];

        assert_eq!(
            actual_fields, expected_fields,
            "unexpected frames returned by measuremnt_fields"
        );
        assert_eq!(
            test_db.get_field_columns_request().await,
            Some(expected_request),
            "unexpected request to measurement-fields"
        );

        // ---
        // test error
        // ---
        let request = MeasurementFieldsRequest {
            source: source.clone(),
            measurement: String::from("TheMeasurement"),
            range: None,
            predicate: None,
        };

        // Note we don't set the response on the test database, so we expect an error
        let response = fixture.storage_client.measurement_fields(request).await;
        assert!(response.is_err());
        let response_string = format!("{:?}", response);
        let expected_error = "No saved field_column_name in TestDatabase";
        assert!(
            response_string.contains(expected_error),
            "'{}' did not contain expected content '{}'",
            response_string,
            expected_error
        );

        let expected_request = Some(FieldColumnsRequest {
            predicate: "Predicate { table_names: TheMeasurement}".into(),
        });
        assert_eq!(test_db.get_field_columns_request().await, expected_request);

        Ok(())
    }

    fn make_timestamp_range(start: i64, end: i64) -> Option<TimestampRange> {
        Some(TimestampRange { start, end })
    }

    /// return a predicate like
    ///
    /// state="MA"
    fn make_state_ma_predicate() -> Option<Predicate> {
        use node::{Comparison, Type, Value};
        let root = Node {
            node_type: Type::ComparisonExpression as i32,
            value: Some(Value::Comparison(Comparison::Equal as i32)),
            children: vec![
                Node {
                    node_type: Type::TagRef as i32,
                    value: Some(Value::TagRefValue("state".to_string().into_bytes())),
                    children: vec![],
                },
                Node {
                    node_type: Type::Literal as i32,
                    value: Some(Value::StringValue("MA".to_string())),
                    children: vec![],
                },
            ],
        };
        Some(Predicate { root: Some(root) })
    }

    /// Convert to a Vec<String> to facilitate comparison with results of client
    fn to_string_vec(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    /// InfluxDB IOx deals with database names. The gRPC interface deals
    /// with org_id and bucket_id represented as 16 digit hex
    /// values. This struct manages creating the org_id, bucket_id,
    /// and database names to be consistent with the implementation
    struct OrgAndBucket {
        org_id: u64,
        bucket_id: u64,
        /// The influxdb_iox database name corresponding to `org_id` and `bucket_id`
        db_name: String,
    }

    impl OrgAndBucket {
        fn new(org_id: u64, bucket_id: u64) -> Self {
            let org_id_str = Id::try_from(org_id).expect("org_id was valid").to_string();

            let bucket_id_str = Id::try_from(bucket_id)
                .expect("bucket_id was valid")
                .to_string();

            let db_name = org_and_bucket_to_database(&org_id_str, &bucket_id_str);

            Self {
                org_id,
                bucket_id,
                db_name,
            }
        }
    }

    /// Wrapper around a StorageClient that does the various tonic /
    /// futures dance
    struct StorageClientWrapper {
        inner: StorageClient,
    }

    impl StorageClientWrapper {
        fn new(inner: StorageClient) -> Self {
            Self { inner }
        }

        /// Create a ReadSource suitable for constructing messages
        fn read_source(org_id: u64, bucket_id: u64, partition_id: u64) -> prost_types::Any {
            let read_source = ReadSource {
                org_id,
                bucket_id,
                partition_id,
            };
            let mut d = Vec::new();
            read_source
                .encode(&mut d)
                .expect("encoded read source appropriately");
            prost_types::Any {
                type_url: "/TODO".to_string(),
                value: d,
            }
        }

        /// return the capabilities of the server as a hash map
        async fn capabilities(&mut self) -> Result<HashMap<String, Vec<String>>, tonic::Status> {
            let response = self.inner.capabilities(()).await?.into_inner();

            let CapabilitiesResponse { caps } = response;

            // unwrap the Vec of Strings inside each `Capability`
            let caps = caps
                .into_iter()
                .map(|(name, capability)| (name, capability.features))
                .collect();

            Ok(caps)
        }

        /// Make a request to query::measurement_names and do the
        /// required async dance to flatten the resulting stream to Strings
        async fn measurement_names(
            &mut self,
            request: MeasurementNamesRequest,
        ) -> Result<Vec<String>, tonic::Status> {
            let responses = self
                .inner
                .measurement_names(request)
                .await?
                .into_inner()
                .try_collect()
                .await?;

            Ok(self.to_string_vec(responses))
        }

        /// Make a request to query::read_window_aggregate and do the
        /// required async dance to flatten the resulting stream to Strings
        async fn read_window_aggregate(
            &mut self,
            request: ReadWindowAggregateRequest,
        ) -> Result<Vec<String>, tonic::Status> {
            let responses: Vec<_> = self
                .inner
                .read_window_aggregate(request)
                .await?
                .into_inner()
                .try_collect()
                .await?;

            let data_frames: Vec<frame::Data> = responses
                .into_iter()
                .flat_map(|r| r.frames)
                .flat_map(|f| f.data)
                .collect();

            let s = format!("{} aggregate_frames", data_frames.len());

            Ok(vec![s])
        }

        /// Make a request to query::tag_keys and do the
        /// required async dance to flatten the resulting stream to Strings
        async fn tag_keys(
            &mut self,
            request: TagKeysRequest,
        ) -> Result<Vec<String>, tonic::Status> {
            let responses = self
                .inner
                .tag_keys(request)
                .await?
                .into_inner()
                .try_collect()
                .await?;

            Ok(self.to_string_vec(responses))
        }

        /// Make a request to query::measurement_tag_keys and do the
        /// required async dance to flatten the resulting stream to Strings
        async fn measurement_tag_keys(
            &mut self,
            request: MeasurementTagKeysRequest,
        ) -> Result<Vec<String>, tonic::Status> {
            let responses = self
                .inner
                .measurement_tag_keys(request)
                .await?
                .into_inner()
                .try_collect()
                .await?;

            Ok(self.to_string_vec(responses))
        }

        /// Make a request to query::tag_values and do the
        /// required async dance to flatten the resulting stream to Strings
        async fn tag_values(
            &mut self,
            request: TagValuesRequest,
        ) -> Result<Vec<String>, tonic::Status> {
            let responses = self
                .inner
                .tag_values(request)
                .await?
                .into_inner()
                .try_collect()
                .await?;

            Ok(self.to_string_vec(responses))
        }

        /// Make a request to query::measurement_tag_values and do the
        /// required async dance to flatten the resulting stream to Strings
        async fn measurement_tag_values(
            &mut self,
            request: MeasurementTagValuesRequest,
        ) -> Result<Vec<String>, tonic::Status> {
            let responses = self
                .inner
                .measurement_tag_values(request)
                .await?
                .into_inner()
                .try_collect()
                .await?;

            Ok(self.to_string_vec(responses))
        }

        /// Make a request to query::read_filter and do the
        /// required async dance to flatten the resulting stream
        async fn read_filter(
            &mut self,
            request: ReadFilterRequest,
        ) -> Result<Vec<String>, tonic::Status> {
            let responses: Vec<_> = self
                .inner
                .read_filter(request)
                .await?
                .into_inner()
                .try_collect()
                .await?;

            let data_frames: Vec<frame::Data> = responses
                .into_iter()
                .flat_map(|r| r.frames)
                .flat_map(|f| f.data)
                .collect();

            let s = format!("{} frames", data_frames.len());

            Ok(vec![s])
        }

        /// Make a request to query::query_groups and do the
        /// required async dance to flatten the resulting stream
        async fn read_group(
            &mut self,
            request: ReadGroupRequest,
        ) -> Result<Vec<String>, tonic::Status> {
            let responses: Vec<_> = self
                .inner
                .read_group(request)
                .await?
                .into_inner()
                .try_collect()
                .await?;

            let data_frames: Vec<frame::Data> = responses
                .into_iter()
                .flat_map(|r| r.frames)
                .flat_map(|f| f.data)
                .collect();

            let s = format!("{} group frames", data_frames.len());

            Ok(vec![s])
        }

        /// Make a request to query::measurement_fields and do the
        /// required async dance to flatten the resulting stream to Strings
        async fn measurement_fields(
            &mut self,
            request: MeasurementFieldsRequest,
        ) -> Result<Vec<String>, tonic::Status> {
            let measurement_fields_response = self.inner.measurement_fields(request).await?;

            let responses: Vec<_> = measurement_fields_response
                .into_inner()
                .try_collect::<Vec<_>>()
                .await?
                .into_iter()
                .flat_map(|r| r.fields)
                .map(|message_field| {
                    format!(
                        "key: {}, type: {}, timestamp: {}",
                        message_field.key, message_field.r#type, message_field.timestamp
                    )
                })
                .collect::<Vec<_>>();

            Ok(responses)
        }

        /// Convert the StringValueResponses into rust Strings, sorting the values
        /// to ensure  consistency.
        fn to_string_vec(&self, responses: Vec<StringValuesResponse>) -> Vec<String> {
            let mut strings = responses
                .into_iter()
                .map(|r| r.values.into_iter())
                .flatten()
                .map(|v| String::from_utf8(v).expect("string value response was not utf8"))
                .collect::<Vec<_>>();

            strings.sort();

            strings
        }
    }

    /// loop and try to make a client connection for 5 seconds,
    /// returning the result of the connection
    async fn connect_to_server<T>(bind_addr: SocketAddr) -> Result<T, tonic::transport::Error>
    where
        T: NewClient,
    {
        const MAX_RETRIES: u32 = 10;
        let mut retry_count = 0;
        loop {
            let mut interval = tokio::time::interval(Duration::from_millis(500));

            match T::connect(format!("http://{}", bind_addr)).await {
                Ok(client) => {
                    println!("Sucessfully connected to server. Client: {:?}", client);
                    return Ok(client);
                }
                Err(e) => {
                    retry_count += 1;

                    if retry_count > 10 {
                        println!("Server did not start in time: {}", e);
                        return Err(e);
                    } else {
                        println!(
                            "Server not yet up. Retrying ({}/{}): {}",
                            retry_count, MAX_RETRIES, e
                        );
                    }
                }
            };
            interval.tick().await;
        }
    }

    #[derive(Debug, Snafu)]
    pub enum FixtureError {
        #[snafu(display("Error binding fixture server: {}", source))]
        Bind { source: std::io::Error },

        #[snafu(display("Error creating fixture: {}", source))]
        Tonic { source: tonic::transport::Error },
    }

    // Wrapper around raw clients and test database
    struct Fixture {
        iox_client: IOxClient,
        storage_client: StorageClientWrapper,
        test_storage: Arc<TestDatabaseStore>,
        _test_executor: Arc<QueryExecutor>,
    }

    impl Fixture {
        /// Start up a test rpc server listening on `port`, returning
        /// a fixture with the test server and clients
        async fn new() -> Result<Self, FixtureError> {
            let test_storage = Arc::new(TestDatabaseStore::new());
            let test_executor = Arc::new(QueryExecutor::default());

            // Get a random port from the kernel by asking for port 0.
            let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
            let socket = tokio::net::TcpListener::bind(bind_addr)
                .await
                .context(Bind)?;

            // Pull the assigned port out of the socket
            let bind_addr = socket.local_addr().unwrap();

            println!("Starting InfluxDB IOx rpc test server on {:?}", bind_addr);

            let server = make_server(socket, test_storage.clone(), test_executor.clone());
            tokio::task::spawn(server);

            let iox_client = connect_to_server::<IOxClient>(bind_addr)
                .await
                .context(Tonic)?;
            let storage_client = StorageClientWrapper::new(
                connect_to_server::<StorageClient>(bind_addr)
                    .await
                    .context(Tonic)?,
            );

            Ok(Self {
                iox_client,
                storage_client,
                test_storage,
                _test_executor: test_executor,
            })
        }
    }

    /// Represents something that can make a connection to a server
    #[tonic::async_trait]
    trait NewClient: Sized + std::fmt::Debug {
        async fn connect(addr: String) -> Result<Self, tonic::transport::Error>;
    }

    #[tonic::async_trait]
    impl NewClient for IOxClient {
        async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
            Self::connect(addr).await
        }
    }

    #[tonic::async_trait]
    impl NewClient for StorageClient {
        async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
            Self::connect(addr).await
        }
    }
}
