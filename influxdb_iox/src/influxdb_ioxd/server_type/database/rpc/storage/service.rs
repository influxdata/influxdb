//! This module contains implementations for the storage gRPC service
//! implemented in terms of the [`QueryDatabase`](query::QueryDatabase) and
//! [`DatabaseStore`]

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use snafu::{OptionExt, ResultExt, Snafu};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use data_types::{error::ErrorLogger, names::org_and_bucket_to_database, DatabaseName};
use generated_types::{
    google::protobuf::Empty, literal_or_regex::Value as RegexOrLiteralValue,
    offsets_response::PartitionOffsetResponse, storage_server::Storage, tag_key_predicate,
    CapabilitiesResponse, Capability, Int64ValuesResponse, LiteralOrRegex,
    MeasurementFieldsRequest, MeasurementFieldsResponse, MeasurementNamesRequest,
    MeasurementTagKeysRequest, MeasurementTagValuesRequest, OffsetsResponse, Predicate,
    ReadFilterRequest, ReadGroupRequest, ReadResponse, ReadSeriesCardinalityRequest,
    ReadWindowAggregateRequest, StringValuesResponse, TagKeyMetaNames, TagKeysRequest,
    TagValuesGroupedByMeasurementAndTagKeyRequest, TagValuesRequest, TagValuesResponse,
    TimestampRange,
};
use observability_deps::tracing::{error, info, trace};
use predicate::predicate::PredicateBuilder;
use query::{
    exec::{
        fieldlist::FieldList, seriesset::converter::Error as SeriesSetError,
        ExecutionContextProvider,
    },
    QueryDatabase,
};
use server::DatabaseStore;

use crate::influxdb_ioxd::{
    planner::Planner,
    server_type::database::rpc::storage::{
        data::{
            fieldlist_to_measurement_fields_response, series_or_groups_to_read_response,
            tag_keys_to_byte_vecs,
        },
        expr::{self, AddRpcNode, GroupByAndAggregate, Loggable, SpecialTagKeys},
        input::GrpcInputs,
        StorageService,
    },
};
use trace::ctx::SpanContext;

use super::{TAG_KEY_FIELD, TAG_KEY_MEASUREMENT};

#[derive(Debug, Snafu)]
pub enum Error {
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
        source: super::expr::Error,
    },

    #[snafu(display("Error converting group type '{}':  {}", aggregate_string, source))]
    ConvertingReadGroupType {
        aggregate_string: String,
        source: super::expr::Error,
    },

    #[snafu(display(
        "Error converting read_group aggregate '{}':  {}",
        aggregate_string,
        source
    ))]
    ConvertingReadGroupAggregate {
        aggregate_string: String,
        source: super::expr::Error,
    },

    #[snafu(display(
        "Error converting read_aggregate_window aggregate definition '{}':  {}",
        aggregate_string,
        source
    ))]
    ConvertingWindowAggregate {
        aggregate_string: String,
        source: super::expr::Error,
    },

    #[snafu(display("Error converting tag_key to UTF-8 in tag_values request, tag_key value '{}': {}", String::from_utf8_lossy(source.as_bytes()), source))]
    ConvertingTagKeyInTagValues { source: std::string::FromUtf8Error },

    #[snafu(display("Error computing groups series: {}", source))]
    ComputingGroupedSeriesSet { source: SeriesSetError },

    #[snafu(display("Converting field information series into gRPC response:  {}", source))]
    ConvertingFieldList { source: super::data::Error },

    #[snafu(display("Error processing measurement constraint {:?}", pred))]
    MeasurementLiteralOrRegex { pred: LiteralOrRegex },

    #[snafu(display("Missing tag key predicate"))]
    MissingTagKeyPredicate {},

    #[snafu(display("Tag Key regex error: {}", source))]
    InvalidTagKeyRegex { source: regex::Error },

    #[snafu(display("Error sending results via channel:  {}", source))]
    SendingResults {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Unexpected hint value on read_group request. Expected 0, got {}",
        hints
    ))]
    InternalHintsFieldNotSupported { hints: u32 },

    #[snafu(display("Operation not yet implemented:  {}", operation))]
    NotYetImplemented { operation: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for tonic::Status {
    /// Converts a result from the business logic into the appropriate tonic
    /// status
    fn from(err: Error) -> Self {
        error!("Error handling gRPC request: {}", err);
        err.to_status()
    }
}

impl Error {
    /// Converts a result from the business logic into the appropriate tonic
    /// status
    fn to_status(&self) -> tonic::Status {
        match &self {
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
            Self::ConvertingTagKeyInTagValues { .. } => Status::invalid_argument(self.to_string()),
            Self::ComputingGroupedSeriesSet { .. } => Status::invalid_argument(self.to_string()),
            Self::ConvertingFieldList { .. } => Status::invalid_argument(self.to_string()),
            Self::SendingResults { .. } => Status::internal(self.to_string()),
            Self::InternalHintsFieldNotSupported { .. } => Status::internal(self.to_string()),
            Self::NotYetImplemented { .. } => Status::internal(self.to_string()),
            Self::MeasurementLiteralOrRegex { .. } => Status::invalid_argument(self.to_string()),
            Self::MissingTagKeyPredicate {} => Status::invalid_argument(self.to_string()),
            Self::InvalidTagKeyRegex { .. } => Status::invalid_argument(self.to_string()),
        }
    }
}

/// Implements the protobuf defined Storage service for a DatabaseStore
#[tonic::async_trait]
impl<T> Storage for StorageService<T>
where
    T: DatabaseStore + 'static,
{
    type ReadFilterStream = futures::stream::Iter<std::vec::IntoIter<Result<ReadResponse, Status>>>;

    async fn read_filter(
        &self,
        req: tonic::Request<ReadFilterRequest>,
    ) -> Result<tonic::Response<Self::ReadFilterStream>, Status> {
        let span_ctx = req.extensions().get().cloned();

        let req = req.into_inner();
        let db_name = get_database_name(&req)?;
        info!(%db_name, ?req.range, predicate=%req.predicate.loggable(), "read filter");

        let db = self
            .db_store
            .db(&db_name)
            .context(DatabaseNotFoundSnafu { db_name: &db_name })?;
        let _query_completed_token = db.record_query("read_filter", defer_json(&req));

        let results = read_filter_impl(Arc::clone(&db), db_name, req, span_ctx)
            .await?
            .into_iter()
            .map(Ok)
            .collect::<Vec<_>>();

        Ok(tonic::Response::new(futures::stream::iter(results)))
    }

    type ReadGroupStream = futures::stream::Iter<std::vec::IntoIter<Result<ReadResponse, Status>>>;

    async fn read_group(
        &self,
        req: tonic::Request<ReadGroupRequest>,
    ) -> Result<tonic::Response<Self::ReadGroupStream>, Status> {
        let span_ctx = req.extensions().get().cloned();
        let req = req.into_inner();

        let db_name = get_database_name(&req)?;
        let db = self
            .db_store
            .db(&db_name)
            .context(DatabaseNotFoundSnafu { db_name: &db_name })?;
        let _query_completed_token = db.record_query("read_group", defer_json(&req));

        let ReadGroupRequest {
            read_source: _read_source,
            range,
            predicate,
            group_keys,
            group,
            aggregate,
        } = req;

        info!(%db_name, ?range, ?group_keys, ?group, ?aggregate,predicate=%predicate.loggable(),"read_group");

        let aggregate_string = format!(
            "aggregate: {:?}, group: {:?}, group_keys: {:?}",
            aggregate, group, group_keys
        );

        let group = expr::convert_group_type(group).context(ConvertingReadGroupTypeSnafu {
            aggregate_string: &aggregate_string,
        })?;

        let gby_agg = expr::make_read_group_aggregate(aggregate, group, group_keys)
            .context(ConvertingReadGroupAggregateSnafu { aggregate_string })?;

        let results = query_group_impl(
            Arc::clone(&db),
            db_name,
            range,
            predicate,
            gby_agg,
            span_ctx,
        )
        .await
        .map_err(|e| e.to_status())?
        .into_iter()
        .map(Ok)
        .collect::<Vec<_>>();

        Ok(tonic::Response::new(futures::stream::iter(results)))
    }

    type ReadWindowAggregateStream =
        futures::stream::Iter<std::vec::IntoIter<Result<ReadResponse, Status>>>;

    async fn read_window_aggregate(
        &self,
        req: tonic::Request<ReadWindowAggregateRequest>,
    ) -> Result<tonic::Response<Self::ReadGroupStream>, Status> {
        let span_ctx = req.extensions().get().cloned();
        let req = req.into_inner();

        let db_name = get_database_name(&req)?;
        let db = self
            .db_store
            .db(&db_name)
            .context(DatabaseNotFoundSnafu { db_name: &db_name })?;
        let _query_completed_token = db.record_query("read_window_aggregate", defer_json(&req));

        let ReadWindowAggregateRequest {
            read_source: _read_source,
            range,
            predicate,
            window_every,
            offset,
            aggregate,
            window,
        } = req;

        info!(%db_name, ?range, ?window_every, ?offset, ?aggregate, ?window, predicate=%predicate.loggable(),"read_window_aggregate");

        let aggregate_string = format!(
            "aggregate: {:?}, window_every: {:?}, offset: {:?}, window: {:?}",
            aggregate, window_every, offset, window
        );

        let gby_agg = expr::make_read_window_aggregate(aggregate, window_every, offset, window)
            .context(ConvertingWindowAggregateSnafu { aggregate_string })?;

        let results = query_group_impl(
            Arc::clone(&db),
            db_name,
            range,
            predicate,
            gby_agg,
            span_ctx,
        )
        .await
        .map_err(|e| e.to_status())?
        .into_iter()
        .map(Ok)
        .collect::<Vec<_>>();

        Ok(tonic::Response::new(futures::stream::iter(results)))
    }

    type TagKeysStream = ReceiverStream<Result<StringValuesResponse, Status>>;

    async fn tag_keys(
        &self,
        req: tonic::Request<TagKeysRequest>,
    ) -> Result<tonic::Response<Self::TagKeysStream>, Status> {
        let span_ctx = req.extensions().get().cloned();
        let (tx, rx) = mpsc::channel(4);

        let req = req.into_inner();

        let db_name = get_database_name(&req)?;
        let db = self
            .db_store
            .db(&db_name)
            .context(DatabaseNotFoundSnafu { db_name: &db_name })?;
        let _query_completed_token = db.record_query("tag_keys", defer_json(&req));

        let TagKeysRequest {
            tags_source: _tag_source,
            range,
            predicate,
        } = req;

        info!(%db_name, ?range, predicate=%predicate.loggable(), "tag_keys");

        let measurement = None;

        let response = tag_keys_impl(
            Arc::clone(&db),
            db_name,
            measurement,
            range,
            predicate,
            span_ctx,
        )
        .await
        .map_err(|e| e.to_status());

        tx.send(response)
            .await
            .expect("sending tag_keys response to server");

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    type TagValuesStream = ReceiverStream<Result<StringValuesResponse, Status>>;

    async fn tag_values(
        &self,
        req: tonic::Request<TagValuesRequest>,
    ) -> Result<tonic::Response<Self::TagValuesStream>, Status> {
        let span_ctx = req.extensions().get().cloned();
        let (tx, rx) = mpsc::channel(4);

        let req = req.into_inner();

        let db_name = get_database_name(&req)?;
        let db = self
            .db_store
            .db(&db_name)
            .context(DatabaseNotFoundSnafu { db_name: &db_name })?;
        let _query_completed_token = db.record_query("tag_values", defer_json(&req));

        let TagValuesRequest {
            tags_source: _tag_source,
            range,
            predicate,
            tag_key,
        } = req;

        let measurement = None;

        // Special case a request for 'tag_key=_measurement" means to list all
        // measurements
        let response = if tag_key.is_measurement() {
            info!(%db_name, ?range, predicate=%predicate.loggable(), "tag_values with tag_key=[x00] (measurement name)");

            if predicate.is_some() {
                return Err(Error::NotYetImplemented {
                    operation: "tag_value for a measurement, with general predicate".to_string(),
                }
                .to_status());
            }

            measurement_name_impl(Arc::clone(&db), db_name, range, predicate, span_ctx).await
        } else if tag_key.is_field() {
            info!(%db_name, ?range, predicate=%predicate.loggable(), "tag_values with tag_key=[xff] (field name)");

            let fieldlist =
                field_names_impl(Arc::clone(&db), db_name, None, range, predicate, span_ctx)
                    .await?;

            // Pick out the field names into a Vec<Vec<u8>>for return
            let values = fieldlist
                .fields
                .into_iter()
                .map(|f| f.name.bytes().collect())
                .collect::<Vec<_>>();

            Ok(StringValuesResponse { values })
        } else {
            let tag_key = String::from_utf8(tag_key).context(ConvertingTagKeyInTagValuesSnafu)?;

            info!(%db_name, ?range, %tag_key, predicate=%predicate.loggable(), "tag_values",);

            tag_values_impl(
                Arc::clone(&db),
                db_name,
                tag_key,
                measurement,
                range,
                predicate,
                span_ctx,
            )
            .await
        };

        let response = response.map_err(|e| e.to_status());

        tx.send(response)
            .await
            .expect("sending tag_values response to server");

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    type TagValuesGroupedByMeasurementAndTagKeyStream =
        futures::stream::Iter<std::vec::IntoIter<Result<TagValuesResponse, Status>>>;

    async fn tag_values_grouped_by_measurement_and_tag_key(
        &self,
        req: tonic::Request<TagValuesGroupedByMeasurementAndTagKeyRequest>,
    ) -> Result<tonic::Response<Self::TagValuesGroupedByMeasurementAndTagKeyStream>, Status> {
        let span_ctx = req.extensions().get().cloned();

        let req = req.into_inner();

        let db_name = get_database_name(&req)?;
        let db = self
            .db_store
            .db(&db_name)
            .context(DatabaseNotFoundSnafu { db_name: &db_name })?;
        let _query_completed_token = db.record_query(
            "tag_values_grouped_by_measurement_and_tag_key",
            defer_json(&req),
        );

        info!(%db_name, ?req.measurement_patterns, ?req.tag_key_predicate, predicate=%req.condition.loggable(), "tag_values_grouped_by_measurement_and_tag_key");

        let results = tag_values_grouped_by_measurement_and_tag_key_impl(
            Arc::clone(&db),
            db_name,
            req,
            span_ctx,
        )
        .await
        .map_err(|e| e.to_status())?
        .into_iter()
        .map(Ok)
        .collect::<Vec<_>>();

        Ok(tonic::Response::new(futures::stream::iter(results)))
    }

    type ReadSeriesCardinalityStream = ReceiverStream<Result<Int64ValuesResponse, Status>>;

    async fn read_series_cardinality(
        &self,
        _req: tonic::Request<ReadSeriesCardinalityRequest>,
    ) -> Result<tonic::Response<Self::ReadSeriesCardinalityStream>, Status> {
        unimplemented!("read_series_cardinality not yet implemented. https://github.com/influxdata/influxdb_iox/issues/447");
    }

    async fn capabilities(
        &self,
        _req: tonic::Request<Empty>,
    ) -> Result<tonic::Response<CapabilitiesResponse>, Status> {
        // Full list of go capabilities in
        // idpe/storage/read/capabilities.go (aka window aggregate /
        // pushdown)
        //

        // For now, hard code our list of support
        let caps = [
            (
                "WindowAggregate",
                vec![
                    "Count", "Sum", // "First"
                    // "Last",
                    "Min", "Max", "Mean",
                    // "Offset"
                ],
            ),
            ("Group", vec!["First", "Last", "Min", "Max"]),
        ];

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

    type MeasurementNamesStream = ReceiverStream<Result<StringValuesResponse, Status>>;

    async fn measurement_names(
        &self,
        req: tonic::Request<MeasurementNamesRequest>,
    ) -> Result<tonic::Response<Self::MeasurementNamesStream>, Status> {
        let span_ctx = req.extensions().get().cloned();
        let (tx, rx) = mpsc::channel(4);

        let req = req.into_inner();

        let db_name = get_database_name(&req)?;
        let db = self
            .db_store
            .db(&db_name)
            .context(DatabaseNotFoundSnafu { db_name: &db_name })?;
        let _query_completed_token = db.record_query("measurement_names", defer_json(&req));

        let MeasurementNamesRequest {
            source: _source,
            range,
            predicate,
        } = req;

        info!(%db_name, ?range, predicate=%predicate.loggable(), "measurement_names");

        let response = measurement_name_impl(Arc::clone(&db), db_name, range, predicate, span_ctx)
            .await
            .map_err(|e| e.to_status());

        tx.send(response)
            .await
            .expect("sending measurement names response to server");

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    type MeasurementTagKeysStream = ReceiverStream<Result<StringValuesResponse, Status>>;

    async fn measurement_tag_keys(
        &self,
        req: tonic::Request<MeasurementTagKeysRequest>,
    ) -> Result<tonic::Response<Self::MeasurementTagKeysStream>, Status> {
        let span_ctx = req.extensions().get().cloned();
        let (tx, rx) = mpsc::channel(4);

        let req = req.into_inner();

        let db_name = get_database_name(&req)?;
        let db = self
            .db_store
            .db(&db_name)
            .context(DatabaseNotFoundSnafu { db_name: &db_name })?;
        let _query_completed_token = db.record_query("measurement_tag_keys", defer_json(&req));

        let MeasurementTagKeysRequest {
            source: _source,
            measurement,
            range,
            predicate,
        } = req;

        info!(%db_name, ?range, %measurement, predicate=%predicate.loggable(), "measurement_tag_keys");

        let measurement = Some(measurement);

        let response = tag_keys_impl(
            Arc::clone(&db),
            db_name,
            measurement,
            range,
            predicate,
            span_ctx,
        )
        .await
        .map_err(|e| e.to_status());

        tx.send(response)
            .await
            .expect("sending measurement_tag_keys response to server");

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    type MeasurementTagValuesStream = ReceiverStream<Result<StringValuesResponse, Status>>;

    async fn measurement_tag_values(
        &self,
        req: tonic::Request<MeasurementTagValuesRequest>,
    ) -> Result<tonic::Response<Self::MeasurementTagValuesStream>, Status> {
        let span_ctx = req.extensions().get().cloned();
        let (tx, rx) = mpsc::channel(4);

        let req = req.into_inner();

        let db_name = get_database_name(&req)?;
        let db = self
            .db_store
            .db(&db_name)
            .context(DatabaseNotFoundSnafu { db_name: &db_name })?;
        let _query_completed_token = db.record_query("measurement_tag_values", defer_json(&req));

        let MeasurementTagValuesRequest {
            source: _source,
            measurement,
            range,
            predicate,
            tag_key,
        } = req;

        info!(%db_name, ?range, %measurement, %tag_key, predicate=%predicate.loggable(), "measurement_tag_values");

        let measurement = Some(measurement);

        let response = tag_values_impl(
            Arc::clone(&db),
            db_name,
            tag_key,
            measurement,
            range,
            predicate,
            span_ctx,
        )
        .await
        .map_err(|e| e.to_status());

        tx.send(response)
            .await
            .expect("sending measurement_tag_values response to server");

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    type MeasurementFieldsStream = ReceiverStream<Result<MeasurementFieldsResponse, Status>>;

    async fn measurement_fields(
        &self,
        req: tonic::Request<MeasurementFieldsRequest>,
    ) -> Result<tonic::Response<Self::MeasurementFieldsStream>, Status> {
        let span_ctx = req.extensions().get().cloned();
        let (tx, rx) = mpsc::channel(4);

        let req = req.into_inner();

        let db_name = get_database_name(&req)?;
        let db = self
            .db_store
            .db(&db_name)
            .context(DatabaseNotFoundSnafu { db_name: &db_name })?;
        let _query_completed_token = db.record_query("measurement_fields", defer_json(&req));

        let MeasurementFieldsRequest {
            source: _source,
            measurement,
            range,
            predicate,
        } = req;

        info!(%db_name, ?range, predicate=%predicate.loggable(), "measurement_fields");

        let measurement = Some(measurement);

        let response = field_names_impl(
            Arc::clone(&db),
            db_name,
            measurement,
            range,
            predicate,
            span_ctx,
        )
        .await
        .map(|fieldlist| {
            fieldlist_to_measurement_fields_response(fieldlist)
                .context(ConvertingFieldListSnafu)
                .map_err(|e| e.to_status())
        })
        .map_err(|e| e.to_status())?;

        tx.send(response)
            .await
            .expect("sending measurement_fields response to server");

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    async fn offsets(
        &self,
        _req: tonic::Request<Empty>,
    ) -> Result<tonic::Response<OffsetsResponse>, Status> {
        // We present ourselves to the rest of IDPE as a single storage node with 1 partition.
        // (Returning offset 1 just in case offset 0 is interpreted by query nodes as being special)
        let the_partition = PartitionOffsetResponse { id: 0, offset: 1 };
        Ok(tonic::Response::new(OffsetsResponse {
            partitions: vec![the_partition],
        }))
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

fn get_database_name(input: &impl GrpcInputs) -> Result<DatabaseName<'static>, Status> {
    org_and_bucket_to_database(input.org_id()?.to_string(), &input.bucket_name()?)
        .map_err(|e| Status::internal(e.to_string()))
}

// The following code implements the business logic of the requests as
// methods that return Results with module specific Errors (and thus
// can use ?, etc). The trait implementations then handle mapping
// to the appropriate tonic Status

/// Gathers all measurement names that have data in the specified
/// (optional) range
async fn measurement_name_impl<D>(
    db: Arc<D>,
    db_name: DatabaseName<'static>,
    range: Option<TimestampRange>,
    rpc_predicate: Option<Predicate>,
    span_ctx: Option<SpanContext>,
) -> Result<StringValuesResponse>
where
    D: QueryDatabase + ExecutionContextProvider + 'static,
{
    let rpc_predicate_string = format!("{:?}", rpc_predicate);
    let db_name = db_name.as_str();

    let predicate = PredicateBuilder::default()
        .set_range(range)
        .rpc_predicate(rpc_predicate)
        .context(ConvertingPredicateSnafu {
            rpc_predicate_string,
        })?
        .build();

    let ctx = db.new_query_context(span_ctx);

    let plan = Planner::new(&ctx)
        .table_names(db, predicate)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ListingTablesSnafu { db_name })?;

    let table_names = ctx
        .to_string_set(plan)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ListingTablesSnafu { db_name })?;

    // Map the resulting collection of Strings into a Vec<Vec<u8>>for return
    let values: Vec<Vec<u8>> = table_names
        .iter()
        .map(|name| name.bytes().collect())
        .collect();

    trace!(measurement_names=?values.iter().map(|k| String::from_utf8_lossy(k)).collect::<Vec<_>>(), "Measurement names response");
    Ok(StringValuesResponse { values })
}

/// Return tag keys with optional measurement, timestamp and arbitrary
/// predicates
async fn tag_keys_impl<D>(
    db: Arc<D>,
    db_name: DatabaseName<'static>,
    measurement: Option<String>,
    range: Option<TimestampRange>,
    rpc_predicate: Option<Predicate>,
    span_ctx: Option<SpanContext>,
) -> Result<StringValuesResponse>
where
    D: QueryDatabase + ExecutionContextProvider + 'static,
{
    let rpc_predicate_string = format!("{:?}", rpc_predicate);
    let db_name = db_name.as_str();

    let predicate = PredicateBuilder::default()
        .set_range(range)
        .table_option(measurement)
        .rpc_predicate(rpc_predicate)
        .context(ConvertingPredicateSnafu {
            rpc_predicate_string,
        })?
        .build();

    let ctx = db.new_query_context(span_ctx);

    let tag_key_plan = Planner::new(&ctx)
        .tag_keys(db, predicate)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ListingColumnsSnafu { db_name })?;

    let tag_keys = ctx
        .to_string_set(tag_key_plan)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ListingColumnsSnafu { db_name })?;

    // Map the resulting collection of Strings into a Vec<Vec<u8>>for return
    let values = tag_keys_to_byte_vecs(tag_keys);

    trace!(tag_keys=?values.iter().map(|k| String::from_utf8_lossy(k)).collect::<Vec<_>>(), "Tag keys response");
    Ok(StringValuesResponse { values })
}

/// Return tag values for tag_name, with optional measurement, timestamp and
/// arbitratry predicates
async fn tag_values_impl<D>(
    db: Arc<D>,
    db_name: DatabaseName<'static>,
    tag_name: String,
    measurement: Option<String>,
    range: Option<TimestampRange>,
    rpc_predicate: Option<Predicate>,
    span_ctx: Option<SpanContext>,
) -> Result<StringValuesResponse>
where
    D: QueryDatabase + ExecutionContextProvider + 'static,
{
    let rpc_predicate_string = format!("{:?}", rpc_predicate);

    let predicate = PredicateBuilder::default()
        .set_range(range)
        .table_option(measurement)
        .rpc_predicate(rpc_predicate)
        .context(ConvertingPredicateSnafu {
            rpc_predicate_string,
        })?
        .build();

    let db_name = db_name.as_str();
    let tag_name = &tag_name;

    let ctx = db.new_query_context(span_ctx);

    let tag_value_plan = Planner::new(&ctx)
        .tag_values(db, tag_name, predicate)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ListingTagValuesSnafu { db_name, tag_name })?;

    let tag_values = ctx
        .to_string_set(tag_value_plan)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ListingTagValuesSnafu { db_name, tag_name })?;

    // Map the resulting collection of Strings into a Vec<Vec<u8>>for return
    let values: Vec<Vec<u8>> = tag_values
        .iter()
        .map(|name| name.bytes().collect())
        .collect();

    trace!(tag_values=?values.iter().map(|k| String::from_utf8_lossy(k)).collect::<Vec<_>>(), "Tag values response");
    Ok(StringValuesResponse { values })
}

/// Return tag values grouped by one or more measurements with optional
/// filtering predicate and optionally scoped to one or more tag keys.
async fn tag_values_grouped_by_measurement_and_tag_key_impl<D>(
    db: Arc<D>,
    db_name: DatabaseName<'static>,
    req: TagValuesGroupedByMeasurementAndTagKeyRequest,
    span_ctx: Option<SpanContext>,
) -> Result<Vec<TagValuesResponse>, Error>
where
    D: QueryDatabase + ExecutionContextProvider + 'static,
{
    // Extract the tag key predicate.
    // See https://docs.influxdata.com/influxdb/v1.8/query_language/explore-schema/#show-tag-values
    // for more details.
    let tag_key_pred = req
        .tag_key_predicate
        .context(MissingTagKeyPredicateSnafu {})?
        .value
        .context(MissingTagKeyPredicateSnafu {})?;

    // Because we need to return tag values grouped by measurements and tag
    // keys we will materialise the measurements up front, so we can build up
    // groups of tag values grouped by a measurement and tag key.
    let measurements = materialise_measurement_names(
        Arc::clone(&db),
        db_name.clone(),
        req.measurement_patterns,
        span_ctx.clone(),
    )
    .await?;

    let mut responses = vec![];
    for name in measurements.into_iter() {
        let tag_keys = materialise_tag_keys(
            Arc::clone(&db),
            db_name.clone(),
            name.clone(),
            tag_key_pred.clone(),
            span_ctx.clone(),
        )
        .await?;

        for key in tag_keys {
            // get all the tag values associated with this measurement name and tag key
            let values = tag_values_impl(
                Arc::clone(&db),
                db_name.clone(),
                key.clone(),
                Some(name.clone()),
                None,
                req.condition.clone(),
                span_ctx.clone(),
            )
            .await?
            .values
            .into_iter()
            .map(|v| String::from_utf8(v).expect("tag values should be UTF-8 valid"))
            .collect::<Vec<_>>();

            // Don't emit a response if there are no matching tag values.
            if values.is_empty() {
                continue;
            }

            responses.push(TagValuesResponse {
                measurement: name.clone(),
                key: key.clone(),
                values,
            });
        }
    }

    Ok(responses)
}

/// Launch async tasks that materialises the result of executing read_filter.
async fn read_filter_impl<D>(
    db: Arc<D>,
    db_name: DatabaseName<'static>,
    req: ReadFilterRequest,
    span_ctx: Option<SpanContext>,
) -> Result<Vec<ReadResponse>, Error>
where
    D: QueryDatabase + ExecutionContextProvider + 'static,
{
    let db_name = db_name.as_str();

    let ctx = db.new_query_context(span_ctx);

    let rpc_predicate_string = format!("{:?}", req.predicate);

    let predicate = PredicateBuilder::default()
        .set_range(req.range)
        .rpc_predicate(req.predicate)
        .context(ConvertingPredicateSnafu {
            rpc_predicate_string,
        })?
        .build();

    // PERF - This used to send responses to the client before execution had
    // completed, but now it doesn't. We may need to revisit this in the future
    // if big queries are causing a significant latency in TTFB.

    // Build the plans
    let series_plan = Planner::new(&ctx)
        .read_filter(db, predicate)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(PlanningFilteringSeriesSnafu { db_name })?;

    // Execute the plans.
    let series_or_groups = ctx
        .to_series_and_groups(series_plan)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(FilteringSeriesSnafu { db_name })
        .log_if_error("Running series set plan")?;

    let emit_tag_keys_binary_format = req.tag_key_meta_names == TagKeyMetaNames::Binary as i32;
    let response = series_or_groups_to_read_response(series_or_groups, emit_tag_keys_binary_format);

    Ok(vec![response])
}

/// Launch async tasks that send the result of executing read_group to `tx`
async fn query_group_impl<D>(
    db: Arc<D>,
    db_name: DatabaseName<'static>,
    range: Option<TimestampRange>,
    rpc_predicate: Option<Predicate>,
    gby_agg: GroupByAndAggregate,
    span_ctx: Option<SpanContext>,
) -> Result<Vec<ReadResponse>, Error>
where
    D: QueryDatabase + ExecutionContextProvider + 'static,
{
    let db_name = db_name.as_str();
    let ctx = db.new_query_context(span_ctx);

    let rpc_predicate_string = format!("{:?}", rpc_predicate);

    let predicate = PredicateBuilder::default()
        .set_range(range)
        .rpc_predicate(rpc_predicate)
        .context(ConvertingPredicateSnafu {
            rpc_predicate_string,
        })?
        .build();

    let planner = Planner::new(&ctx);
    let grouped_series_set_plan = match gby_agg {
        GroupByAndAggregate::Columns { agg, group_columns } => {
            planner.read_group(db, predicate, agg, group_columns).await
        }
        GroupByAndAggregate::Window { agg, every, offset } => {
            planner
                .read_window_aggregate(db, predicate, agg, every, offset)
                .await
        }
    };
    let grouped_series_set_plan = grouped_series_set_plan
        .map_err(|e| Box::new(e) as _)
        .context(PlanningGroupSeriesSnafu { db_name })?;

    // PERF - This used to send responses to the client before execution had
    // completed, but now it doesn't. We may need to revisit this in the future
    // if big queries are causing a significant latency in TTFB.

    // Execute the plans
    let series_or_groups = ctx
        .to_series_and_groups(grouped_series_set_plan)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(GroupingSeriesSnafu { db_name })
        .log_if_error("Running Grouped SeriesSet Plan")?;

    // ReadGroupRequest does not have a field to control the format of
    // _measurement and _field tag keys, so always request in string format.
    let response = series_or_groups_to_read_response(series_or_groups, false);

    Ok(vec![response])
}

/// Return field names, restricted via optional measurement, timestamp and
/// predicate
async fn field_names_impl<D>(
    db: Arc<D>,
    db_name: DatabaseName<'static>,
    measurement: Option<String>,
    range: Option<TimestampRange>,
    rpc_predicate: Option<Predicate>,
    span_ctx: Option<SpanContext>,
) -> Result<FieldList>
where
    D: QueryDatabase + ExecutionContextProvider + 'static,
{
    let rpc_predicate_string = format!("{:?}", rpc_predicate);

    let predicate = PredicateBuilder::default()
        .set_range(range)
        .table_option(measurement)
        .rpc_predicate(rpc_predicate)
        .context(ConvertingPredicateSnafu {
            rpc_predicate_string,
        })?
        .build();

    let db_name = db_name.as_str();
    let ctx = db.new_query_context(span_ctx);

    let field_list_plan = Planner::new(&ctx)
        .field_columns(db, predicate)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ListingFieldsSnafu { db_name })?;

    let field_list = ctx
        .to_field_list(field_list_plan)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ListingFieldsSnafu { db_name })?;

    trace!(field_names=?field_list, "Field names response");
    Ok(field_list)
}

/// Materialises a collection of measurement names. Typically used as part of
/// a plan to scope and group multiple plans by measurement name.
async fn materialise_measurement_names<D>(
    db: Arc<D>,
    db_name: DatabaseName<'static>,
    measurement_exprs: Vec<LiteralOrRegex>,
    span_ctx: Option<SpanContext>,
) -> Result<BTreeSet<String>, Error>
where
    D: QueryDatabase + ExecutionContextProvider + 'static,
{
    use generated_types::{
        node::{Comparison, Type, Value},
        Node,
    };

    let mut names = BTreeSet::new();

    // Materialise all measurements
    if measurement_exprs.is_empty() {
        let resp = measurement_name_impl(
            Arc::clone(&db),
            db_name.clone(),
            None,
            None,
            span_ctx.clone(),
        )
        .await?;
        for name in resp.values {
            names
                .insert(String::from_utf8(name).expect("table/measurement name to be valid UTF-8"));
        }
        return Ok(names);
    }

    // Materialise measurements that satisfy the provided predicates.
    for expr in measurement_exprs {
        match expr.value {
            Some(expr) => match expr {
                RegexOrLiteralValue::LiteralValue(lit) => {
                    names.insert(lit);
                }
                RegexOrLiteralValue::RegexValue(pattern) => {
                    let regex_node = Node {
                        node_type: Type::ComparisonExpression as i32,
                        children: vec![
                            Node {
                                node_type: Type::TagRef as i32,
                                children: vec![],
                                value: Some(Value::TagRefValue(TAG_KEY_MEASUREMENT.to_vec())),
                            },
                            Node {
                                node_type: Type::Literal as i32,
                                children: vec![],
                                value: Some(Value::RegexValue(pattern)),
                            },
                        ],
                        value: Some(Value::Comparison(Comparison::Regex as i32)),
                    };
                    let resp = measurement_name_impl(
                        Arc::clone(&db),
                        db_name.clone(),
                        None,
                        Some(Predicate {
                            root: Some(regex_node),
                        }),
                        span_ctx.clone(),
                    )
                    .await?;
                    for name in resp.values {
                        names.insert(
                            String::from_utf8(name)
                                .expect("table/measurement name to be valid UTF-8"),
                        );
                    }
                }
            },
            None => return MeasurementLiteralOrRegexSnafu { pred: expr }.fail(),
        }
    }
    Ok(names)
}

/// Materialises a collection of tag keys for a given measurement.
///
/// TODO(edd): this might be better represented as a plan against the `columns`
/// system table.
async fn materialise_tag_keys<D>(
    db: Arc<D>,
    db_name: DatabaseName<'static>,
    measurement_name: String,
    tag_key_predicate: tag_key_predicate::Value,
    span_ctx: Option<SpanContext>,
) -> Result<BTreeSet<String>, Error>
where
    D: QueryDatabase + ExecutionContextProvider + 'static,
{
    use generated_types::tag_key_predicate::Value;

    if let Value::Eq(elem) = tag_key_predicate {
        // If the predicate is a simple literal match then return that value
        // regardless of whether or not the tag key exists.
        return Ok(vec![elem].into_iter().collect::<BTreeSet<_>>());
    } else if let Value::In(elem) = tag_key_predicate {
        // If the predicate is a list of literal matches then return those
        // regardless of whether or not those tag keys exist.
        return Ok(elem.vals.into_iter().collect::<BTreeSet<_>>());
    }

    // Otherwise materialise the tag keys for this measurement and filter out
    // any that don't pass the provided tag key predicate.
    let mut tag_keys = tag_keys_impl(
        Arc::clone(&db),
        db_name.clone(),
        Some(measurement_name),
        None,
        None,
        span_ctx.clone(),
    )
    .await?
    .values
    .into_iter()
    .filter_map(|v| match v.as_slice() {
        // The tag_keys plan will yield the special measurement and field tag keys
        // which are not real tag keys. Filter them out.
        TAG_KEY_MEASUREMENT | TAG_KEY_FIELD => None,
        _ => Some(String::from_utf8(v).expect("tag keys should be UTF-8 valid")),
    })
    .collect::<BTreeSet<_>>();

    // Filter out tag keys according to the type of expression provided.
    match tag_key_predicate {
        Value::Neq(value) => tag_keys.retain(|elem| elem != &value),
        Value::EqRegex(pattern) => {
            let re = regex::Regex::new(&pattern).context(InvalidTagKeyRegexSnafu)?;
            tag_keys.retain(|elem| re.is_match(elem));
        }
        Value::NeqRegex(pattern) => {
            let re = regex::Regex::new(&pattern).context(InvalidTagKeyRegexSnafu)?;
            tag_keys.retain(|elem| !re.is_match(elem));
        }
        x => unreachable!("predicate should have been handled already {:?}", x),
    }

    Ok(tag_keys)
}

/// Return something which can be formatted as json ("pbjson"
/// specifically)
fn defer_json<S>(s: &S) -> impl Into<String> + '_
where
    S: serde::Serialize,
{
    /// Defers conversion into a String
    struct DeferredToJson<'a, S>
    where
        S: serde::Serialize,
    {
        s: &'a S,
    }

    impl<S> From<DeferredToJson<'_, S>> for String
    where
        S: serde::Serialize,
    {
        fn from(w: DeferredToJson<'_, S>) -> Self {
            match serde_json::to_string_pretty(&w.s) {
                Ok(json) => json,
                Err(e) => e.to_string(),
            }
        }
    }

    DeferredToJson { s }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        num::NonZeroU64,
        sync::Arc,
    };

    use data_types::chunk_metadata::ChunkId;
    use generated_types::{i_ox_testing_client::IOxTestingClient, tag_key_predicate::Value};
    use parking_lot::Mutex;
    use tokio_stream::wrappers::TcpListenerStream;

    use datafusion::logical_plan::{col, lit, Expr};
    use influxdb_storage_client::{
        connection::{Builder as ConnectionBuilder, Connection},
        generated_types::*,
        Client as StorageClient, OrgAndBucket,
    };
    use panic_logging::SendPanicsToTracing;
    use predicate::predicate::PredicateMatch;
    use query::{
        exec::Executor,
        test::{TestChunk, TestDatabase, TestError},
    };
    use test_helpers::{assert_contains, tracing::TracingCapture};

    use super::*;
    use metric::{Attributes, Metric, U64Counter};

    fn to_str_vec(s: &[&str]) -> Vec<String> {
        s.iter().map(|s| s.to_string()).collect()
    }

    // Helper function to assert that metric tracking all gRPC requests has
    // correctly updated.
    fn grpc_request_metric_has_count(
        fixture: &Fixture,
        path: &'static str,
        status: &'static str,
        count: u64,
    ) {
        let metric = fixture
            .test_storage
            .metric_registry
            .get_instrument::<Metric<U64Counter>>("grpc_requests")
            .unwrap();

        let observation = metric
            .get_observer(&Attributes::from([
                (
                    "path",
                    format!("/influxdata.platform.storage.Storage/{}", path).into(),
                ),
                ("status", status.into()),
            ]))
            .unwrap()
            .fetch();

        assert_eq!(observation, count);
    }

    #[tokio::test]
    async fn test_storage_rpc_capabilities() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        // Test response from storage server
        let mut expected_capabilities: HashMap<String, Vec<String>> = HashMap::new();
        expected_capabilities.insert(
            "WindowAggregate".into(),
            to_str_vec(&["Count", "Sum", "Min", "Max", "Mean"]),
        );

        expected_capabilities.insert("Group".into(), to_str_vec(&["First", "Last", "Min", "Max"]));

        assert_eq!(
            expected_capabilities,
            fixture.storage_client.capabilities().await.unwrap()
        );
    }

    fn org_and_bucket() -> OrgAndBucket {
        OrgAndBucket::new(NonZeroU64::new(123).unwrap(), NonZeroU64::new(456).unwrap())
    }

    #[tokio::test]
    async fn test_storage_rpc_measurement_names() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        let chunk0 = TestChunk::new("h2o")
            .with_id(0)
            .with_predicate_match(PredicateMatch::AtLeastOneNonNullField);

        let chunk1 = TestChunk::new("o2")
            .with_id(1)
            .with_predicate_match(PredicateMatch::AtLeastOneNonNullField);

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk0))
            .add_chunk("my_partition_key", Arc::new(chunk1));

        let source = Some(StorageClient::read_source(&db_info, 1));

        // --- No timestamps
        let request = MeasurementNamesRequest {
            source: source.clone(),
            range: None,
            predicate: None,
        };

        let actual_measurements = fixture
            .storage_client
            .measurement_names(request)
            .await
            .unwrap();
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

        let actual_measurements = fixture
            .storage_client
            .measurement_names(request)
            .await
            .unwrap();
        let expected_measurements = to_string_vec(&["h2o", "o2"]);
        assert_eq!(actual_measurements, expected_measurements);

        // also ensure the plumbing is hooked correctly and that the predicate made it
        // down to the chunk
        let actual_predicates = fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .expect("getting db")
            .get_chunk("my_partition_key", ChunkId::new_test(0))
            .unwrap()
            .predicates();

        let expected_predicate = PredicateBuilder::default()
            .timestamp_range(150, 200)
            .build();

        assert!(
            actual_predicates.contains(&expected_predicate),
            "\nActual: {:?}\nExpected: {:?}",
            actual_predicates,
            expected_predicate
        );

        // --- general predicate
        let request = MeasurementNamesRequest {
            source: Some(StorageClient::read_source(&db_info, 1)),
            range: Some(TimestampRange {
                start: 150,
                end: 200,
            }),
            predicate: Some(make_state_eq_ma_predicate()),
        };

        let actual_measurements = fixture
            .storage_client
            .measurement_names(request)
            .await
            .unwrap();
        let expected_measurements = to_string_vec(&["h2o", "o2"]);
        assert_eq!(actual_measurements, expected_measurements);

        grpc_request_metric_has_count(&fixture, "MeasurementNames", "ok", 3);
    }

    /// test the plumbing of the RPC layer for tag_keys -- specifically that
    /// the right parameters are passed into the Database interface
    /// and that the returned values are sent back via gRPC.
    #[tokio::test]
    async fn test_storage_rpc_tag_keys() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        // Note multiple tables / measureemnts:
        let chunk0 = TestChunk::new("m1")
            .with_id(0)
            .with_tag_column("k1")
            .with_tag_column("k2");

        let chunk1 = TestChunk::new("m2")
            .with_id(1)
            .with_tag_column("k3")
            .with_tag_column("k4");

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk0))
            .add_chunk("my_partition_key", Arc::new(chunk1));

        let source = Some(StorageClient::read_source(&db_info, 1));

        let request = TagKeysRequest {
            tags_source: source.clone(),
            range: Some(make_timestamp_range(150, 200)),
            predicate: Some(make_state_eq_ma_predicate()),
        };

        let actual_tag_keys = fixture.storage_client.tag_keys(request).await.unwrap();
        let expected_tag_keys = vec!["_f(0xff)", "_m(0x00)", "k1", "k2", "k3", "k4"];

        assert_eq!(actual_tag_keys, expected_tag_keys,);

        // also ensure the plumbing is hooked correctly and that the predicate made it
        // down to the chunk
        let actual_predicates = fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .expect("getting db")
            .get_chunk("my_partition_key", ChunkId::new_test(0))
            .unwrap()
            .predicates();

        let expected_predicate = PredicateBuilder::default()
            .timestamp_range(150, 200)
            .add_expr(make_state_ma_expr())
            .build();

        assert!(
            actual_predicates.contains(&expected_predicate),
            "\nActual: {:?}\nExpected: {:?}",
            actual_predicates,
            expected_predicate
        );

        grpc_request_metric_has_count(&fixture, "TagKeys", "ok", 1);
    }

    #[tokio::test]
    async fn test_storage_rpc_tag_keys_error() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        let chunk = TestChunk::new("my_table").with_error("Sugar we are going down");

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

        // ---
        // test error
        // ---
        let request = TagKeysRequest {
            tags_source: source.clone(),
            range: None,
            predicate: None,
        };

        let response = fixture.storage_client.tag_keys(request).await;
        assert_contains!(response.unwrap_err().to_string(), "Sugar we are going down");

        grpc_request_metric_has_count(&fixture, "TagKeys", "client_error", 1);
    }

    /// test the plumbing of the RPC layer for measurement_tag_keys--
    /// specifically that the right parameters are passed into the Database
    /// interface and that the returned values are sent back via gRPC.
    #[tokio::test]
    async fn test_storage_rpc_measurement_tag_keys() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        let chunk0 = TestChunk::new("m1")
            // predicate specifies m4, so this is filtered out
            .with_tag_column("k0");

        let chunk1 = TestChunk::new("m4")
            .with_tag_column("k1")
            .with_tag_column("k2")
            .with_tag_column("k3")
            .with_tag_column("k4");

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk0))
            .add_chunk("my_partition_key", Arc::new(chunk1));

        let source = Some(StorageClient::read_source(&db_info, 1));

        // ---
        // Timestamp + Predicate
        // ---
        let request = MeasurementTagKeysRequest {
            measurement: "m4".into(),
            source: source.clone(),
            range: Some(make_timestamp_range(150, 200)),
            predicate: Some(make_state_eq_ma_predicate()),
        };

        let actual_tag_keys = fixture
            .storage_client
            .measurement_tag_keys(request)
            .await
            .unwrap();
        let expected_tag_keys = vec!["_f(0xff)", "_m(0x00)", "k1", "k2", "k3", "k4"];

        assert_eq!(
            actual_tag_keys, expected_tag_keys,
            "unexpected tag keys while getting column names"
        );

        // also ensure the plumbing is hooked correctly and that the predicate made it
        // down to the chunk
        let actual_predicates = fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .expect("getting db")
            .get_chunk("my_partition_key", ChunkId::new_test(0))
            .unwrap()
            .predicates();

        let expected_predicate = PredicateBuilder::default()
            .timestamp_range(150, 200)
            .add_expr(make_state_ma_expr())
            .table("m4")
            .build();

        assert!(
            actual_predicates.contains(&expected_predicate),
            "\nActual: {:?}\nExpected: {:?}",
            actual_predicates,
            expected_predicate
        );

        grpc_request_metric_has_count(&fixture, "MeasurementTagKeys", "ok", 1);
    }

    #[tokio::test]
    async fn test_storage_rpc_measurement_tag_keys_error() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        // predicate specifies m4, so this is filtered out
        let chunk = TestChunk::new("my_table").with_error("This is an error");

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

        // ---
        // test error
        // ---
        let request = MeasurementTagKeysRequest {
            measurement: "m5".into(),
            source: source.clone(),
            range: None,
            predicate: None,
        };

        let response = fixture.storage_client.measurement_tag_keys(request).await;
        assert_contains!(response.unwrap_err().to_string(), "This is an error");

        grpc_request_metric_has_count(&fixture, "MeasurementTagKeys", "client_error", 1);
    }

    /// test the plumbing of the RPC layer for tag_values -- specifically that
    /// the right parameters are passed into the Database interface
    /// and that the returned values are sent back via gRPC.
    #[tokio::test]
    async fn test_storage_rpc_tag_values() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        // Add a chunk with a field
        let chunk = TestChunk::new("TheMeasurement")
            .with_time_column()
            .with_tag_column("state")
            .with_one_row_of_data();

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

        let request = TagValuesRequest {
            tags_source: source.clone(),
            range: Some(make_timestamp_range(150, 2000)),
            predicate: Some(make_state_eq_ma_predicate()),
            tag_key: "state".into(),
        };

        let actual_tag_values = fixture.storage_client.tag_values(request).await.unwrap();
        assert_eq!(actual_tag_values, vec!["MA"]);

        grpc_request_metric_has_count(&fixture, "TagValues", "ok", 1);
    }

    /// test the plumbing of the RPC layer for tag_values
    ///
    /// For the special case of
    ///
    /// tag_key = _measurement means listing all measurement names
    #[tokio::test]
    async fn test_storage_rpc_tag_values_with_measurement() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        let source = Some(StorageClient::read_source(&db_info, 1));

        // ---
        // test tag_key = _measurement means listing all measurement names
        // ---
        let request = TagValuesRequest {
            tags_source: source.clone(),
            range: Some(make_timestamp_range(1000, 1500)),
            predicate: None,
            tag_key: [0].into(),
        };

        let chunk =
            TestChunk::new("h2o").with_predicate_match(PredicateMatch::AtLeastOneNonNullField);

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let tag_values = vec!["h2o"];
        let actual_tag_values = fixture.storage_client.tag_values(request).await.unwrap();
        assert_eq!(
            actual_tag_values, tag_values,
            "unexpected tag values while getting tag values for measurement names"
        );

        grpc_request_metric_has_count(&fixture, "TagValues", "ok", 1);
    }

    #[tokio::test]
    async fn test_storage_rpc_tag_values_field() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        // Add a chunk with a field
        let chunk = TestChunk::new("TheMeasurement")
            .with_i64_field_column("Field1")
            .with_time_column()
            .with_tag_column("state")
            .with_one_row_of_data();

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

        // ---
        // test tag_key = _field means listing all field names
        // ---
        let request = TagValuesRequest {
            tags_source: source.clone(),
            range: Some(make_timestamp_range(0, 2000)),
            predicate: Some(make_state_eq_ma_predicate()),
            tag_key: [255].into(),
        };

        let expected_tag_values = vec!["Field1"];
        let actual_tag_values = fixture.storage_client.tag_values(request).await.unwrap();
        assert_eq!(
            actual_tag_values, expected_tag_values,
            "unexpected tag values while getting tag values for field names"
        );

        grpc_request_metric_has_count(&fixture, "TagValues", "ok", 1);
    }

    #[tokio::test]
    async fn test_storage_rpc_tag_values_error() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        let chunk = TestChunk::new("my_table").with_error("Sugar we are going down");

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

        // ---
        // test error
        // ---
        let request = TagValuesRequest {
            tags_source: source.clone(),
            range: None,
            predicate: None,
            tag_key: "the_tag_key".into(),
        };

        let response_string = fixture
            .storage_client
            .tag_values(request)
            .await
            .unwrap_err()
            .to_string();

        assert_contains!(response_string, "Sugar we are going down");

        // ---
        // test error with non utf8 value
        // ---
        let request = TagValuesRequest {
            tags_source: source.clone(),
            range: None,
            predicate: None,
            tag_key: [0, 255].into(), // this is not a valid UTF-8 string
        };

        let response_string = fixture
            .storage_client
            .tag_values(request)
            .await
            .unwrap_err()
            .to_string();

        assert_contains!(
            response_string,
            "Error converting tag_key to UTF-8 in tag_values request"
        );

        grpc_request_metric_has_count(&fixture, "TagValues", "client_error", 2);
    }

    #[tokio::test]
    async fn test_storage_rpc_tag_values_grouped_by_measurement_and_tag_key() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();
        let chunk1 = TestChunk::new("table_a")
            .with_id(0)
            .with_tag_column("state")
            .with_one_row_of_data();
        let chunk2 = TestChunk::new("table_b")
            .with_id(1)
            .with_tag_column("state")
            .with_one_row_of_data();

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk1))
            .add_chunk("my_partition_key", Arc::new(chunk2));

        let source = Some(StorageClient::read_source(&db_info, 1));

        let cases = vec![
            (
                "SHOW TAG VALUES WITH KEY = 'state'",
                vec![],
                Some(TagKeyPredicate {
                    value: Some(Value::Eq("state".into())),
                }),
                None,
                vec![
                    TagValuesResponse {
                        measurement: "table_a".into(),
                        key: "state".into(),
                        values: vec!["MA".into()],
                    },
                    TagValuesResponse {
                        measurement: "table_b".into(),
                        key: "state".into(),
                        values: vec!["MA".into()],
                    },
                ],
            ),
            (
                "SHOW TAG VALUES FROM 'table_b' WITH KEY = 'state'",
                vec![LiteralOrRegex {
                    value: Some(RegexOrLiteralValue::LiteralValue("table_a".into())),
                }],
                Some(TagKeyPredicate {
                    value: Some(Value::Eq("state".into())),
                }),
                None,
                vec![TagValuesResponse {
                    measurement: "table_a".into(),
                    key: "state".into(),
                    values: vec!["MA".into()],
                }],
            ),
            (
                "SHOW TAG VALUES FROM /table.*/ WITH KEY = 'state'",
                vec![LiteralOrRegex {
                    value: Some(RegexOrLiteralValue::RegexValue("table.*".into())),
                }],
                Some(TagKeyPredicate {
                    value: Some(Value::Eq("state".into())),
                }),
                None,
                vec![
                    TagValuesResponse {
                        measurement: "table_a".into(),
                        key: "state".into(),
                        values: vec!["MA".into()],
                    },
                    TagValuesResponse {
                        measurement: "table_b".into(),
                        key: "state".into(),
                        values: vec!["MA".into()],
                    },
                ],
            ),
            (
                "SHOW TAG VALUES FROM /.*a$/ WITH KEY = 'state'",
                vec![LiteralOrRegex {
                    value: Some(RegexOrLiteralValue::RegexValue(".*a$".into())),
                }],
                Some(TagKeyPredicate {
                    value: Some(Value::Eq("state".into())),
                }),
                None,
                vec![TagValuesResponse {
                    measurement: "table_a".into(),
                    key: "state".into(),
                    values: vec!["MA".into()],
                }],
            ),
            (
                "SHOW TAG VALUES FROM /.*a$/ WITH KEY = 'state' WHERE state != 'MA'",
                vec![],
                Some(TagKeyPredicate {
                    value: Some(Value::Eq("state".into())),
                }),
                Some(make_state_neq_ma_predicate()),
                vec![],
            ),
            (
                "SHOW TAG VALUES FROM /.*a$/ WITH KEY = 'state' WHERE state >= 'MA'",
                vec![],
                Some(TagKeyPredicate {
                    value: Some(Value::Eq("state".into())),
                }),
                Some(make_state_geq_ma_predicate()),
                vec![
                    TagValuesResponse {
                        measurement: "table_a".into(),
                        key: "state".into(),
                        values: vec!["MA".into()],
                    },
                    TagValuesResponse {
                        measurement: "table_b".into(),
                        key: "state".into(),
                        values: vec!["MA".into()],
                    },
                ],
            ),
            (
                "SHOW TAG VALUES FROM 'table_b' WITH KEY != 'foo'",
                vec![LiteralOrRegex {
                    value: Some(RegexOrLiteralValue::LiteralValue("table_b".into())),
                }],
                Some(TagKeyPredicate {
                    value: Some(Value::Neq("foo".into())),
                }),
                None,
                vec![TagValuesResponse {
                    measurement: "table_b".into(),
                    key: "state".into(),
                    values: vec!["MA".into()],
                }],
            ),
            (
                "SHOW TAG VALUES WITH KEY =~ /sta.*/",
                vec![],
                Some(TagKeyPredicate {
                    value: Some(Value::EqRegex("sta.*".into())),
                }),
                Some(make_state_geq_ma_predicate()),
                vec![
                    TagValuesResponse {
                        measurement: "table_a".into(),
                        key: "state".into(),
                        values: vec!["MA".into()],
                    },
                    TagValuesResponse {
                        measurement: "table_b".into(),
                        key: "state".into(),
                        values: vec!["MA".into()],
                    },
                ],
            ),
            (
                "SHOW TAG VALUES FROM 'table_b' WITH KEY !~ /$ab/",
                vec![LiteralOrRegex {
                    value: Some(RegexOrLiteralValue::LiteralValue("table_b".into())),
                }],
                Some(TagKeyPredicate {
                    value: Some(Value::NeqRegex("$ab".into())),
                }),
                Some(make_state_geq_ma_predicate()),
                vec![TagValuesResponse {
                    measurement: "table_b".into(),
                    key: "state".into(),
                    values: vec!["MA".into()],
                }],
            ),
            (
                "SHOW TAG VALUES FROM 'table_a' WITH KEY in (\"state\", \"foo\")",
                vec![LiteralOrRegex {
                    value: Some(RegexOrLiteralValue::LiteralValue("table_a".into())),
                }],
                Some(TagKeyPredicate {
                    value: Some(Value::NeqRegex("$ab".into())),
                }),
                Some(make_state_geq_ma_predicate()),
                vec![TagValuesResponse {
                    measurement: "table_a".into(),
                    key: "state".into(),
                    values: vec!["MA".into()],
                }],
            ),
        ];

        for (
            description,
            measurement_patterns,
            tag_key_predicate,
            condition,
            expected_tag_values,
        ) in cases
        {
            let request = TagValuesGroupedByMeasurementAndTagKeyRequest {
                source: source.clone(),
                measurement_patterns,
                tag_key_predicate,
                condition,
            };

            let actual_tag_values = fixture
                .storage_client
                .tag_values_grouped_by_measurement_and_tag_key(request)
                .await
                .unwrap();

            assert_eq!(
                actual_tag_values, expected_tag_values,
                "{} failed",
                description
            );
        }
    }

    #[tokio::test]
    async fn test_storage_rpc_tag_values_grouped_by_measurement_and_tag_key_error() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();
        let chunk = TestChunk::new("my_table");

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

        // ---
        // test error
        // ---
        let request = TagValuesGroupedByMeasurementAndTagKeyRequest {
            source: source.clone(),
            measurement_patterns: vec![],
            tag_key_predicate: None,
            condition: None,
        };

        let response_string = fixture
            .storage_client
            .tag_values_grouped_by_measurement_and_tag_key(request)
            .await
            .unwrap_err()
            .to_string();

        assert_contains!(response_string, "Missing tag key predicate");
    }

    /// test the plumbing of the RPC layer for measurement_tag_values
    #[tokio::test]
    async fn test_storage_rpc_measurement_tag_values() {
        test_helpers::maybe_start_logging();
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        // Add a chunk with a field
        let chunk = TestChunk::new("TheMeasurement")
            .with_time_column()
            .with_tag_column("state")
            .with_one_row_of_data();

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

        let request = MeasurementTagValuesRequest {
            measurement: "TheMeasurement".into(),
            source: source.clone(),
            range: Some(make_timestamp_range(150, 2000)),
            predicate: Some(make_state_eq_ma_predicate()),
            tag_key: "state".into(),
        };

        let actual_tag_values = fixture
            .storage_client
            .measurement_tag_values(request)
            .await
            .unwrap();

        assert_eq!(
            actual_tag_values,
            vec!["MA"],
            "unexpected tag values while getting tag values",
        );

        grpc_request_metric_has_count(&fixture, "MeasurementTagValues", "ok", 1);
    }

    #[tokio::test]
    async fn test_storage_rpc_measurement_tag_values_error() {
        test_helpers::maybe_start_logging();
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        let chunk = TestChunk::new("my_table").with_error("Sugar we are going down");

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

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

        // Note we don't set the column_names on the test database, so we expect an
        // error
        let response_string = fixture
            .storage_client
            .measurement_tag_values(request)
            .await
            .unwrap_err()
            .to_string();

        assert_contains!(response_string, "Sugar we are going down");

        grpc_request_metric_has_count(&fixture, "MeasurementTagValues", "client_error", 1);
    }

    #[tokio::test]
    async fn test_log_on_panic() {
        // Send a message to a route that causes a panic and ensure:
        // 1. We don't use up all executors 2. The panic message
        // message ends up in the log system

        // Normally, the global panic logger is set at program start
        let _f = SendPanicsToTracing::new();

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
                assert_eq!(status.code(), tonic::Code::Unknown);
                assert_contains!(status.message(), "transport error");
            }
        };

        // Ensure that the logs captured the panic
        let captured_logs = tracing_capture.to_string();
        // Note we don't include the actual line / column in the
        // expected panic message to avoid needing to update the test
        // whenever the source code file changed.
        let expected_error =
            "panicked at 'This is a test panic', influxdb_iox/src/influxdb_ioxd/rpc/testing.rs";
        assert_contains!(captured_logs, expected_error);

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
        let caps = fixture.storage_client.capabilities().await.unwrap();
        assert!(!caps.is_empty(), "Caps: {:?}", caps);
    }

    #[tokio::test]
    async fn test_read_filter() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        // Add a chunk with a field
        let chunk = TestChunk::new("TheMeasurement")
            .with_time_column()
            .with_tag_column("state")
            .with_one_row_of_data();

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

        let request = ReadFilterRequest {
            read_source: source.clone(),
            range: Some(make_timestamp_range(0, 10000)),
            predicate: Some(make_state_eq_ma_predicate()),
            ..Default::default()
        };

        let frames = fixture.storage_client.read_filter(request).await.unwrap();

        // TODO: encode the actual output in the test case or something
        assert_eq!(
            frames.len(),
            0,
            "unexpected frames returned by query_series"
        );

        grpc_request_metric_has_count(&fixture, "ReadFilter", "ok", 1);
    }

    #[tokio::test]
    async fn test_read_filter_error() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        let chunk = TestChunk::new("my_table").with_error("Sugar we are going down");

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

        // ---
        // test error
        // ---
        let request = ReadFilterRequest {
            read_source: source.clone(),
            range: None,
            predicate: None,
            ..Default::default()
        };

        // Note we don't set the response on the test database, so we expect an error
        let response = fixture.storage_client.read_filter(request).await;
        assert_contains!(response.unwrap_err().to_string(), "Sugar we are going down");

        grpc_request_metric_has_count(&fixture, "ReadFilter", "client_error", 1);
    }

    #[tokio::test]
    async fn test_read_group() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        let chunk = TestChunk::new("TheMeasurement")
            .with_time_column()
            .with_i64_field_column("my field")
            .with_tag_column("state")
            .with_one_row_of_data();

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

        let group = generated_types::read_group_request::Group::By as i32;

        let request = ReadGroupRequest {
            read_source: source.clone(),
            range: Some(make_timestamp_range(0, 2000)),
            predicate: Some(make_state_eq_ma_predicate()),
            group_keys: vec!["state".into()],
            group,
            aggregate: Some(Aggregate {
                r#type: aggregate::AggregateType::Sum as i32,
            }),
        };

        let frames = fixture.storage_client.read_group(request).await.unwrap();

        // three frames:
        // GroupFrame
        // SeriesFrame (tag=state, field=my field)
        // DataFrame
        assert_eq!(frames.len(), 3);

        grpc_request_metric_has_count(&fixture, "ReadGroup", "ok", 1);
    }

    #[tokio::test]
    async fn test_read_group_error() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        let chunk = TestChunk::new("my_table").with_error("Sugar we are going down");

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

        let group = generated_types::read_group_request::Group::By as i32;

        // ---
        // test error returned in database processing
        // ---
        let request = ReadGroupRequest {
            read_source: source.clone(),
            range: None,
            predicate: None,
            group_keys: vec!["tag1".into()],
            group,
            aggregate: Some(Aggregate {
                r#type: aggregate::AggregateType::Sum as i32,
            }),
        };

        // Note we don't set the response on the test database, so we expect an error
        let response_string = fixture
            .storage_client
            .read_group(request)
            .await
            .unwrap_err()
            .to_string();
        assert_contains!(response_string, "Sugar we are going down");

        grpc_request_metric_has_count(&fixture, "ReadGroup", "client_error", 1);
    }

    #[tokio::test]
    async fn test_read_window_aggregate_window_every() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        // Add a chunk with a field
        let chunk = TestChunk::new("TheMeasurement")
            .with_time_column()
            .with_tag_column("state")
            .with_one_row_of_data();

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

        // -----
        // Test with window_every/offset setup
        // -----

        let request_window_every = ReadWindowAggregateRequest {
            read_source: source.clone(),
            range: Some(make_timestamp_range(0, 2000)),
            predicate: Some(make_state_eq_ma_predicate()),
            window_every: 1122,
            offset: 15,
            aggregate: vec![Aggregate {
                r#type: aggregate::AggregateType::Sum as i32,
            }],
            // old skool window definition
            window: None,
        };

        let frames = fixture
            .storage_client
            .read_window_aggregate(request_window_every)
            .await
            .unwrap();
        assert_eq!(
            frames.len(),
            0,
            "unexpected frames returned by query_groups"
        );

        grpc_request_metric_has_count(&fixture, "ReadWindowAggregate", "ok", 1);
    }

    #[tokio::test]
    async fn test_read_window_aggregate_every_offset() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        // Add a chunk with a field
        let chunk = TestChunk::new("TheMeasurement")
            .with_time_column()
            .with_tag_column("state")
            .with_one_row_of_data();

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

        // -----
        // Test with window.every and window.offset durations specified
        // -----

        let request_window = ReadWindowAggregateRequest {
            read_source: source.clone(),
            range: Some(make_timestamp_range(150, 200)),
            predicate: Some(make_state_eq_ma_predicate()),
            window_every: 0,
            offset: 0,
            aggregate: vec![Aggregate {
                r#type: aggregate::AggregateType::Sum as i32,
            }],
            // old skool window definition
            window: Some(Window {
                every: Some(Duration {
                    nsecs: 1122,
                    months: 0,
                    negative: false,
                }),
                offset: Some(Duration {
                    nsecs: 0,
                    months: 4,
                    negative: false,
                }),
            }),
        };

        let frames = fixture
            .storage_client
            .read_window_aggregate(request_window)
            .await
            .unwrap();

        assert_eq!(
            frames.len(),
            0,
            "unexpected frames returned by query_groups"
        )
    }

    #[tokio::test]
    async fn test_read_window_aggregate_error() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        let chunk = TestChunk::new("my_table").with_error("Sugar we are going down");

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

        // ---
        // test error
        // ---

        let request_window = ReadWindowAggregateRequest {
            read_source: source.clone(),
            range: Some(make_timestamp_range(0, 2000)),
            predicate: Some(make_state_eq_ma_predicate()),
            window_every: 1122,
            offset: 15,
            aggregate: vec![Aggregate {
                r#type: aggregate::AggregateType::Sum as i32,
            }],
            // old skool window definition
            window: None,
        };

        let response_string = fixture
            .storage_client
            .read_window_aggregate(request_window)
            .await
            .unwrap_err()
            .to_string();

        assert_contains!(response_string, "Sugar we are going down");

        grpc_request_metric_has_count(&fixture, "ReadWindowAggregate", "client_error", 1);
    }

    #[tokio::test]
    async fn test_measurement_fields() {
        test_helpers::maybe_start_logging();

        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        // Add a chunk with a field
        let chunk = TestChunk::new("TheMeasurement")
            .with_i64_field_column("Field1")
            .with_time_column()
            .with_tag_column("state")
            .with_one_row_of_data();

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

        let request = MeasurementFieldsRequest {
            source: source.clone(),
            measurement: "TheMeasurement".into(),
            range: Some(make_timestamp_range(0, 2000)),
            predicate: Some(make_state_eq_ma_predicate()),
        };

        let actual_fields = fixture
            .storage_client
            .measurement_fields(request)
            .await
            .unwrap();
        let expected_fields: Vec<String> = vec!["key: Field1, type: 1, timestamp: 1000".into()];

        assert_eq!(
            actual_fields, expected_fields,
            "unexpected frames returned by measurement_fields"
        );

        grpc_request_metric_has_count(&fixture, "MeasurementFields", "ok", 1);
    }

    #[tokio::test]
    async fn test_measurement_fields_error() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        let chunk = TestChunk::new("t").with_error("Sugar we are going down");

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

        // ---
        // test error
        // ---
        let request = MeasurementFieldsRequest {
            source: source.clone(),
            measurement: "TheMeasurement".into(),
            range: None,
            predicate: None,
        };

        let response_string = fixture
            .storage_client
            .measurement_fields(request)
            .await
            .unwrap_err()
            .to_string();
        assert_contains!(response_string, "Sugar we are going down");
    }

    fn make_timestamp_range(start: i64, end: i64) -> TimestampRange {
        TimestampRange { start, end }
    }

    /// return a gRPC predicate like
    ///
    /// state="MA"
    fn make_state_eq_ma_predicate() -> Predicate {
        make_state_predicate(node::Comparison::Equal)
    }

    /// return a gRPC predicate like
    ///
    /// state != "MA"
    fn make_state_neq_ma_predicate() -> Predicate {
        make_state_predicate(node::Comparison::NotEqual)
    }

    /// return a gRPC predicate like
    ///
    /// state >= "MA"
    fn make_state_geq_ma_predicate() -> Predicate {
        make_state_predicate(node::Comparison::Gte)
    }

    fn make_state_predicate(op: node::Comparison) -> Predicate {
        use node::{Type, Value};
        let root = Node {
            node_type: Type::ComparisonExpression as i32,
            value: Some(Value::Comparison(op as i32)),
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
        Predicate { root: Some(root) }
    }

    /// return an DataFusion Expr predicate like
    ///
    /// state="MA"
    fn make_state_ma_expr() -> Expr {
        col("state").eq(lit("MA"))
    }

    /// Convert to a Vec<String> to facilitate comparison with results of client
    fn to_string_vec(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
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
        iox_client: IOxTestingClient<Connection>,
        storage_client: StorageClient,
        test_storage: Arc<TestDatabaseStore>,
    }

    impl Fixture {
        /// Start up a test storage server listening on `port`, returning
        /// a fixture with the test server and clients
        async fn new() -> Result<Self, FixtureError> {
            let test_storage = Arc::new(TestDatabaseStore::new());

            // Get a random port from the kernel by asking for port 0.
            let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
            let socket = tokio::net::TcpListener::bind(bind_addr)
                .await
                .context(BindSnafu)?;

            // Pull the assigned port out of the socket
            let bind_addr = socket.local_addr().unwrap();

            println!(
                "Starting InfluxDB IOx storage test server on {:?}",
                bind_addr
            );

            let trace_header_parser = trace_http::ctx::TraceHeaderParser::new();

            let router = tonic::transport::Server::builder()
                .layer(trace_http::tower::TraceLayer::new(
                    trace_header_parser,
                    Arc::clone(&test_storage.metric_registry),
                    None,
                    true,
                ))
                .add_service(crate::influxdb_ioxd::rpc::testing::make_server())
                .add_service(
                    crate::influxdb_ioxd::server_type::database::rpc::storage::make_server(
                        Arc::clone(&test_storage),
                    ),
                );

            let server = async move {
                let stream = TcpListenerStream::new(socket);

                router
                    .serve_with_incoming(stream)
                    .await
                    .log_if_error("Running Tonic Server")
            };

            tokio::task::spawn(server);

            let conn = ConnectionBuilder::default()
                .connect_timeout(std::time::Duration::from_secs(30))
                .build(format!("http://{}", bind_addr))
                .await
                .unwrap();

            let iox_client = IOxTestingClient::new(conn.clone());

            let storage_client = StorageClient::new(conn);

            Ok(Self {
                iox_client,
                storage_client,
                test_storage,
            })
        }
    }

    #[derive(Debug)]
    pub struct TestDatabaseStore {
        databases: Mutex<BTreeMap<String, Arc<TestDatabase>>>,
        executor: Arc<Executor>,
        pub metric_registry: Arc<metric::Registry>,
    }

    impl TestDatabaseStore {
        pub fn new() -> Self {
            Self::default()
        }
    }

    impl Default for TestDatabaseStore {
        fn default() -> Self {
            Self {
                databases: Mutex::new(BTreeMap::new()),
                executor: Arc::new(Executor::new(1)),
                metric_registry: Default::default(),
            }
        }
    }

    #[tonic::async_trait]
    impl DatabaseStore for TestDatabaseStore {
        type Database = TestDatabase;
        type Error = TestError;

        /// Retrieve the database specified name
        fn db(&self, name: &str) -> Option<Arc<Self::Database>> {
            let databases = self.databases.lock();

            databases.get(name).cloned()
        }

        /// Retrieve the database specified by name, creating it if it
        /// doesn't exist.
        async fn db_or_create(&self, name: &str) -> Result<Arc<Self::Database>, Self::Error> {
            let mut databases = self.databases.lock();

            if let Some(db) = databases.get(name) {
                Ok(Arc::clone(db))
            } else {
                let new_db = Arc::new(TestDatabase::new(Arc::clone(&self.executor)));
                databases.insert(name.to_string(), Arc::clone(&new_db));
                Ok(new_db)
            }
        }
    }
}
