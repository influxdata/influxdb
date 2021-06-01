//! This module contains implementations for the storage gRPC service
//! implemented in terms of the `query::Database` and
//! `query::DatabaseStore`

use crate::influxdb_ioxd::{
    planner::Planner,
    rpc::storage::{
        data::{
            fieldlist_to_measurement_fields_response, series_set_item_to_read_response,
            tag_keys_to_byte_vecs,
        },
        expr::{self, AddRpcNode, GroupByAndAggregate, Loggable, SpecialTagKeys},
        input::GrpcInputs,
        StorageService,
    },
};
use data_types::{error::ErrorLogger, names::org_and_bucket_to_database, DatabaseName};
use generated_types::{
    google::protobuf::Empty, storage_server::Storage, CapabilitiesResponse, Capability,
    Int64ValuesResponse, MeasurementFieldsRequest, MeasurementFieldsResponse,
    MeasurementNamesRequest, MeasurementTagKeysRequest, MeasurementTagValuesRequest, Predicate,
    ReadFilterRequest, ReadGroupRequest, ReadResponse, ReadSeriesCardinalityRequest,
    ReadWindowAggregateRequest, StringValuesResponse, TagKeysRequest, TagValuesRequest,
    TimestampRange,
};
use metrics::KeyValue;
use observability_deps::tracing::{error, info};
use query::{
    exec::fieldlist::FieldList, exec::seriesset::Error as SeriesSetError,
    predicate::PredicateBuilder, DatabaseStore,
};
use snafu::{OptionExt, ResultExt, Snafu};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

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

    #[snafu(display("Error computing series: {}", source))]
    ComputingSeriesSet { source: SeriesSetError },

    #[snafu(display("Error converting tag_key to UTF-8 in tag_values request, tag_key value '{}': {}", String::from_utf8_lossy(source.as_bytes()), source))]
    ConvertingTagKeyInTagValues { source: std::string::FromUtf8Error },

    #[snafu(display("Error computing groups series: {}", source))]
    ComputingGroupedSeriesSet { source: SeriesSetError },

    #[snafu(display("Error converting time series into gRPC response:  {}", source))]
    ConvertingSeriesSet { source: super::data::Error },

    #[snafu(display("Converting field information series into gRPC response:  {}", source))]
    ConvertingFieldList { source: super::data::Error },

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
            Self::ComputingSeriesSet { .. } => Status::invalid_argument(self.to_string()),
            Self::ConvertingTagKeyInTagValues { .. } => Status::invalid_argument(self.to_string()),
            Self::ComputingGroupedSeriesSet { .. } => Status::invalid_argument(self.to_string()),
            Self::ConvertingSeriesSet { .. } => Status::invalid_argument(self.to_string()),
            Self::ConvertingFieldList { .. } => Status::invalid_argument(self.to_string()),
            Self::SendingResults { .. } => Status::internal(self.to_string()),
            Self::InternalHintsFieldNotSupported { .. } => Status::internal(self.to_string()),
            Self::NotYetImplemented { .. } => Status::internal(self.to_string()),
        }
    }

    fn is_internal(&self) -> bool {
        matches!(self.to_status().code(), tonic::Code::Internal)
    }
}

/// Implementes the protobuf defined Storage service for a DatabaseStore
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
        let read_filter_request = req.into_inner();

        let db_name = get_database_name(&read_filter_request)?;

        let ReadFilterRequest {
            read_source: _read_source,
            range,
            predicate,
        } = read_filter_request;

        info!(%db_name, ?range, predicate=%predicate.loggable(),"read filter");

        let ob = self.metrics.requests.observation();
        let labels = &[
            KeyValue::new("operation", "read_filter"),
            KeyValue::new("db_name", db_name.to_string()),
        ];

        let results = read_filter_impl(Arc::clone(&self.db_store), db_name, range, predicate)
            .await
            .map_err(|e| {
                if e.is_internal() {
                    ob.error_with_labels(labels);
                } else {
                    ob.client_error_with_labels(labels);
                }
                e.to_status()
            })?
            .into_iter()
            .map(Ok)
            .collect::<Vec<_>>();

        ob.ok_with_labels(labels);
        Ok(tonic::Response::new(futures::stream::iter(results)))
    }

    type ReadGroupStream = futures::stream::Iter<std::vec::IntoIter<Result<ReadResponse, Status>>>;

    async fn read_group(
        &self,
        req: tonic::Request<ReadGroupRequest>,
    ) -> Result<tonic::Response<Self::ReadGroupStream>, Status> {
        let read_group_request = req.into_inner();

        let db_name = get_database_name(&read_group_request)?;

        let ReadGroupRequest {
            read_source: _read_source,
            range,
            predicate,
            group_keys,
            group,
            aggregate,
            hints,
        } = read_group_request;

        info!(%db_name, ?range, ?group_keys, ?group, ?aggregate,predicate=%predicate.loggable(),"read_group");

        let ob = self.metrics.requests.observation();
        let labels = &[
            KeyValue::new("operation", "read_group"),
            KeyValue::new("db_name", db_name.to_string()),
        ];

        if hints != 0 {
            ob.error_with_labels(labels);
            InternalHintsFieldNotSupported { hints }.fail()?
        }

        let aggregate_string = format!(
            "aggregate: {:?}, group: {:?}, group_keys: {:?}",
            aggregate, group, group_keys
        );

        let group = expr::convert_group_type(group)
            .context(ConvertingReadGroupType {
                aggregate_string: &aggregate_string,
            })
            .map_err(|e| {
                ob.client_error_with_labels(labels);
                e
            })?;

        let gby_agg = expr::make_read_group_aggregate(aggregate, group, group_keys)
            .context(ConvertingReadGroupAggregate { aggregate_string })
            .map_err(|e| {
                ob.client_error_with_labels(labels);
                e
            })?;

        let results = query_group_impl(
            Arc::clone(&self.db_store),
            db_name,
            range,
            predicate,
            gby_agg,
        )
        .await
        .map_err(|e| {
            if e.is_internal() {
                ob.error_with_labels(labels);
            } else {
                ob.client_error_with_labels(labels);
            }
            e.to_status()
        })?
        .into_iter()
        .map(Ok)
        .collect::<Vec<_>>();

        ob.ok_with_labels(labels);
        Ok(tonic::Response::new(futures::stream::iter(results)))
    }

    type ReadWindowAggregateStream =
        futures::stream::Iter<std::vec::IntoIter<Result<ReadResponse, Status>>>;

    async fn read_window_aggregate(
        &self,
        req: tonic::Request<ReadWindowAggregateRequest>,
    ) -> Result<tonic::Response<Self::ReadGroupStream>, Status> {
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

        info!(%db_name, ?range, ?window_every, ?offset, ?aggregate, ?window, predicate=%predicate.loggable(),"read_window_aggregate");

        let ob = self.metrics.requests.observation();
        let labels = &[
            KeyValue::new("operation", "read_window_aggregate"),
            KeyValue::new("db_name", db_name.to_string()),
        ];

        let aggregate_string = format!(
            "aggregate: {:?}, window_every: {:?}, offset: {:?}, window: {:?}",
            aggregate, window_every, offset, window
        );

        let gby_agg = expr::make_read_window_aggregate(aggregate, window_every, offset, window)
            .context(ConvertingWindowAggregate { aggregate_string })
            .map_err(|e| {
                ob.client_error_with_labels(labels);
                e
            })?;

        let results = query_group_impl(
            Arc::clone(&self.db_store),
            db_name,
            range,
            predicate,
            gby_agg,
        )
        .await
        .map_err(|e| {
            if e.is_internal() {
                ob.error_with_labels(labels);
            } else {
                ob.client_error_with_labels(labels);
            }
            e.to_status()
        })?
        .into_iter()
        .map(Ok)
        .collect::<Vec<_>>();

        ob.ok_with_labels(labels);
        Ok(tonic::Response::new(futures::stream::iter(results)))
    }

    type TagKeysStream = ReceiverStream<Result<StringValuesResponse, Status>>;

    async fn tag_keys(
        &self,
        req: tonic::Request<TagKeysRequest>,
    ) -> Result<tonic::Response<Self::TagKeysStream>, Status> {
        let (tx, rx) = mpsc::channel(4);

        let tag_keys_request = req.into_inner();

        let db_name = get_database_name(&tag_keys_request)?;

        let TagKeysRequest {
            tags_source: _tag_source,
            range,
            predicate,
        } = tag_keys_request;

        info!(%db_name, ?range, predicate=%predicate.loggable(), "tag_keys");

        let ob = self.metrics.requests.observation();
        let labels = &[
            KeyValue::new("operation", "tag_keys"),
            KeyValue::new("db_name", db_name.to_string()),
        ];

        let measurement = None;

        let response = tag_keys_impl(
            Arc::clone(&self.db_store),
            db_name,
            measurement,
            range,
            predicate,
        )
        .await
        .map_err(|e| {
            if e.is_internal() {
                ob.error_with_labels(labels);
            } else {
                ob.client_error_with_labels(labels);
            }
            e.to_status()
        });

        tx.send(response)
            .await
            .expect("sending tag_keys response to server");

        ob.ok_with_labels(labels);
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    type TagValuesStream = ReceiverStream<Result<StringValuesResponse, Status>>;

    async fn tag_values(
        &self,
        req: tonic::Request<TagValuesRequest>,
    ) -> Result<tonic::Response<Self::TagValuesStream>, Status> {
        let (tx, rx) = mpsc::channel(4);

        let tag_values_request = req.into_inner();

        let db_name = get_database_name(&tag_values_request)?;

        let ob = self.metrics.requests.observation();
        let labels = &[
            KeyValue::new("operation", "tag_values"),
            KeyValue::new("db_name", db_name.to_string()),
        ];

        let TagValuesRequest {
            tags_source: _tag_source,
            range,
            predicate,
            tag_key,
        } = tag_values_request;

        let measurement = None;

        // Special case a request for 'tag_key=_measurement" means to list all
        // measurements
        let response = if tag_key.is_measurement() {
            info!(%db_name, ?range, predicate=%predicate.loggable(), "tag_values with tag_key=[x00] (measurement name)");

            if predicate.is_some() {
                ob.client_error_with_labels(labels);
                return Err(Error::NotYetImplemented {
                    operation: "tag_value for a measurement, with general predicate".to_string(),
                }
                .to_status());
            }

            measurement_name_impl(Arc::clone(&self.db_store), db_name, range)
                .await
                .map_err(|e| {
                    if e.is_internal() {
                        ob.error_with_labels(labels);
                    } else {
                        ob.client_error_with_labels(labels);
                    }
                    e
                })
        } else if tag_key.is_field() {
            info!(%db_name, ?range, predicate=%predicate.loggable(), "tag_values with tag_key=[xff] (field name)");

            let fieldlist =
                field_names_impl(Arc::clone(&self.db_store), db_name, None, range, predicate)
                    .await
                    .map_err(|e| {
                        if e.is_internal() {
                            ob.error_with_labels(labels);
                        } else {
                            ob.client_error_with_labels(labels);
                        }
                        e
                    })?;

            // Pick out the field names into a Vec<Vec<u8>>for return
            let values = fieldlist
                .fields
                .into_iter()
                .map(|f| f.name.bytes().collect())
                .collect::<Vec<_>>();

            Ok(StringValuesResponse { values })
        } else {
            let tag_key = String::from_utf8(tag_key)
                .context(ConvertingTagKeyInTagValues)
                .map_err(|e| {
                    ob.client_error_with_labels(labels);
                    e
                })?;

            info!(%db_name, ?range, %tag_key, predicate=%predicate.loggable(), "tag_values",);

            tag_values_impl(
                Arc::clone(&self.db_store),
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

        ob.ok_with_labels(labels);
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    type ReadSeriesCardinalityStream = ReceiverStream<Result<Int64ValuesResponse, Status>>;

    async fn read_series_cardinality(
        &self,
        _req: tonic::Request<ReadSeriesCardinalityRequest>,
    ) -> Result<tonic::Response<Self::ReadSeriesCardinalityStream>, Status> {
        unimplemented!("read_series_cardinality not yet implemented");
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
        let (tx, rx) = mpsc::channel(4);

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

        info!(%db_name, ?range, predicate=%predicate.loggable(), "measurement_names");

        let ob = self.metrics.requests.observation();
        let labels = &[
            KeyValue::new("operation", "measurement_names"),
            KeyValue::new("db_name", db_name.to_string()),
        ];

        let response = measurement_name_impl(Arc::clone(&self.db_store), db_name, range)
            .await
            .map_err(|e| {
                if e.is_internal() {
                    ob.error_with_labels(labels);
                } else {
                    ob.client_error_with_labels(labels);
                }
                e.to_status()
            });

        tx.send(response)
            .await
            .expect("sending measurement names response to server");

        ob.ok_with_labels(labels);
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    type MeasurementTagKeysStream = ReceiverStream<Result<StringValuesResponse, Status>>;

    async fn measurement_tag_keys(
        &self,
        req: tonic::Request<MeasurementTagKeysRequest>,
    ) -> Result<tonic::Response<Self::MeasurementTagKeysStream>, Status> {
        let (tx, rx) = mpsc::channel(4);

        let measurement_tag_keys_request = req.into_inner();

        let db_name = get_database_name(&measurement_tag_keys_request)?;

        let MeasurementTagKeysRequest {
            source: _source,
            measurement,
            range,
            predicate,
        } = measurement_tag_keys_request;

        info!(%db_name, ?range, %measurement, predicate=%predicate.loggable(), "measurement_tag_keys");

        let ob = self.metrics.requests.observation();
        let labels = &[
            KeyValue::new("operation", "measurement_tag_keys"),
            KeyValue::new("db_name", db_name.to_string()),
        ];

        let measurement = Some(measurement);

        let response = tag_keys_impl(
            Arc::clone(&self.db_store),
            db_name,
            measurement,
            range,
            predicate,
        )
        .await
        .map_err(|e| {
            if e.is_internal() {
                ob.error_with_labels(labels);
            } else {
                ob.client_error_with_labels(labels);
            }
            e.to_status()
        });

        tx.send(response)
            .await
            .expect("sending measurement_tag_keys response to server");

        ob.ok_with_labels(labels);
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    type MeasurementTagValuesStream = ReceiverStream<Result<StringValuesResponse, Status>>;

    async fn measurement_tag_values(
        &self,
        req: tonic::Request<MeasurementTagValuesRequest>,
    ) -> Result<tonic::Response<Self::MeasurementTagValuesStream>, Status> {
        let (tx, rx) = mpsc::channel(4);

        let measurement_tag_values_request = req.into_inner();

        let db_name = get_database_name(&measurement_tag_values_request)?;

        let MeasurementTagValuesRequest {
            source: _source,
            measurement,
            range,
            predicate,
            tag_key,
        } = measurement_tag_values_request;

        info!(%db_name, ?range, %measurement, %tag_key, predicate=%predicate.loggable(), "measurement_tag_values");

        let ob = self.metrics.requests.observation();
        let labels = &[
            KeyValue::new("operation", "measurement_tag_values"),
            KeyValue::new("db_name", db_name.to_string()),
        ];

        let measurement = Some(measurement);

        let response = tag_values_impl(
            Arc::clone(&self.db_store),
            db_name,
            tag_key,
            measurement,
            range,
            predicate,
        )
        .await
        .map_err(|e| {
            if e.is_internal() {
                ob.error_with_labels(labels);
            } else {
                ob.client_error_with_labels(labels);
            }
            e.to_status()
        });

        tx.send(response)
            .await
            .expect("sending measurement_tag_values response to server");

        ob.ok_with_labels(labels);
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    type MeasurementFieldsStream = ReceiverStream<Result<MeasurementFieldsResponse, Status>>;

    async fn measurement_fields(
        &self,
        req: tonic::Request<MeasurementFieldsRequest>,
    ) -> Result<tonic::Response<Self::MeasurementFieldsStream>, Status> {
        let (tx, rx) = mpsc::channel(4);

        let measurement_fields_request = req.into_inner();

        let db_name = get_database_name(&measurement_fields_request)?;

        let MeasurementFieldsRequest {
            source: _source,
            measurement,
            range,
            predicate,
        } = measurement_fields_request;

        info!(%db_name, ?range, predicate=%predicate.loggable(), "measurement_fields");

        let ob = self.metrics.requests.observation();
        let labels = &[
            KeyValue::new("operation", "measurement_fields"),
            KeyValue::new("db_name", db_name.to_string()),
        ];

        let measurement = Some(measurement);

        let response = field_names_impl(
            Arc::clone(&self.db_store),
            db_name,
            measurement,
            range,
            predicate,
        )
        .await
        .map(|fieldlist| {
            fieldlist_to_measurement_fields_response(fieldlist)
                .context(ConvertingFieldList)
                .map_err(|e| e.to_status())
        })
        .map_err(|e| {
            if e.is_internal() {
                ob.error_with_labels(labels);
            } else {
                ob.client_error_with_labels(labels);
            }
            e.to_status()
        })?;

        tx.send(response)
            .await
            .expect("sending measurement_fields response to server");

        ob.ok_with_labels(labels);
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
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
// can use ?, etc). The trait implemententations then handle mapping
// to the appropriate tonic Status

/// Gathers all measurement names that have data in the specified
/// (optional) range
async fn measurement_name_impl<T>(
    db_store: Arc<T>,
    db_name: DatabaseName<'static>,
    range: Option<TimestampRange>,
) -> Result<StringValuesResponse>
where
    T: DatabaseStore + 'static,
{
    let predicate = PredicateBuilder::default().set_range(range).build();
    let db_name = db_name.as_ref();

    let db = db_store
        .db(&db_name)
        .context(DatabaseNotFound { db_name })?;
    let executor = db_store.executor();

    let plan = Planner::new(Arc::clone(&executor))
        .table_names(db, predicate)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ListingTables { db_name })?;

    let table_names = executor
        .to_string_set(plan)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ListingTables { db_name })?;

    // Map the resulting collection of Strings into a Vec<Vec<u8>>for return
    let values: Vec<Vec<u8>> = table_names
        .iter()
        .map(|name| name.bytes().collect())
        .collect();

    Ok(StringValuesResponse { values })
}

/// Return tag keys with optional measurement, timestamp and arbitratry
/// predicates
async fn tag_keys_impl<T>(
    db_store: Arc<T>,
    db_name: DatabaseName<'static>,
    measurement: Option<String>,
    range: Option<TimestampRange>,
    rpc_predicate: Option<Predicate>,
) -> Result<StringValuesResponse>
where
    T: DatabaseStore + 'static,
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

    let db = db_store.db(&db_name).context(DatabaseNotFound {
        db_name: db_name.as_str(),
    })?;

    let executor = db_store.executor();

    let tag_key_plan = Planner::new(Arc::clone(&executor))
        .tag_keys(db, predicate)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ListingColumns {
            db_name: db_name.as_str(),
        })?;

    let tag_keys = executor
        .to_string_set(tag_key_plan)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ListingColumns {
            db_name: db_name.as_str(),
        })?;

    // Map the resulting collection of Strings into a Vec<Vec<u8>>for return
    let values = tag_keys_to_byte_vecs(tag_keys);
    // Debugging help: uncomment this out to see what is coming back
    // info!("Returning tag keys");
    // values.iter().for_each(|k| info!("  {}", String::from_utf8_lossy(k)));

    Ok(StringValuesResponse { values })
}

/// Return tag values for tag_name, with optional measurement, timestamp and
/// arbitratry predicates
async fn tag_values_impl<T>(
    db_store: Arc<T>,
    db_name: DatabaseName<'static>,
    tag_name: String,
    measurement: Option<String>,
    range: Option<TimestampRange>,
    rpc_predicate: Option<Predicate>,
) -> Result<StringValuesResponse>
where
    T: DatabaseStore + 'static,
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

    let db_name = db_name.as_str();
    let tag_name = &tag_name;

    let db = db_store.db(db_name).context(DatabaseNotFound { db_name })?;
    let executor = db_store.executor();

    let tag_value_plan = Planner::new(Arc::clone(&executor))
        .tag_values(db, tag_name, predicate)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ListingTagValues { db_name, tag_name })?;

    let tag_values = executor
        .to_string_set(tag_value_plan)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ListingTagValues { db_name, tag_name })?;

    // Map the resulting collection of Strings into a Vec<Vec<u8>>for return
    let values: Vec<Vec<u8>> = tag_values
        .iter()
        .map(|name| name.bytes().collect())
        .collect();

    // Debugging help: uncomment to see raw values coming back
    //info!("Returning tag values");
    //values.iter().for_each(|k| info!("  {}", String::from_utf8_lossy(k)));

    Ok(StringValuesResponse { values })
}

/// Launch async tasks that materialises the result of executing read_filter.
async fn read_filter_impl<'a, T>(
    db_store: Arc<T>,
    db_name: DatabaseName<'static>,
    range: Option<TimestampRange>,
    rpc_predicate: Option<Predicate>,
) -> Result<Vec<ReadResponse>, Error>
where
    T: DatabaseStore + 'static,
{
    let rpc_predicate_string = format!("{:?}", rpc_predicate);

    let predicate = PredicateBuilder::default()
        .set_range(range)
        .rpc_predicate(rpc_predicate)
        .context(ConvertingPredicate {
            rpc_predicate_string,
        })?
        .build();

    // keep original name so we can transfer ownership
    // to closure below
    let owned_db_name = db_name;

    let db_name = owned_db_name.as_str();
    let db = db_store.db(db_name).context(DatabaseNotFound { db_name })?;
    let executor = db_store.executor();

    // PERF - This used to send responses to the client before execution had
    // completed, but now it doesn't. We may need to revisit this in the future
    // if big queries are causing a significant latency in TTFB.

    // Build the plans
    let series_plan = Planner::new(Arc::clone(&executor))
        .read_filter(db, predicate)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(PlanningFilteringSeries { db_name })?;

    // Execute the plans.
    let ss_items = executor
        .to_series_set(series_plan)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(FilteringSeries {
            db_name: owned_db_name.as_str(),
        })
        .log_if_error("Running series set plan")?;

    // Convert results into API responses
    ss_items
        .into_iter()
        .map(|series_set| series_set_item_to_read_response(series_set).context(ConvertingSeriesSet))
        .collect::<Result<Vec<ReadResponse>, Error>>()
}

/// Launch async tasks that send the result of executing read_group to `tx`
async fn query_group_impl<T>(
    db_store: Arc<T>,
    db_name: DatabaseName<'static>,
    range: Option<TimestampRange>,
    rpc_predicate: Option<Predicate>,
    gby_agg: GroupByAndAggregate,
) -> Result<Vec<ReadResponse>, Error>
where
    T: DatabaseStore + 'static,
{
    let rpc_predicate_string = format!("{:?}", rpc_predicate);

    let predicate = PredicateBuilder::default()
        .set_range(range)
        .rpc_predicate(rpc_predicate)
        .context(ConvertingPredicate {
            rpc_predicate_string,
        })?
        .build();

    // keep original name so we can transfer ownership
    // to closure below
    let owned_db_name = db_name;
    let db_name = owned_db_name.as_str();

    let db = db_store
        .db(&db_name)
        .context(DatabaseNotFound { db_name })?;
    let executor = db_store.executor();

    let planner = Planner::new(Arc::clone(&executor));
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
        .context(PlanningGroupSeries { db_name })?;

    // PERF - This used to send responses to the client before execution had
    // completed, but now it doesn't. We may need to revisit this in the future
    // if big queries are causing a significant latency in TTFB.

    // Execute the plans
    let ss_items = executor
        .to_series_set(grouped_series_set_plan)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(GroupingSeries {
            db_name: owned_db_name.as_str(),
        })
        .log_if_error("Running Grouped SeriesSet Plan")?;

    // Convert plans to API responses
    ss_items
        .into_iter()
        .map(|series_set| series_set_item_to_read_response(series_set).context(ConvertingSeriesSet))
        .collect::<Result<Vec<ReadResponse>, Error>>()
}

/// Return field names, restricted via optional measurement, timestamp and
/// predicate
async fn field_names_impl<T>(
    db_store: Arc<T>,
    db_name: DatabaseName<'static>,
    measurement: Option<String>,
    range: Option<TimestampRange>,
    rpc_predicate: Option<Predicate>,
) -> Result<FieldList>
where
    T: DatabaseStore + 'static,
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

    let db_name = db_name.as_str();
    let db = db_store.db(db_name).context(DatabaseNotFound { db_name })?;
    let executor = db_store.executor();

    let field_list_plan = Planner::new(Arc::clone(&executor))
        .field_columns(db, predicate)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ListingFields { db_name })?;

    let field_list = executor
        .to_field_list(field_list_plan)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ListingFields { db_name })?;

    Ok(field_list)
}

#[cfg(test)]
mod tests {
    use super::super::super::ServingReadinessInterceptor;
    use super::super::id::Id;

    use super::*;
    use datafusion::logical_plan::{col, lit, Expr};
    use panic_logging::SendPanicsToTracing;
    use query::{test::TestChunk, test::TestDatabaseStore};
    use std::{
        convert::TryFrom,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::Duration,
    };
    use test_helpers::{assert_contains, tag_key_bytes_to_strings, tracing::TracingCapture};

    use tonic::Code;

    use futures::prelude::*;

    use generated_types::{
        aggregate::AggregateType, i_ox_testing_client, node, read_response::frame, storage_client,
        Aggregate as RPCAggregate, Duration as RPCDuration, Node, ReadSource, TestErrorRequest,
        Window as RPCWindow,
    };

    use crate::influxdb_ioxd::serving_readiness::ServingReadinessState;
    use generated_types::google::protobuf::Any;
    use prost::Message;
    use tokio_stream::wrappers::TcpListenerStream;

    type IOxTestingClient = i_ox_testing_client::IOxTestingClient<tonic::transport::Channel>;
    type StorageClient = storage_client::StorageClient<tonic::transport::Channel>;

    fn to_str_vec(s: &[&str]) -> Vec<String> {
        s.iter().map(|s| s.to_string()).collect()
    }

    // Helper function to assert that metric tracking all gRPC requests has
    // correctly updated.
    fn grpc_request_metric_has_count(
        fixture: &Fixture,
        operation: &'static str,
        status: &'static str,
        count: usize,
    ) -> Result<(), metrics::Error> {
        fixture
            .test_storage
            .metrics_registry
            .has_metric_family("gRPC_requests_total")
            .with_labels(&[
                ("operation", operation),
                ("db_name", "000000000000007b_00000000000001c8"),
                ("status", status),
            ])
            .counter()
            .eq(count as f64)
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

    #[tokio::test]
    async fn test_storage_rpc_measurement_names() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let chunk0 = TestChunk::new(0).with_table("h2o");
        let chunk1 = TestChunk::new(1).with_table("o2");

        fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk0))
            .add_chunk("my_partition_key", Arc::new(chunk1));

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
            .db_or_create(&db_info.db_name)
            .await
            .expect("getting db")
            .get_chunk("my_partition_key", 0)
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

        grpc_request_metric_has_count(&fixture, "measurement_names", "ok", 2).unwrap();
    }

    /// test the plumbing of the RPC layer for tag_keys -- specifically that
    /// the right parameters are passed into the Database interface
    /// and that the returned values are sent back via gRPC.
    #[tokio::test]
    async fn test_storage_rpc_tag_keys() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        // Note multiple tables / measureemnts:
        let chunk0 = TestChunk::new(0)
            .with_tag_column("m1", "k1")
            .with_tag_column("m1", "k2");

        let chunk1 = TestChunk::new(1)
            .with_tag_column("m2", "k3")
            .with_tag_column("m2", "k4");

        fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk0))
            .add_chunk("my_partition_key", Arc::new(chunk1));

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        let request = TagKeysRequest {
            tags_source: source.clone(),
            range: Some(make_timestamp_range(150, 200)),
            predicate: Some(make_state_ma_predicate()),
        };

        let actual_tag_keys = fixture.storage_client.tag_keys(request).await.unwrap();
        let expected_tag_keys = vec!["_f(0xff)", "_m(0x00)", "k1", "k2", "k3", "k4"];

        assert_eq!(actual_tag_keys, expected_tag_keys,);

        // also ensure the plumbing is hooked correctly and that the predicate made it
        // down to the chunk
        let actual_predicates = fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .expect("getting db")
            .get_chunk("my_partition_key", 0)
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

        grpc_request_metric_has_count(&fixture, "tag_keys", "ok", 1).unwrap();
    }

    #[tokio::test]
    async fn test_storage_rpc_tag_keys_error() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let chunk = TestChunk::new(0).with_error("Sugar we are going down");

        fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

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

        grpc_request_metric_has_count(&fixture, "tag_keys", "client_error", 1).unwrap();
    }

    /// test the plumbing of the RPC layer for measurement_tag_keys--
    /// specifically that the right parameters are passed into the Database
    /// interface and that the returned values are sent back via gRPC.
    #[tokio::test]
    async fn test_storage_rpc_measurement_tag_keys() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let chunk0 = TestChunk::new(0)
            // predicate specifies m4, so this is filtered out
            .with_tag_column("m1", "k0");

        let chunk1 = TestChunk::new(1)
            .with_tag_column("m4", "k1")
            .with_tag_column("m4", "k2")
            .with_tag_column("m4", "k3")
            .with_tag_column("m4", "k4");

        fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk0))
            .add_chunk("my_partition_key", Arc::new(chunk1));

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        // ---
        // Timestamp + Predicate
        // ---
        let request = MeasurementTagKeysRequest {
            measurement: "m4".into(),
            source: source.clone(),
            range: Some(make_timestamp_range(150, 200)),
            predicate: Some(make_state_ma_predicate()),
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
            .db_or_create(&db_info.db_name)
            .await
            .expect("getting db")
            .get_chunk("my_partition_key", 0)
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

        grpc_request_metric_has_count(&fixture, "measurement_tag_keys", "ok", 1).unwrap();
    }

    #[tokio::test]
    async fn test_storage_rpc_measurement_tag_keys_error() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let chunk = TestChunk::new(0)
            // predicate specifies m4, so this is filtered out
            .with_error("This is an error");

        fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

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

        grpc_request_metric_has_count(&fixture, "measurement_tag_keys", "client_error", 1).unwrap();
    }

    /// test the plumbing of the RPC layer for tag_keys -- specifically that
    /// the right parameters are passed into the Database interface
    /// and that the returned values are sent back via gRPC.
    #[tokio::test]
    async fn test_storage_rpc_tag_values() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        // Add a chunk with a field
        let chunk = TestChunk::new(0)
            .with_time_column("TheMeasurement")
            .with_tag_column("TheMeasurement", "state")
            .with_one_row_of_null_data("TheMeasurement");

        fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        let request = TagValuesRequest {
            tags_source: source.clone(),
            range: Some(make_timestamp_range(150, 2000)),
            predicate: Some(make_state_ma_predicate()),
            tag_key: "state".into(),
        };

        let actual_tag_values = fixture.storage_client.tag_values(request).await.unwrap();
        assert_eq!(actual_tag_values, vec!["MA"]);

        grpc_request_metric_has_count(&fixture, "tag_values", "ok", 1).unwrap();
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

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        // ---
        // test tag_key = _measurement means listing all measurement names
        // ---
        let request = TagValuesRequest {
            tags_source: source.clone(),
            range: Some(make_timestamp_range(1000, 1500)),
            predicate: None,
            tag_key: [0].into(),
        };

        let chunk = TestChunk::new(0).with_table("h2o");

        fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let tag_values = vec!["h2o"];
        let actual_tag_values = fixture.storage_client.tag_values(request).await.unwrap();
        assert_eq!(
            actual_tag_values, tag_values,
            "unexpected tag values while getting tag values for measurement names"
        );

        grpc_request_metric_has_count(&fixture, "tag_values", "ok", 1).unwrap();
    }

    #[tokio::test]
    async fn test_storage_rpc_tag_values_field() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        // Add a chunk with a field
        let chunk = TestChunk::new(0)
            .with_int_field_column("TheMeasurement", "Field1")
            .with_time_column("TheMeasurement")
            .with_tag_column("TheMeasurement", "state")
            .with_one_row_of_null_data("TheMeasurement");

        fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        // ---
        // test tag_key = _field means listing all field names
        // ---
        let request = TagValuesRequest {
            tags_source: source.clone(),
            range: Some(make_timestamp_range(0, 2000)),
            predicate: Some(make_state_ma_predicate()),
            tag_key: [255].into(),
        };

        let expected_tag_values = vec!["Field1"];
        let actual_tag_values = fixture.storage_client.tag_values(request).await.unwrap();
        assert_eq!(
            actual_tag_values, expected_tag_values,
            "unexpected tag values while getting tag values for field names"
        );

        grpc_request_metric_has_count(&fixture, "tag_values", "ok", 1).unwrap();
    }

    #[tokio::test]
    async fn test_storage_rpc_tag_values_error() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let chunk = TestChunk::new(0).with_error("Sugar we are going down");

        fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

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

        grpc_request_metric_has_count(&fixture, "tag_values", "client_error", 1).unwrap();
    }

    /// test the plumbing of the RPC layer for measurement_tag_values
    #[tokio::test]
    async fn test_storage_rpc_measurement_tag_values() {
        test_helpers::maybe_start_logging();
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        // Add a chunk with a field
        let chunk = TestChunk::new(0)
            .with_time_column("TheMeasurement")
            .with_tag_column("TheMeasurement", "state")
            .with_one_row_of_null_data("TheMeasurement");

        fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        let request = MeasurementTagValuesRequest {
            measurement: "TheMeasurement".into(),
            source: source.clone(),
            range: Some(make_timestamp_range(150, 2000)),
            predicate: Some(make_state_ma_predicate()),
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

        grpc_request_metric_has_count(&fixture, "measurement_tag_values", "ok", 1).unwrap();
    }

    #[tokio::test]
    async fn test_storage_rpc_measurement_tag_values_error() {
        test_helpers::maybe_start_logging();
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let chunk = TestChunk::new(0).with_error("Sugar we are going down");

        fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

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

        grpc_request_metric_has_count(&fixture, "measurement_tag_values", "ok", 1).unwrap();
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
                assert_eq!(status.code(), Code::Cancelled);
                assert_contains!(status.message(), "stream no longer needed");
            }
        };

        // Ensure that the logs captured the panic
        let captured_logs = tracing_capture.to_string();
        // Note we don't include the actual line / column in the
        // expected panic message to avoid needing to update the test
        // whenever the source code file changed.
        let expected_error = "panicked at 'This is a test panic', src/influxdb_ioxd/rpc/testing.rs";
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

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        // Add a chunk with a field
        let chunk = TestChunk::new(0)
            .with_time_column("TheMeasurement")
            .with_tag_column("TheMeasurement", "state")
            .with_one_row_of_null_data("TheMeasurement");

        fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        let request = ReadFilterRequest {
            read_source: source.clone(),
            range: Some(make_timestamp_range(0, 10000)),
            predicate: Some(make_state_ma_predicate()),
        };

        let actual_frames = fixture.storage_client.read_filter(request).await.unwrap();

        // TODO: encode the actual output in the test case or something
        let expected_frames: Vec<String> = vec!["0 frames".into()];

        assert_eq!(
            actual_frames, expected_frames,
            "unexpected frames returned by query_series",
        );

        grpc_request_metric_has_count(&fixture, "read_filter", "ok", 1).unwrap();
    }

    #[tokio::test]
    async fn test_read_filter_error() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let chunk = TestChunk::new(0).with_error("Sugar we are going down");

        fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

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
        assert_contains!(response.unwrap_err().to_string(), "Sugar we are going down");

        grpc_request_metric_has_count(&fixture, "read_filter", "client_error", 1).unwrap();
    }

    #[tokio::test]
    async fn test_read_group() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let chunk = TestChunk::new(0)
            .with_time_column("TheMeasurement")
            .with_tag_column("TheMeasurement", "state")
            .with_one_row_of_null_data("TheMeasurement");

        fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        let group = generated_types::read_group_request::Group::By as i32;

        let request = ReadGroupRequest {
            read_source: source.clone(),
            range: Some(make_timestamp_range(0, 2000)),
            predicate: Some(make_state_ma_predicate()),
            group_keys: vec!["state".into()],
            group,
            aggregate: Some(RPCAggregate {
                r#type: AggregateType::Sum as i32,
            }),
            hints: 0,
        };

        let actual_frames = fixture.storage_client.read_group(request).await.unwrap();
        let expected_frames: Vec<String> = vec!["1 group frames".into()];

        assert_eq!(
            actual_frames, expected_frames,
            "unexpected frames returned by query_groups"
        );

        grpc_request_metric_has_count(&fixture, "read_group", "ok", 1).unwrap();
    }

    #[tokio::test]
    async fn test_read_group_error() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let chunk = TestChunk::new(0).with_error("Sugar we are going down");

        fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        let group = generated_types::read_group_request::Group::By as i32;

        // ---
        // test error hit in request processing
        // ---
        let request = ReadGroupRequest {
            read_source: source.clone(),
            range: None,
            predicate: None,
            group_keys: vec!["tag1".into()],
            group,
            aggregate: Some(RPCAggregate {
                r#type: AggregateType::Sum as i32,
            }),
            hints: 42,
        };

        let response_string = fixture
            .storage_client
            .read_group(request)
            .await
            .unwrap_err()
            .to_string();
        assert_contains!(
            response_string,
            "Unexpected hint value on read_group request. Expected 0, got 42"
        );

        grpc_request_metric_has_count(&fixture, "read_group", "error", 1).unwrap();

        // ---
        // test error returned in database processing
        // ---
        let request = ReadGroupRequest {
            read_source: source.clone(),
            range: None,
            predicate: None,
            group_keys: vec!["tag1".into()],
            group,
            aggregate: Some(RPCAggregate {
                r#type: AggregateType::Sum as i32,
            }),
            hints: 0,
        };

        // Note we don't set the response on the test database, so we expect an error
        let response_string = fixture
            .storage_client
            .read_group(request)
            .await
            .unwrap_err()
            .to_string();
        assert_contains!(response_string, "Sugar we are going down");

        grpc_request_metric_has_count(&fixture, "read_group", "client_error", 1).unwrap();
    }

    #[tokio::test]
    async fn test_read_window_aggregate_window_every() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        // Add a chunk with a field
        let chunk = TestChunk::new(0)
            .with_time_column("TheMeasurement")
            .with_tag_column("TheMeasurement", "state")
            .with_one_row_of_null_data("TheMeasurement");

        fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

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
            range: Some(make_timestamp_range(0, 2000)),
            predicate: Some(make_state_ma_predicate()),
            window_every: 1122,
            offset: 15,
            aggregate: vec![RPCAggregate {
                r#type: AggregateType::Sum as i32,
            }],
            // old skool window definition
            window: None,
        };

        let actual_frames = fixture
            .storage_client
            .read_window_aggregate(request_window_every)
            .await
            .unwrap();
        let expected_frames: Vec<String> = vec!["0 aggregate_frames".into()];

        assert_eq!(
            actual_frames, expected_frames,
            "unexpected frames returned by query_groups"
        );

        grpc_request_metric_has_count(&fixture, "read_window_aggregate", "ok", 1).unwrap();
    }

    #[tokio::test]
    async fn test_read_window_aggregate_every_offset() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        // Add a chunk with a field
        let chunk = TestChunk::new(0)
            .with_time_column("TheMeasurement")
            .with_tag_column("TheMeasurement", "state")
            .with_one_row_of_null_data("TheMeasurement");

        fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        // -----
        // Test with window.every and window.offset durations specified
        // -----

        let request_window = ReadWindowAggregateRequest {
            read_source: source.clone(),
            range: Some(make_timestamp_range(150, 200)),
            predicate: Some(make_state_ma_predicate()),
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
                    negative: false,
                }),
            }),
        };

        let actual_frames = fixture
            .storage_client
            .read_window_aggregate(request_window)
            .await
            .unwrap();
        let expected_frames: Vec<String> = vec!["0 aggregate_frames".into()];

        assert_eq!(
            actual_frames, expected_frames,
            "unexpected frames returned by query_groups"
        );
    }

    #[tokio::test]
    async fn test_read_window_aggregate_error() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let chunk = TestChunk::new(0).with_error("Sugar we are going down");

        fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        // ---
        // test error
        // ---

        let request_window = ReadWindowAggregateRequest {
            read_source: source.clone(),
            range: Some(make_timestamp_range(0, 2000)),
            predicate: Some(make_state_ma_predicate()),
            window_every: 1122,
            offset: 15,
            aggregate: vec![RPCAggregate {
                r#type: AggregateType::Sum as i32,
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

        grpc_request_metric_has_count(&fixture, "read_window_aggregate", "client_error", 1)
            .unwrap();
    }

    #[tokio::test]
    async fn test_measurement_fields() {
        test_helpers::maybe_start_logging();

        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        // Add a chunk with a field
        let chunk = TestChunk::new(0)
            .with_int_field_column("TheMeasurement", "Field1")
            .with_time_column("TheMeasurement")
            .with_tag_column("TheMeasurement", "state")
            .with_one_row_of_null_data("TheMeasurement");

        fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        let request = MeasurementFieldsRequest {
            source: source.clone(),
            measurement: "TheMeasurement".into(),
            range: Some(make_timestamp_range(0, 2000)),
            predicate: Some(make_state_ma_predicate()),
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

        grpc_request_metric_has_count(&fixture, "measurement_fields", "ok", 1).unwrap();
    }

    #[tokio::test]
    async fn test_measurement_fields_error() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let chunk = TestChunk::new(0).with_error("Sugar we are going down");

        fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .unwrap()
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

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
    fn make_state_ma_predicate() -> Predicate {
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

    /// InfluxDB IOx deals with database names. The gRPC interface deals
    /// with org_id and bucket_id represented as 16 digit hex
    /// values. This struct manages creating the org_id, bucket_id,
    /// and database names to be consistent with the implementation
    struct OrgAndBucket {
        org_id: u64,
        bucket_id: u64,
        /// The influxdb_iox database name corresponding to `org_id` and
        /// `bucket_id`
        db_name: DatabaseName<'static>,
    }

    impl OrgAndBucket {
        fn new(org_id: u64, bucket_id: u64) -> Self {
            let org_id_str = Id::try_from(org_id).expect("org_id was valid").to_string();

            let bucket_id_str = Id::try_from(bucket_id)
                .expect("bucket_id was valid")
                .to_string();

            let db_name = org_and_bucket_to_database(&org_id_str, &bucket_id_str)
                .expect("mock database name construction failed");

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
        fn read_source(org_id: u64, bucket_id: u64, partition_id: u64) -> Any {
            let read_source = ReadSource {
                org_id,
                bucket_id,
                partition_id,
            };
            let mut d = bytes::BytesMut::new();
            read_source
                .encode(&mut d)
                .expect("encoded read source appropriately");
            Any {
                type_url: "/TODO".to_string(),
                value: d.freeze(),
            }
        }

        /// return the capabilities of the server as a hash map
        async fn capabilities(&mut self) -> Result<HashMap<String, Vec<String>>, tonic::Status> {
            let response = self.inner.capabilities(Empty {}).await?.into_inner();

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

        /// Convert the StringValueResponses into rust Strings, sorting the
        /// values to ensure  consistency.
        fn to_string_vec(&self, responses: Vec<StringValuesResponse>) -> Vec<String> {
            let mut strings = responses
                .into_iter()
                .map(|r| r.values.into_iter())
                .flatten()
                .map(tag_key_bytes_to_strings)
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
        iox_client: IOxTestingClient,
        storage_client: StorageClientWrapper,
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
                .context(Bind)?;

            // Pull the assigned port out of the socket
            let bind_addr = socket.local_addr().unwrap();

            println!(
                "Starting InfluxDB IOx storage test server on {:?}",
                bind_addr
            );

            let router = tonic::transport::Server::builder()
                .add_service(crate::influxdb_ioxd::rpc::testing::make_server())
                .add_service(crate::influxdb_ioxd::rpc::storage::make_server(
                    Arc::clone(&test_storage),
                    test_storage.metrics_registry.registry(),
                    ServingReadinessInterceptor(ServingReadinessState::Serving.into()),
                ));

            let server = async move {
                let stream = TcpListenerStream::new(socket);

                router
                    .serve_with_incoming(stream)
                    .await
                    .log_if_error("Running Tonic Server")
            };

            tokio::task::spawn(server);

            let iox_client = connect_to_server::<IOxTestingClient>(bind_addr)
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
            })
        }
    }

    /// Represents something that can make a connection to a server
    #[tonic::async_trait]
    trait NewClient: Sized + std::fmt::Debug {
        async fn connect(addr: String) -> Result<Self, tonic::transport::Error>;
    }

    #[tonic::async_trait]
    impl NewClient for IOxTestingClient {
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
