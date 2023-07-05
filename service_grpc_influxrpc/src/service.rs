//! This module contains implementations for the storage gRPC service
//! implemented in terms of the [`QueryNamespace`](iox_query::QueryNamespace).

use super::{TAG_KEY_FIELD, TAG_KEY_MEASUREMENT};
use crate::{
    data::{
        fieldlist_to_measurement_fields_response, series_or_groups_to_frames, tag_keys_to_byte_vecs,
    },
    expr::{self, DecodedTagKey, GroupByAndAggregate, InfluxRpcPredicateBuilder, Loggable},
    input::GrpcInputs,
    permit::StreamWithPermit,
    query_completed_token::QueryCompletedTokenStream,
    response_chunking::ChunkReadResponses,
    StorageService,
};
use data_types::NamespaceName;
use datafusion::error::DataFusionError;
use futures::{stream::BoxStream, Stream, StreamExt, TryStreamExt};
use generated_types::{
    google::protobuf::{Any as ProtoAny, Empty},
    influxdata::platform::errors::InfluxDbError,
    literal_or_regex::Value as RegexOrLiteralValue,
    offsets_response::PartitionOffsetResponse,
    read_response::Frame,
    storage_server::Storage,
    tag_key_predicate, CapabilitiesResponse, Capability, Int64ValuesResponse, LiteralOrRegex,
    MeasurementFieldsRequest, MeasurementFieldsResponse, MeasurementNamesRequest,
    MeasurementTagKeysRequest, MeasurementTagValuesRequest, OffsetsResponse, Predicate,
    ReadFilterRequest, ReadGroupRequest, ReadResponse, ReadSeriesCardinalityRequest,
    ReadWindowAggregateRequest, StringValuesResponse, TagKeyMetaNames, TagKeysRequest,
    TagValuesGroupedByMeasurementAndTagKeyRequest, TagValuesRequest, TagValuesResponse,
    TimestampRange,
};
use iox_query::{
    exec::{
        fieldlist::FieldList, seriesset::converter::Error as SeriesSetError,
        ExecutionContextProvider, IOxSessionContext,
    },
    QueryCompletedToken, QueryNamespace, QueryText,
};
use observability_deps::tracing::{error, info, trace};
use prost::{bytes::BytesMut, Message};
use service_common::{datafusion_error_to_tonic_code, planner::Planner, QueryNamespaceProvider};
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    collections::{BTreeSet, HashMap},
    fmt::{Display, Formatter, Result as FmtResult},
    sync::Arc,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{metadata::MetadataMap, Response, Status};
use trace::{ctx::SpanContext, span::SpanExt};
use trace_http::ctx::{RequestLogContext, RequestLogContextExt};
use tracker::InstrumentedAsyncOwnedSemaphorePermit;

/// The size to which we limit our [`ReadResponse`] payloads.
///
/// We will regroup the returned frames (preserving order) to only produce [`ReadResponse`] objects of approximately
/// this size (there's a bit of additional encoding overhead on top of that, but that should be OK).
const MAX_READ_RESPONSE_SIZE: usize = 4194304 - 100_000; // 4MB - <wiggle room>

/// The max number of points allowed in each output data frame. This is the same value TSM uses,
/// and is used to avoid overlarge individual gRPC messages.
const MAX_POINTS_PER_FRAME: usize = 1000;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Namespace not found: {}", db_name))]
    NamespaceNotFound { db_name: String },

    #[snafu(display("Error listing tables in namespace '{}': {}", db_name, source))]
    ListingTables {
        db_name: String,
        source: DataFusionError,
    },

    #[snafu(display("Error listing columns in namespace '{}': {}", db_name, source))]
    ListingColumns {
        db_name: String,
        source: DataFusionError,
    },

    #[snafu(display("Error listing fields in namespace '{}': {}", db_name, source))]
    ListingFields {
        db_name: String,
        source: DataFusionError,
    },

    #[snafu(display("Error creating series plans for namespace '{}': {}", db_name, source))]
    PlanningFilteringSeries {
        db_name: String,
        source: DataFusionError,
    },

    #[snafu(display("Error creating group plans for namespace '{}': {}", db_name, source))]
    PlanningGroupSeries {
        db_name: String,
        source: DataFusionError,
    },

    #[snafu(display("Error running series plans for namespace '{}': {}", db_name, source))]
    FilteringSeries {
        db_name: String,
        source: DataFusionError,
    },

    #[snafu(display("Error running grouping plans for namespace '{}': {}", db_name, source))]
    GroupingSeries {
        db_name: String,
        source: DataFusionError,
    },

    #[snafu(display(
        "Can not retrieve tag values for '{}' in namespace '{}': {}",
        tag_name,
        db_name,
        source
    ))]
    ListingTagValues {
        db_name: String,
        tag_name: String,
        source: DataFusionError,
    },

    #[snafu(display("Error setting predicate table '{:?}': {}", table, source))]
    SettingPredicateTable {
        table: Option<String>,
        source: super::expr::Error,
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

impl From<Error> for Status {
    /// Converts a result from the business logic into the appropriate tonic
    /// status
    fn from(err: Error) -> Self {
        error!(e=%err, "Error handling gRPC request");
        err.into_status()
    }
}

impl Error {
    /// Converts a result from the business logic into the appropriate tonic
    /// status
    fn into_status(self) -> Status {
        let msg = self.to_string();

        let code = match self {
            Self::NamespaceNotFound { .. } => tonic::Code::NotFound,
            Self::ListingTables { source, .. }
            | Self::ListingColumns { source, .. }
            | Self::ListingFields { source, .. }
            | Self::PlanningFilteringSeries { source, .. }
            | Self::PlanningGroupSeries { source, .. }
            | Self::FilteringSeries { source, .. }
            | Self::GroupingSeries { source, .. }
            | Self::ListingTagValues { source, .. } => datafusion_error_to_tonic_code(&source),
            Self::ConvertingPredicate { source, .. }
            | Self::ConvertingReadGroupType { source, .. }
            | Self::ConvertingReadGroupAggregate { source, .. }
            | Self::ConvertingWindowAggregate { source, .. }
            | Self::SettingPredicateTable { source, .. }
                if matches!(
                    source,
                    super::expr::Error::FieldColumnsNotSupported { .. }
                        | super::expr::Error::MultipleTablePredicateNotSupported { .. }
                ) =>
            {
                tonic::Code::Unimplemented
            }
            Self::ConvertingPredicate { .. }
            | Self::ConvertingReadGroupAggregate { .. }
            | Self::ConvertingReadGroupType { .. }
            | Self::ConvertingWindowAggregate { .. }
            | Self::ConvertingTagKeyInTagValues { .. }
            | Self::ComputingGroupedSeriesSet { .. }
            | Self::ConvertingFieldList { .. }
            | Self::SettingPredicateTable { .. }
            | Self::MeasurementLiteralOrRegex { .. }
            | Self::MissingTagKeyPredicate {}
            | Self::InvalidTagKeyRegex { .. } => tonic::Code::InvalidArgument,
            Self::SendingResults { .. } | Self::InternalHintsFieldNotSupported { .. } => {
                tonic::Code::Internal
            }
            Self::NotYetImplemented { .. } => tonic::Code::Unimplemented,
        };

        // InfluxRPC clients expect an instance of InfluxDbError
        // (or another error type from platform.influxdata.errors)
        // to appear in the `details` field of the gRPC status, which
        // helps the client determine if the error should be
        // displayed to users, is retryable, etc.
        let influxdb_error = InfluxDbError {
            code: InfluxCode::from(code).to_string(),
            message: msg.clone(),
            op: "iox/influxrpc".to_string(),
            error: None,
        };
        let mut err_bytes = BytesMut::new();
        match influxdb_error.encode(&mut err_bytes) {
            Ok(()) => (),
            Err(e) => {
                error!(e=%e, "failed to serialized InfluxDBError");
                return Status::unknown(format!("failed to serialize InfluxDB error: {e}"));
            }
        }

        let any_err = ProtoAny {
            type_url: generated_types::protobuf_type_url(
                "influxdata.platform.errors.InfluxDBError",
            ),
            value: err_bytes.freeze(),
        };

        let mut tonic_status = generated_types::google::encode_status(code, msg, any_err);
        add_headers(tonic_status.metadata_mut());
        tonic_status
    }
}

/// These are the set of error codes that can appear in an InfluxDBError.
/// Taken from here:
/// <https://github.com/influxdata/idpe/blob/master/pkg/influxerror/errors.go>
/// Disabling Clippy warning about variant names so that they can match what
/// is in idpe.
#[allow(clippy::enum_variant_names)]
enum InfluxCode {
    EInternal,
    ENotFound,
    EConflict,
    EInvalid,
    // EUnprocessableEntity,
    // EEmptyValue,
    EUnavailable,
    // EForbidden,
    // ETooManyRequests,
    EUnauthorized,
    // EMethodNotAllowed,
    ETooLarge,
    ENotImplemented,
    // EUpstreamServer,
    ERequestCanceled,
}

impl Display for InfluxCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let str = match self {
            InfluxCode::EInternal => "internal error",
            InfluxCode::ENotFound => "not found",
            InfluxCode::EConflict => "conflict",
            InfluxCode::EInvalid => "invalid",
            // InfluxCode::EUnprocessableEntity => "unprocessable entity",
            // InfluxCode::EEmptyValue => "empty value",
            InfluxCode::EUnavailable => "unavailable",
            // InfluxCode::EForbidden => "forbidden",
            // InfluxCode::ETooManyRequests => "too many requests",
            InfluxCode::EUnauthorized => "unauthorized",
            // InfluxCode::EMethodNotAllowed => "method not allowed",
            InfluxCode::ETooLarge => "request too large",
            InfluxCode::ENotImplemented => "not implemented",
            // InfluxCode::EUpstreamServer => "upstream server",
            InfluxCode::ERequestCanceled => "request canceled",
        };
        f.write_str(str)
    }
}

impl From<tonic::Code> for InfluxCode {
    fn from(tonic_code: tonic::Code) -> InfluxCode {
        match tonic_code {
            tonic::Code::Cancelled => InfluxCode::ERequestCanceled,
            tonic::Code::InvalidArgument => InfluxCode::EInvalid,
            tonic::Code::NotFound => InfluxCode::ENotFound,
            tonic::Code::AlreadyExists => InfluxCode::EConflict,
            tonic::Code::PermissionDenied => InfluxCode::EUnauthorized,
            tonic::Code::ResourceExhausted => InfluxCode::ETooLarge,
            tonic::Code::FailedPrecondition => InfluxCode::EInvalid,
            tonic::Code::OutOfRange => InfluxCode::EInvalid,
            tonic::Code::Unimplemented => InfluxCode::ENotImplemented,
            tonic::Code::Unavailable => InfluxCode::EUnavailable,
            _ => InfluxCode::EInternal,
        }
    }
}

/// Add IOx specific headers to the response
///
/// storage-type: iox (needed so IDPE can show the errors to users)
///   see <https://github.com/influxdata/conductor/issues/1208>
fn add_headers(metadata: &mut MetadataMap) {
    // Note we can't use capital letters otherwise the http header
    // library asserts, so return lowercase storage-type
    metadata.insert("storage-type", "iox".parse().unwrap());
}

/// Implements the protobuf defined Storage service for a [`QueryNamespaceProvider`]
#[tonic::async_trait]
impl<T> Storage for StorageService<T>
where
    T: QueryNamespaceProvider + 'static,
{
    type ReadFilterStream =
        StreamWithPermit<QueryCompletedTokenStream<ChunkReadResponses, ReadResponse, Status>>;

    async fn read_filter(
        &self,
        req: tonic::Request<ReadFilterRequest>,
    ) -> Result<Response<Self::ReadFilterStream>, Status> {
        let external_span_ctx: Option<RequestLogContext> = req.extensions().get().cloned();
        let span_ctx: Option<SpanContext> = req.extensions().get().cloned();

        let req = req.into_inner();
        let permit = self
            .db_store
            .acquire_semaphore(span_ctx.child_span("query rate limit semaphore"))
            .await;
        let db_name = get_namespace_name(&req)?;
        info!(
            %db_name,
            ?req.range,
            predicate=%req.predicate.loggable(),
            trace=%external_span_ctx.format_jaeger(),
            "read filter",
        );

        let db = self
            .db_store
            .db(&db_name, span_ctx.child_span("get namespace"), false)
            .await
            .context(NamespaceNotFoundSnafu { db_name: &db_name })?;

        let ctx = db.new_query_context(span_ctx);
        let query_completed_token = db.record_query(&ctx, "read_filter", defer_json(&req));

        let frames = read_filter_impl(Arc::clone(&db), db_name, req, &ctx)
            .await?
            .map_err(|e| e.into_status());

        make_response(
            ChunkReadResponses::new(frames, MAX_READ_RESPONSE_SIZE),
            query_completed_token,
            permit,
        )
    }

    type ReadGroupStream =
        StreamWithPermit<QueryCompletedTokenStream<ChunkReadResponses, ReadResponse, Status>>;

    async fn read_group(
        &self,
        req: tonic::Request<ReadGroupRequest>,
    ) -> Result<Response<Self::ReadGroupStream>, Status> {
        let external_span_ctx: Option<RequestLogContext> = req.extensions().get().cloned();
        let span_ctx: Option<SpanContext> = req.extensions().get().cloned();
        let req = req.into_inner();
        let permit = self
            .db_store
            .acquire_semaphore(span_ctx.child_span("query rate limit semaphore"))
            .await;

        let db_name = get_namespace_name(&req)?;

        info!(
            %db_name,
            ?req.range,
            ?req.group_keys,
            ?req.group,
            ?req.aggregate,
            predicate=%req.predicate.loggable(),
            trace=%external_span_ctx.format_jaeger(),
            "read_group",
        );

        let db = self
            .db_store
            .db(&db_name, span_ctx.child_span("get namespace"), false)
            .await
            .context(NamespaceNotFoundSnafu { db_name: &db_name })?;

        let ctx = db.new_query_context(span_ctx);
        let query_completed_token = db.record_query(&ctx, "read_group", defer_json(&req));

        let ReadGroupRequest {
            read_source: _read_source,
            range,
            predicate,
            group_keys,
            group,
            aggregate,
        } = req;

        let aggregate_string =
            format!("aggregate: {aggregate:?}, group: {group:?}, group_keys: {group_keys:?}");

        let group = expr::convert_group_type(group).context(ConvertingReadGroupTypeSnafu {
            aggregate_string: &aggregate_string,
        })?;

        let gby_agg = expr::make_read_group_aggregate(aggregate, group, group_keys)
            .context(ConvertingReadGroupAggregateSnafu { aggregate_string })?;

        let frames = query_group_impl(
            Arc::clone(&db),
            db_name,
            range,
            predicate,
            gby_agg,
            TagKeyMetaNames::Text,
            &ctx,
        )
        .await
        .map_err(|e| e.into_status())?
        .map_err(|e| e.into_status());

        make_response(
            ChunkReadResponses::new(frames, MAX_READ_RESPONSE_SIZE),
            query_completed_token,
            permit,
        )
    }

    type ReadWindowAggregateStream =
        StreamWithPermit<QueryCompletedTokenStream<ChunkReadResponses, ReadResponse, Status>>;

    async fn read_window_aggregate(
        &self,
        req: tonic::Request<ReadWindowAggregateRequest>,
    ) -> Result<Response<Self::ReadGroupStream>, Status> {
        let external_span_ctx: Option<RequestLogContext> = req.extensions().get().cloned();
        let span_ctx: Option<SpanContext> = req.extensions().get().cloned();
        let req = req.into_inner();
        let permit = self
            .db_store
            .acquire_semaphore(span_ctx.child_span("query rate limit semaphore"))
            .await;

        let db_name = get_namespace_name(&req)?;
        info!(
            %db_name,
            ?req.range,
            ?req.window_every,
            ?req.offset,
            ?req.aggregate,
            ?req.window,
            predicate=%req.predicate.loggable(),
            trace=%external_span_ctx.format_jaeger(),
            "read_window_aggregate",
        );

        let db = self
            .db_store
            .db(&db_name, span_ctx.child_span("get namespace"), false)
            .await
            .context(NamespaceNotFoundSnafu { db_name: &db_name })?;

        let ctx = db.new_query_context(span_ctx);
        let query_completed_token =
            db.record_query(&ctx, "read_window_aggregate", defer_json(&req));

        let ReadWindowAggregateRequest {
            read_source: _read_source,
            range,
            predicate,
            window_every,
            offset,
            aggregate,
            window,
            tag_key_meta_names,
        } = req;

        let aggregate_string = format!(
            "aggregate: {aggregate:?}, window_every: {window_every:?}, offset: {offset:?}, window: {window:?}"
        );

        let gby_agg = expr::make_read_window_aggregate(aggregate, window_every, offset, window)
            .context(ConvertingWindowAggregateSnafu { aggregate_string })?;

        let frames = query_group_impl(
            Arc::clone(&db),
            db_name,
            range,
            predicate,
            gby_agg,
            TagKeyMetaNames::from_i32(tag_key_meta_names).unwrap_or_default(),
            &ctx,
        )
        .await
        .map_err(|e| e.into_status())?
        .map_err(|e| e.into_status());

        make_response(
            ChunkReadResponses::new(frames, MAX_READ_RESPONSE_SIZE),
            query_completed_token,
            permit,
        )
    }

    type TagKeysStream = StreamWithPermit<
        QueryCompletedTokenStream<
            BoxStream<'static, Result<StringValuesResponse, Status>>,
            StringValuesResponse,
            Status,
        >,
    >;

    async fn tag_keys(
        &self,
        req: tonic::Request<TagKeysRequest>,
    ) -> Result<Response<Self::TagKeysStream>, Status> {
        let external_span_ctx: Option<RequestLogContext> = req.extensions().get().cloned();
        let span_ctx: Option<SpanContext> = req.extensions().get().cloned();

        let req = req.into_inner();
        let permit = self
            .db_store
            .acquire_semaphore(span_ctx.child_span("query rate limit semaphore"))
            .await;

        let db_name = get_namespace_name(&req)?;
        info!(
            %db_name,
            ?req.range,
            predicate=%req.predicate.loggable(),
            trace=%external_span_ctx.format_jaeger(),
            "tag_keys",
        );

        let db = self
            .db_store
            .db(&db_name, span_ctx.child_span("get namespace"), false)
            .await
            .context(NamespaceNotFoundSnafu { db_name: &db_name })?;

        let ctx = db.new_query_context(span_ctx);
        let query_completed_token = db.record_query(&ctx, "tag_keys", defer_json(&req));

        let TagKeysRequest {
            tags_source: _tag_source,
            range,
            predicate,
        } = req;

        let measurement = None;

        let response = tag_keys_impl(
            Arc::clone(&db),
            db_name,
            measurement,
            range,
            predicate,
            &ctx,
        )
        .await
        .map_err(|e| e.into_status());

        make_response(
            futures::stream::once(async move { response }).boxed(),
            query_completed_token,
            permit,
        )
    }

    type TagValuesStream = StreamWithPermit<
        QueryCompletedTokenStream<
            BoxStream<'static, Result<StringValuesResponse, Status>>,
            StringValuesResponse,
            Status,
        >,
    >;

    async fn tag_values(
        &self,
        req: tonic::Request<TagValuesRequest>,
    ) -> Result<Response<Self::TagValuesStream>, Status> {
        let external_span_ctx: Option<RequestLogContext> = req.extensions().get().cloned();
        let span_ctx: Option<SpanContext> = req.extensions().get().cloned();

        let req = req.into_inner();
        let permit = self
            .db_store
            .acquire_semaphore(span_ctx.child_span("query rate limit semaphore"))
            .await;

        let db_name = get_namespace_name(&req)?;
        let tag_key = DecodedTagKey::try_from(req.tag_key.clone())
            .context(ConvertingTagKeyInTagValuesSnafu)?;
        info!(
            %db_name,
            ?req.range,
            %tag_key,
            predicate=%req.predicate.loggable(),
            trace=%external_span_ctx.format_jaeger(),
            "tag_values",
        );

        let db = self
            .db_store
            .db(&db_name, span_ctx.child_span("get namespace"), false)
            .await
            .context(NamespaceNotFoundSnafu { db_name: &db_name })?;

        let ctx = db.new_query_context(span_ctx);
        let query_completed_token = db.record_query(&ctx, "tag_values", defer_json(&req));

        let TagValuesRequest {
            tags_source: _tag_source,
            range,
            predicate,
            ..
        } = req;

        let measurement = None;

        // Special case a request for 'tag_key=_measurement" means to list all
        // measurements
        let response = match tag_key {
            DecodedTagKey::Measurement => {
                if predicate.is_some() {
                    return Err(Error::NotYetImplemented {
                        operation: "tag_value for a measurement, with general predicate"
                            .to_string(),
                    }
                    .into_status());
                }

                measurement_name_impl(Arc::clone(&db), db_name, range, predicate, &ctx).await
            }
            DecodedTagKey::Field => {
                let fieldlist =
                    field_names_impl(Arc::clone(&db), db_name, None, range, predicate, &ctx)
                        .await?;

                // Pick out the field names into a Vec<Vec<u8>>for return
                let values = fieldlist
                    .fields
                    .into_iter()
                    .map(|f| f.name.bytes().collect())
                    .collect::<Vec<_>>();

                Ok(StringValuesResponse { values })
            }
            DecodedTagKey::Normal(tag_key) => {
                tag_values_impl(
                    Arc::clone(&db),
                    db_name,
                    tag_key,
                    measurement,
                    range,
                    predicate,
                    &ctx,
                )
                .await
            }
        };

        let response = response.map_err(|e| e.into_status());

        make_response(
            futures::stream::once(async move { response }).boxed(),
            query_completed_token,
            permit,
        )
    }

    type TagValuesGroupedByMeasurementAndTagKeyStream = StreamWithPermit<
        QueryCompletedTokenStream<
            futures::stream::Iter<std::vec::IntoIter<Result<TagValuesResponse, Status>>>,
            TagValuesResponse,
            Status,
        >,
    >;

    async fn tag_values_grouped_by_measurement_and_tag_key(
        &self,
        req: tonic::Request<TagValuesGroupedByMeasurementAndTagKeyRequest>,
    ) -> Result<Response<Self::TagValuesGroupedByMeasurementAndTagKeyStream>, Status> {
        let external_span_ctx: Option<RequestLogContext> = req.extensions().get().cloned();
        let span_ctx: Option<SpanContext> = req.extensions().get().cloned();

        let req = req.into_inner();
        let permit = self
            .db_store
            .acquire_semaphore(span_ctx.child_span("query rate limit semaphore"))
            .await;

        let db_name = get_namespace_name(&req)?;
        info!(
            %db_name,
            ?req.measurement_patterns,
            ?req.tag_key_predicate,
            predicate=%req.condition.loggable(),
            trace=%external_span_ctx.format_jaeger(),
            "tag_values_grouped_by_measurement_and_tag_key",
        );

        let db = self
            .db_store
            .db(&db_name, span_ctx.child_span("get namespace"), false)
            .await
            .context(NamespaceNotFoundSnafu { db_name: &db_name })?;

        let ctx = db.new_query_context(span_ctx);
        let query_completed_token = db.record_query(
            &ctx,
            "tag_values_grouped_by_measurement_and_tag_key",
            defer_json(&req),
        );

        let results =
            tag_values_grouped_by_measurement_and_tag_key_impl(Arc::clone(&db), db_name, req, &ctx)
                .await
                .map_err(|e| e.into_status())?
                .into_iter()
                .map(Ok)
                .collect::<Vec<_>>();

        make_response(
            futures::stream::iter(results),
            query_completed_token,
            permit,
        )
    }

    type ReadSeriesCardinalityStream = ReceiverStream<Result<Int64ValuesResponse, Status>>;

    async fn read_series_cardinality(
        &self,
        _req: tonic::Request<ReadSeriesCardinalityRequest>,
    ) -> Result<Response<Self::ReadSeriesCardinalityStream>, Status> {
        unimplemented!("read_series_cardinality not yet implemented. https://github.com/influxdata/influxdb_iox/issues/447");
    }

    async fn capabilities(
        &self,
        _req: tonic::Request<Empty>,
    ) -> Result<Response<CapabilitiesResponse>, Status> {
        // Full list of go capabilities in
        // idpe/storage/read/capabilities.go (aka window aggregate /
        // pushdown)
        //
        info!("capabilities");

        // For now, hard code our list of support
        let caps = [
            ("KeySortCapability", vec!["ReadFilter"]),
            ("Group", vec!["First", "Last", "Min", "Max"]),
            (
                "TagKeyMetaNamesCapability",
                vec!["TagKeyMetaNamesWindowAggregate"],
            ),
            (
                "WindowAggregate",
                vec![
                    "Count", "Sum", // "First"
                    // "Last",
                    "Min", "Max", "Mean",
                    // "Offset"
                ],
            ),
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
        Ok(Response::new(caps))
    }

    type MeasurementNamesStream = StreamWithPermit<
        QueryCompletedTokenStream<
            BoxStream<'static, Result<StringValuesResponse, Status>>,
            StringValuesResponse,
            Status,
        >,
    >;

    async fn measurement_names(
        &self,
        req: tonic::Request<MeasurementNamesRequest>,
    ) -> Result<Response<Self::MeasurementNamesStream>, Status> {
        let external_span_ctx: Option<RequestLogContext> = req.extensions().get().cloned();
        let span_ctx: Option<SpanContext> = req.extensions().get().cloned();

        let req = req.into_inner();
        let permit = self
            .db_store
            .acquire_semaphore(span_ctx.child_span("query rate limit semaphore"))
            .await;

        let db_name = get_namespace_name(&req)?;
        info!(
            %db_name,
            ?req.range,
            predicate=%req.predicate.loggable(),
            trace=%external_span_ctx.format_jaeger(),
            "measurement_names",
        );

        let db = self
            .db_store
            .db(&db_name, span_ctx.child_span("get namespace"), false)
            .await
            .context(NamespaceNotFoundSnafu { db_name: &db_name })?;

        let ctx = db.new_query_context(span_ctx);
        let query_completed_token = db.record_query(&ctx, "measurement_names", defer_json(&req));

        let MeasurementNamesRequest {
            source: _source,
            range,
            predicate,
        } = req;

        let response = measurement_name_impl(Arc::clone(&db), db_name, range, predicate, &ctx)
            .await
            .map_err(|e| e.into_status());

        make_response(
            futures::stream::once(async move { response }).boxed(),
            query_completed_token,
            permit,
        )
    }

    type MeasurementTagKeysStream = StreamWithPermit<
        QueryCompletedTokenStream<
            BoxStream<'static, Result<StringValuesResponse, Status>>,
            StringValuesResponse,
            Status,
        >,
    >;

    async fn measurement_tag_keys(
        &self,
        req: tonic::Request<MeasurementTagKeysRequest>,
    ) -> Result<Response<Self::MeasurementTagKeysStream>, Status> {
        let external_span_ctx: Option<RequestLogContext> = req.extensions().get().cloned();
        let span_ctx: Option<SpanContext> = req.extensions().get().cloned();

        let req = req.into_inner();
        let permit = self
            .db_store
            .acquire_semaphore(span_ctx.child_span("query rate limit semaphore"))
            .await;

        let db_name = get_namespace_name(&req)?;
        info!(
            %db_name,
            ?req.range,
            %req.measurement,
            predicate=%req.predicate.loggable(),
            trace=%external_span_ctx.format_jaeger(),
            "measurement_tag_keys",
        );

        let db = self
            .db_store
            .db(&db_name, span_ctx.child_span("get namespace"), false)
            .await
            .context(NamespaceNotFoundSnafu { db_name: &db_name })?;

        let ctx = db.new_query_context(span_ctx);
        let query_completed_token = db.record_query(&ctx, "measurement_tag_keys", defer_json(&req));

        let MeasurementTagKeysRequest {
            source: _source,
            measurement,
            range,
            predicate,
        } = req;

        let measurement = Some(measurement);

        let response = tag_keys_impl(
            Arc::clone(&db),
            db_name,
            measurement,
            range,
            predicate,
            &ctx,
        )
        .await
        .map_err(|e| e.into_status());

        make_response(
            futures::stream::once(async move { response }).boxed(),
            query_completed_token,
            permit,
        )
    }

    type MeasurementTagValuesStream = StreamWithPermit<
        QueryCompletedTokenStream<
            BoxStream<'static, Result<StringValuesResponse, Status>>,
            StringValuesResponse,
            Status,
        >,
    >;

    async fn measurement_tag_values(
        &self,
        req: tonic::Request<MeasurementTagValuesRequest>,
    ) -> Result<Response<Self::MeasurementTagValuesStream>, Status> {
        let external_span_ctx: Option<RequestLogContext> = req.extensions().get().cloned();
        let span_ctx: Option<SpanContext> = req.extensions().get().cloned();

        let req = req.into_inner();
        let permit = self
            .db_store
            .acquire_semaphore(span_ctx.child_span("query rate limit semaphore"))
            .await;

        let db_name = get_namespace_name(&req)?;
        info!(
            %db_name,
            ?req.range,
            %req.measurement,
            %req.tag_key,
            predicate=%req.predicate.loggable(),
            trace=%external_span_ctx.format_jaeger(),
            "measurement_tag_values",
        );

        let db = self
            .db_store
            .db(&db_name, span_ctx.child_span("get namespace"), false)
            .await
            .context(NamespaceNotFoundSnafu { db_name: &db_name })?;

        let ctx = db.new_query_context(span_ctx);
        let query_completed_token =
            db.record_query(&ctx, "measurement_tag_values", defer_json(&req));

        let MeasurementTagValuesRequest {
            source: _source,
            measurement,
            range,
            predicate,
            tag_key,
        } = req;

        let measurement = Some(measurement);

        let response = tag_values_impl(
            Arc::clone(&db),
            db_name,
            tag_key,
            measurement,
            range,
            predicate,
            &ctx,
        )
        .await
        .map_err(|e| e.into_status());

        make_response(
            futures::stream::once(async move { response }).boxed(),
            query_completed_token,
            permit,
        )
    }

    type MeasurementFieldsStream = StreamWithPermit<
        QueryCompletedTokenStream<
            BoxStream<'static, Result<MeasurementFieldsResponse, Status>>,
            MeasurementFieldsResponse,
            Status,
        >,
    >;

    async fn measurement_fields(
        &self,
        req: tonic::Request<MeasurementFieldsRequest>,
    ) -> Result<Response<Self::MeasurementFieldsStream>, Status> {
        let external_span_ctx: Option<RequestLogContext> = req.extensions().get().cloned();
        let span_ctx: Option<SpanContext> = req.extensions().get().cloned();

        let req = req.into_inner();
        let permit = self
            .db_store
            .acquire_semaphore(span_ctx.child_span("query rate limit semaphore"))
            .await;

        let db_name = get_namespace_name(&req)?;
        info!(
            %db_name,
            ?req.range,
            %req.measurement,
            predicate=%req.predicate.loggable(),
            trace=%external_span_ctx.format_jaeger(),
            "measurement_fields",
        );

        let db = self
            .db_store
            .db(&db_name, span_ctx.child_span("get namespace"), false)
            .await
            .context(NamespaceNotFoundSnafu { db_name: &db_name })?;

        let ctx = db.new_query_context(span_ctx);
        let query_completed_token = db.record_query(&ctx, "measurement_fields", defer_json(&req));

        let MeasurementFieldsRequest {
            source: _source,
            measurement,
            range,
            predicate,
        } = req;

        let measurement = Some(measurement);

        let response = field_names_impl(
            Arc::clone(&db),
            db_name,
            measurement,
            range,
            predicate,
            &ctx,
        )
        .await
        .map(|fieldlist| {
            fieldlist_to_measurement_fields_response(fieldlist)
                .context(ConvertingFieldListSnafu)
                .map_err(|e| e.into_status())
        })
        .map_err(|e| e.into_status())?;

        make_response(
            futures::stream::once(async move { response }).boxed(),
            query_completed_token,
            permit,
        )
    }

    async fn offsets(
        &self,
        _req: tonic::Request<Empty>,
    ) -> Result<Response<OffsetsResponse>, Status> {
        // We present ourselves to the rest of IDPE as a single storage node with 1 partition.
        // (Returning offset 1 just in case offset 0 is interpreted by query nodes as being special)
        let the_partition = PartitionOffsetResponse { id: 0, offset: 1 };
        Ok(Response::new(OffsetsResponse {
            partitions: vec![the_partition],
        }))
    }
}

fn get_namespace_name(input: &impl GrpcInputs) -> Result<NamespaceName<'static>, Status> {
    NamespaceName::from_org_and_bucket(input.org_id()?.to_string(), input.bucket_name()?)
        .map_err(|e| Status::internal(e.to_string()))
}

// The following code implements the business logic of the requests as
// methods that return Results with module specific Errors (and thus
// can use ?, etc). The trait implementations then handle mapping
// to the appropriate tonic Status

/// Gathers all measurement names that have data in the specified
/// (optional) range
async fn measurement_name_impl<N>(
    db: Arc<N>,
    db_name: NamespaceName<'static>,
    range: Option<TimestampRange>,
    rpc_predicate: Option<Predicate>,
    ctx: &IOxSessionContext,
) -> Result<StringValuesResponse>
where
    N: QueryNamespace + ExecutionContextProvider + 'static,
{
    let rpc_predicate_string = format!("{rpc_predicate:?}");
    let db_name = db_name.as_str();

    let predicate = InfluxRpcPredicateBuilder::default()
        .set_range(range)
        .rpc_predicate(rpc_predicate)
        .context(ConvertingPredicateSnafu {
            rpc_predicate_string,
        })?
        .build();

    let plan = Planner::new(ctx)
        .table_names(db, predicate)
        .await
        .context(ListingTablesSnafu { db_name })?;

    let table_names = ctx
        .to_string_set(plan)
        .await
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
async fn tag_keys_impl<N>(
    db: Arc<N>,
    db_name: NamespaceName<'static>,
    measurement: Option<String>,
    range: Option<TimestampRange>,
    rpc_predicate: Option<Predicate>,
    ctx: &IOxSessionContext,
) -> Result<StringValuesResponse>
where
    N: QueryNamespace + ExecutionContextProvider + 'static,
{
    let rpc_predicate_string = format!("{rpc_predicate:?}");
    let db_name = db_name.as_str();

    let predicate = InfluxRpcPredicateBuilder::default()
        .set_range(range)
        .table_option(measurement.clone())
        .context(SettingPredicateTableSnafu { table: measurement })?
        .rpc_predicate(rpc_predicate)
        .context(ConvertingPredicateSnafu {
            rpc_predicate_string,
        })?
        .build();

    let tag_key_plan = Planner::new(ctx)
        .tag_keys(db, predicate)
        .await
        .context(ListingColumnsSnafu { db_name })?;

    let tag_keys = ctx
        .to_string_set(tag_key_plan)
        .await
        .context(ListingColumnsSnafu { db_name })?;

    // Map the resulting collection of Strings into a Vec<Vec<u8>>for return
    let values = tag_keys_to_byte_vecs(tag_keys);

    trace!(tag_keys=?values.iter().map(|k| String::from_utf8_lossy(k)).collect::<Vec<_>>(), "Tag keys response");
    Ok(StringValuesResponse { values })
}

/// Return tag values for tag_name, with optional measurement, timestamp and
/// arbitratry predicates
async fn tag_values_impl<N>(
    db: Arc<N>,
    db_name: NamespaceName<'static>,
    tag_name: String,
    measurement: Option<String>,
    range: Option<TimestampRange>,
    rpc_predicate: Option<Predicate>,
    ctx: &IOxSessionContext,
) -> Result<StringValuesResponse>
where
    N: QueryNamespace + ExecutionContextProvider + 'static,
{
    let rpc_predicate_string = format!("{rpc_predicate:?}");

    let predicate = InfluxRpcPredicateBuilder::default()
        .set_range(range)
        .table_option(measurement.clone())
        .context(SettingPredicateTableSnafu { table: measurement })?
        .rpc_predicate(rpc_predicate)
        .context(ConvertingPredicateSnafu {
            rpc_predicate_string,
        })?
        .build();

    let db_name = db_name.as_str();
    let tag_name = &tag_name;

    let tag_value_plan = Planner::new(ctx)
        .tag_values(db, tag_name, predicate)
        .await
        .context(ListingTagValuesSnafu { db_name, tag_name })?;

    let tag_values = ctx
        .to_string_set(tag_value_plan)
        .await
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
async fn tag_values_grouped_by_measurement_and_tag_key_impl<N>(
    db: Arc<N>,
    db_name: NamespaceName<'static>,
    req: TagValuesGroupedByMeasurementAndTagKeyRequest,
    ctx: &IOxSessionContext,
) -> Result<Vec<TagValuesResponse>, Error>
where
    N: QueryNamespace + ExecutionContextProvider + 'static,
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
        ctx,
    )
    .await?;

    let mut responses = vec![];
    for name in measurements.into_iter() {
        let tag_keys = materialise_tag_keys(
            Arc::clone(&db),
            db_name.clone(),
            name.clone(),
            tag_key_pred.clone(),
            ctx,
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
                ctx,
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
async fn read_filter_impl<N>(
    db: Arc<N>,
    db_name: NamespaceName<'static>,
    req: ReadFilterRequest,
    ctx: &IOxSessionContext,
) -> Result<impl Stream<Item = Result<Frame, Error>>, Error>
where
    N: QueryNamespace + ExecutionContextProvider + 'static,
{
    let db_name = db_name.as_str();

    let rpc_predicate_string = format!("{:?}", req.predicate);

    let predicate = InfluxRpcPredicateBuilder::default()
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
    let series_plan = Planner::new(ctx)
        .read_filter(db, predicate)
        .await
        .context(PlanningFilteringSeriesSnafu { db_name })?;

    // Execute the plans.
    let db_name = db_name.to_owned();
    let series_or_groups = ctx
        .to_series_and_groups(
            series_plan,
            Arc::clone(&ctx.inner().runtime_env().memory_pool),
            MAX_POINTS_PER_FRAME,
        )
        .await
        .context(FilteringSeriesSnafu {
            db_name: db_name.clone(),
        })
        .log_if_error("Running series set plan")?
        .map_err(move |e| Error::FilteringSeries {
            db_name: db_name.clone(),
            source: e,
        });

    let emit_tag_keys_binary_format = req.tag_key_meta_names == TagKeyMetaNames::Binary as i32;

    Ok(series_or_groups_to_frames(
        series_or_groups,
        emit_tag_keys_binary_format,
    ))
}

/// Launch async tasks that send the result of executing read_group to `tx`
async fn query_group_impl<N>(
    db: Arc<N>,
    db_name: NamespaceName<'static>,
    range: Option<TimestampRange>,
    rpc_predicate: Option<Predicate>,
    gby_agg: GroupByAndAggregate,
    tag_key_meta_names: TagKeyMetaNames,
    ctx: &IOxSessionContext,
) -> Result<impl Stream<Item = Result<Frame, Error>>>
where
    N: QueryNamespace + ExecutionContextProvider + 'static,
{
    let db_name = db_name.as_str();

    let rpc_predicate_string = format!("{rpc_predicate:?}");

    let predicate = InfluxRpcPredicateBuilder::default()
        .set_range(range)
        .rpc_predicate(rpc_predicate)
        .context(ConvertingPredicateSnafu {
            rpc_predicate_string,
        })?
        .build();

    let planner = Planner::new(ctx);
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
    let grouped_series_set_plan =
        grouped_series_set_plan.context(PlanningGroupSeriesSnafu { db_name })?;

    // PERF - This used to send responses to the client before execution had
    // completed, but now it doesn't. We may need to revisit this in the future
    // if big queries are causing a significant latency in TTFB.

    // Execute the plans
    let db_name = db_name.to_owned();
    let series_or_groups = ctx
        .to_series_and_groups(
            grouped_series_set_plan,
            Arc::clone(&ctx.inner().runtime_env().memory_pool),
            MAX_POINTS_PER_FRAME,
        )
        .await
        .context(GroupingSeriesSnafu {
            db_name: db_name.clone(),
        })
        .log_if_error("Running Grouped SeriesSet Plan")?
        .map_err(move |e| Error::FilteringSeries {
            db_name: db_name.clone(),
            source: e,
        });

    let tag_key_binary_format = tag_key_meta_names == TagKeyMetaNames::Binary;

    Ok(series_or_groups_to_frames(
        series_or_groups,
        tag_key_binary_format,
    ))
}

/// Return field names, restricted via optional measurement, timestamp and
/// predicate
async fn field_names_impl<N>(
    db: Arc<N>,
    db_name: NamespaceName<'static>,
    measurement: Option<String>,
    range: Option<TimestampRange>,
    rpc_predicate: Option<Predicate>,
    ctx: &IOxSessionContext,
) -> Result<FieldList>
where
    N: QueryNamespace + ExecutionContextProvider + 'static,
{
    let rpc_predicate_string = format!("{rpc_predicate:?}");

    let predicate = InfluxRpcPredicateBuilder::default()
        .set_range(range)
        .table_option(measurement.clone())
        .context(SettingPredicateTableSnafu { table: measurement })?
        .rpc_predicate(rpc_predicate)
        .context(ConvertingPredicateSnafu {
            rpc_predicate_string,
        })?
        .build();

    let db_name = db_name.as_str();

    let field_list_plan = Planner::new(ctx)
        .field_columns(db, predicate)
        .await
        .context(ListingFieldsSnafu { db_name })?;

    let field_list = ctx
        .to_field_list(field_list_plan)
        .await
        .context(ListingFieldsSnafu { db_name })?;

    trace!(field_names=?field_list, "Field names response");
    Ok(field_list)
}

/// Materialises a collection of measurement names. Typically used as part of
/// a plan to scope and group multiple plans by measurement name.
async fn materialise_measurement_names<N>(
    db: Arc<N>,
    db_name: NamespaceName<'static>,
    measurement_exprs: Vec<LiteralOrRegex>,
    ctx: &IOxSessionContext,
) -> Result<BTreeSet<String>, Error>
where
    N: QueryNamespace + ExecutionContextProvider + 'static,
{
    use generated_types::{
        node::{Comparison, Type, Value},
        Node,
    };

    let mut names = BTreeSet::new();

    // Materialise all measurements
    if measurement_exprs.is_empty() {
        let resp = measurement_name_impl(Arc::clone(&db), db_name.clone(), None, None, ctx).await?;
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
                        ctx,
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
async fn materialise_tag_keys<N>(
    db: Arc<N>,
    db_name: NamespaceName<'static>,
    measurement_name: String,
    tag_key_predicate: tag_key_predicate::Value,
    ctx: &IOxSessionContext,
) -> Result<BTreeSet<String>, Error>
where
    N: QueryNamespace + ExecutionContextProvider + 'static,
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
        ctx,
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
fn defer_json<S>(s: &S) -> QueryText
where
    S: serde::Serialize + Send + Sync + Clone + 'static,
{
    /// Defers conversion into a String
    struct DeferredToJson<S>
    where
        S: serde::Serialize,
    {
        s: S,
    }

    impl<S: serde::Serialize> std::fmt::Display for DeferredToJson<S> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            // This buffering is unfortunate but `Formatter` doesn't implement `std::io::Write`
            match serde_json::to_string_pretty(&self.s) {
                Ok(s) => f.write_str(&s),
                Err(e) => write!(f, "error formatting: {e}"),
            }
        }
    }

    Box::new(DeferredToJson { s: s.clone() })
}

/// Add ability for Results to log error messages via `error!` logs.
/// This is useful when using async tasks that may not have any code
/// checking their return values.
pub trait ErrorLogger {
    /// Log the contents of self with a string of context. The context
    /// should appear in a message such as
    ///
    /// "Error `<context>`: `<formatted error message>`
    fn log_if_error(self, context: &str) -> Self;

    /// Provided method to log an error via the `error!` macro
    fn log_error<E: std::fmt::Debug>(context: &str, e: E) {
        error!("Error {}: {:?}", context, e);
    }
}

/// Implement logging for all results
impl<T, E: std::fmt::Debug> ErrorLogger for Result<T, E> {
    fn log_if_error(self, context: &str) -> Self {
        if let Err(e) = &self {
            Self::log_error(context, e);
        }
        self
    }
}

/// Return the stream of results as a gRPC (tonic) response
#[allow(clippy::type_complexity)]
pub fn make_response<S, T, E>(
    stream: S,
    token: QueryCompletedToken,
    permit: InstrumentedAsyncOwnedSemaphorePermit,
) -> Result<Response<StreamWithPermit<QueryCompletedTokenStream<S, T, E>>>, Status>
where
    S: Stream<Item = Result<T, E>> + Unpin + Send,
{
    let mut response = Response::new(StreamWithPermit::new(
        QueryCompletedTokenStream::new(stream, token),
        permit,
    ));
    add_headers(response.metadata_mut());
    Ok(response)
}

#[cfg(test)]
mod tests {
    use crate::test_util::Fixture;

    use super::*;
    use futures::Future;
    use generated_types::{google::rpc::Status as GrpcStatus, tag_key_predicate::Value};
    use influxdb_storage_client::{generated_types::*, Client as StorageClient, OrgAndBucket};
    use iox_query::test::TestChunk;
    use metric::{Attributes, Metric, U64Counter, U64Gauge};
    use service_common::test_util::TestDatabaseStore;
    use std::{any::Any, num::NonZeroU64, sync::Arc};
    use test_helpers::{assert_contains, maybe_start_logging};
    use tokio::pin;

    fn to_str_vec(s: &[&str]) -> Vec<String> {
        s.iter().map(|s| s.to_string()).collect()
    }

    // Helper function to assert that metric tracking all gRPC requests has
    // correctly updated.
    fn grpc_request_metric_has_count(
        fixture: &Fixture,
        path: &'static str,
        status: &'static str,
        expected: u64,
    ) {
        let metrics = fixture
            .test_storage
            .metric_registry
            .get_instrument::<Metric<U64Counter>>("grpc_requests")
            .unwrap();

        let observation = metrics
            .get_observer(&Attributes::from([
                (
                    "path",
                    format!("/influxdata.platform.storage.Storage/{path}").into(),
                ),
                ("status", status.into()),
            ]))
            .unwrap()
            .fetch();

        assert_eq!(
            observation, expected,
            "\n\npath: {path}\nstatus:{status}\nobservation:{observation}\nexpected:{expected}\n\nAll metrics:\n\n{metrics:#?}"
        );
    }

    #[tokio::test]
    async fn test_storage_rpc_capabilities() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        // Test response from storage server
        let mut expected_capabilities: HashMap<String, Vec<String>> = HashMap::new();
        expected_capabilities.insert("KeySortCapability".into(), to_str_vec(&["ReadFilter"]));
        expected_capabilities.insert(
            "TagKeyMetaNamesCapability".into(),
            to_str_vec(&["TagKeyMetaNamesWindowAggregate"]),
        );
        expected_capabilities.insert("Group".into(), to_str_vec(&["First", "Last", "Min", "Max"]));
        expected_capabilities.insert(
            "WindowAggregate".into(),
            to_str_vec(&["Count", "Sum", "Min", "Max", "Mean"]),
        );

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
            .with_tag_column("state")
            .with_time_column_with_stats(Some(1000), Some(1000))
            .with_one_row_of_data();

        let chunk1 = TestChunk::new("o2")
            .with_id(1)
            .with_tag_column("state")
            .with_time_column_with_stats(Some(1000), Some(1000))
            .with_one_row_of_data();

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
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
            start: 900,
            end: 1100,
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

        // --- general predicate
        let request = MeasurementNamesRequest {
            source: Some(StorageClient::read_source(&db_info, 1)),
            range: Some(TimestampRange {
                start: 900,
                end: 1100,
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
    /// the right parameters are passed into the Namespace interface
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
            .with_tag_column("state")
            .with_tag_column("k1")
            .with_tag_column("k2")
            .with_time_column()
            .with_one_row_of_data();

        let chunk1 = TestChunk::new("m2")
            .with_id(1)
            .with_tag_column("state")
            .with_tag_column("k3")
            .with_tag_column("k4")
            .with_time_column()
            .with_one_row_of_data();

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .add_chunk("my_partition_key", Arc::new(chunk0))
            .add_chunk("my_partition_key", Arc::new(chunk1));

        let source = Some(StorageClient::read_source(&db_info, 1));

        let request = TagKeysRequest {
            tags_source: source.clone(),
            range: Some(make_timestamp_range(950, 1050)),
            predicate: Some(make_state_eq_ma_predicate()),
        };

        let actual_tag_keys = fixture.storage_client.tag_keys(request).await.unwrap();
        let expected_tag_keys = vec!["_f(0xff)", "_m(0x00)", "k1", "k2", "k3", "k4", "state"];

        assert_eq!(actual_tag_keys, expected_tag_keys,);

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
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

        // ---
        // test error
        // ---
        let request = TagKeysRequest {
            tags_source: source.clone(),
            // add some time range or predicate, otherwise we do a pure metadata lookup
            range: Some(make_timestamp_range(150, 200)),
            predicate: None,
        };

        let response = fixture.storage_client.tag_keys(request).await;
        assert_contains!(response.unwrap_err().to_string(), "Sugar we are going down");

        grpc_request_metric_has_count(&fixture, "TagKeys", "server_error", 1);
    }

    /// test the plumbing of the RPC layer for measurement_tag_keys--
    /// specifically that the right parameters are passed into the Namespace
    /// interface and that the returned values are sent back via gRPC.
    #[tokio::test]
    async fn test_storage_rpc_measurement_tag_keys() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        let chunk0 = TestChunk::new("m1")
            // predicate specifies m4, so this is filtered out
            .with_tag_column("k0")
            .with_time_column()
            .with_one_row_of_data();

        let chunk1 = TestChunk::new("m4")
            .with_tag_column("state")
            .with_tag_column("k1")
            .with_tag_column("k2")
            .with_tag_column("k3")
            .with_tag_column("k4")
            .with_time_column()
            .with_one_row_of_data();

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .add_chunk("my_partition_key", Arc::new(chunk0))
            .add_chunk("my_partition_key", Arc::new(chunk1));

        let source = Some(StorageClient::read_source(&db_info, 1));

        // ---
        // Timestamp + Predicate
        // ---
        let request = MeasurementTagKeysRequest {
            measurement: "m4".into(),
            source: source.clone(),
            range: Some(make_timestamp_range(950, 1050)),
            predicate: Some(make_state_eq_ma_predicate()),
        };

        let actual_tag_keys = fixture
            .storage_client
            .measurement_tag_keys(request)
            .await
            .unwrap();
        let expected_tag_keys = vec!["_f(0xff)", "_m(0x00)", "k1", "k2", "k3", "k4", "state"];

        assert_eq!(
            actual_tag_keys, expected_tag_keys,
            "unexpected tag keys while getting column names"
        );

        grpc_request_metric_has_count(&fixture, "MeasurementTagKeys", "ok", 1);
    }

    #[tokio::test]
    async fn test_storage_rpc_measurement_tag_keys_error() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        // predicate specifies m5
        let chunk = TestChunk::new("m5").with_error("This is an error");

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

        // ---
        // test error
        // ---
        let request = MeasurementTagKeysRequest {
            measurement: "m5".into(),
            source: source.clone(),
            // add some time range or predicate, otherwise we do a pure metadata lookup
            range: Some(make_timestamp_range(150, 200)),
            predicate: None,
        };

        let response = fixture.storage_client.measurement_tag_keys(request).await;
        assert_contains!(response.unwrap_err().to_string(), "This is an error");

        grpc_request_metric_has_count(&fixture, "MeasurementTagKeys", "server_error", 1);
    }

    /// test the plumbing of the RPC layer for tag_values -- specifically that
    /// the right parameters are passed into the Namespace interface
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

        let chunk = TestChunk::new("h2o")
            .with_tag_column("tag")
            .with_time_column_with_stats(Some(1100), Some(1200))
            .with_one_row_of_data();

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
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

        let chunk = TestChunk::new("my_table")
            .with_tag_column("the_tag_key")
            .with_error("Sugar we are going down");

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
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

        // error from backend error
        grpc_request_metric_has_count(&fixture, "TagValues", "server_error", 1);

        // error from bad utf8
        grpc_request_metric_has_count(&fixture, "TagValues", "client_error", 1);
    }

    #[tokio::test]
    async fn test_storage_rpc_tag_values_grouped_by_measurement_and_tag_key() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();
        let chunk1 = TestChunk::new("table_a")
            .with_time_column()
            .with_id(0)
            .with_tag_column("state")
            .with_one_row_of_data();
        let chunk2 = TestChunk::new("table_b")
            .with_time_column()
            .with_id(1)
            .with_tag_column("state")
            .with_one_row_of_data();

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
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
                "{description} failed"
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

        let chunk = TestChunk::new("m5")
            .with_tag_column("the_tag_key")
            .with_error("Sugar we are going down");

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
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

        // Note we don't set the column_names on the test namespace, so we expect an
        // error
        let response_string = fixture
            .storage_client
            .measurement_tag_values(request)
            .await
            .unwrap_err()
            .to_string();

        assert_contains!(response_string, "Sugar we are going down");

        grpc_request_metric_has_count(&fixture, "MeasurementTagValues", "server_error", 1);
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
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

        let request = ReadFilterRequest {
            read_source: source.clone(),
            range: Some(make_timestamp_range(0, 10000)),
            predicate: Some(make_state_eq_ma_predicate()),
            ..Default::default()
        };

        let frames = fixture
            .storage_client
            .read_filter(request.clone())
            .await
            .unwrap();

        // TODO: encode the actual output in the test case or something
        assert_eq!(
            frames.len(),
            0,
            "unexpected frames returned by query_series"
        );

        grpc_request_metric_has_count(&fixture, "ReadFilter", "ok", 1);
    }

    #[tokio::test]
    async fn test_read_filter_empty_string() {
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
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

        let request = ReadFilterRequest {
            read_source: source.clone(),
            range: Some(make_timestamp_range(0, 10000)),
            predicate: Some(make_tag_predicate("state", "", node::Comparison::Equal)),
            ..Default::default()
        };

        fixture
            .storage_client
            .read_filter(request.clone())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_read_filter_field_as_tag() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let mut fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        // Add a chunk with a field
        let chunk = TestChunk::new("TheMeasurement")
            .with_time_column()
            .with_tag_column("state")
            .with_string_field_column_with_stats("fff", None, None)
            .with_one_row_of_data();

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

        // Create a tag predicate that happens to match the name
        // of a field.
        let request = ReadFilterRequest {
            read_source: source.clone(),
            range: None,
            predicate: Some(make_tag_predicate("fff", "MA", node::Comparison::Equal)),
            ..Default::default()
        };

        let frames = fixture
            .storage_client
            .read_filter(request.clone())
            .await
            .unwrap();

        // should return no data because `fff` is not a tag, it is a field.
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

        // Note we don't set the response on the test namespace, so we expect an error
        let response = fixture.storage_client.read_filter(request).await;
        assert_contains!(response.unwrap_err().to_string(), "Sugar we are going down");

        grpc_request_metric_has_count(&fixture, "ReadFilter", "server_error", 1);
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
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

        let group = generated_types::read_group_request::Group::By as i32;

        // ---
        // test error returned in namespace processing
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

        // Note we don't set the response on the test namespace, so we expect an error
        let response_string = fixture
            .storage_client
            .read_group(request)
            .await
            .unwrap_err()
            .to_string();
        assert_contains!(response_string, "Sugar we are going down");

        grpc_request_metric_has_count(&fixture, "ReadGroup", "server_error", 1);
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
            tag_key_meta_names: TagKeyMetaNames::Text as i32,
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
            tag_key_meta_names: TagKeyMetaNames::Text as i32,
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
            tag_key_meta_names: TagKeyMetaNames::Text as i32,
        };

        let response_string = fixture
            .storage_client
            .read_window_aggregate(request_window)
            .await
            .unwrap_err()
            .to_string();

        assert_contains!(response_string, "Sugar we are going down");

        grpc_request_metric_has_count(&fixture, "ReadWindowAggregate", "server_error", 1);
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

        let chunk = TestChunk::new("TheMeasurement").with_error("Sugar we are going down");

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .add_chunk("my_partition_key", Arc::new(chunk));

        let source = Some(StorageClient::read_source(&db_info, 1));

        // ---
        // test error
        // ---
        let request = MeasurementFieldsRequest {
            source: source.clone(),
            measurement: "TheMeasurement".into(),
            range: Some(make_timestamp_range(i64::MIN, i64::MAX - 2)),
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

    #[derive(Debug, Clone)]
    enum SemaphoredRequest {
        MeasurementFields,
        MeasurementNames,
        MeasurementTagKeys,
        MeasurementTagValues,
        ReadFilter,
        ReadGroup,
        ReadWindowAggregate,
        TagKeys,
        TagValues,
        TagValuesGroupedByMeasurementAndTagKey,
    }

    impl SemaphoredRequest {
        async fn request(&self, service: &StorageService<TestDatabaseStore>) -> Box<dyn Any> {
            let db_info = org_and_bucket();
            let source = Some(StorageClient::read_source(&db_info, 1));

            match self {
                Self::MeasurementFields => {
                    let request = MeasurementFieldsRequest {
                        source: source.clone(),
                        measurement: "TheMeasurement".into(),
                        range: Some(make_timestamp_range(0, 2000)),
                        predicate: Some(make_state_eq_ma_predicate()),
                    };
                    let streaming_resp = service
                        .measurement_fields(tonic::Request::new(request))
                        .await
                        .unwrap();
                    Box::new(streaming_resp) as _
                }
                Self::MeasurementNames => {
                    let request = MeasurementNamesRequest {
                        source: source.clone(),
                        range: None,
                        predicate: None,
                    };
                    let streaming_resp = service
                        .measurement_names(tonic::Request::new(request))
                        .await
                        .unwrap();
                    Box::new(streaming_resp) as _
                }
                Self::MeasurementTagKeys => {
                    let request = MeasurementTagKeysRequest {
                        measurement: "TheMeasurement".into(),
                        source: source.clone(),
                        range: Some(make_timestamp_range(0, 200)),
                        predicate: Some(make_state_eq_ma_predicate()),
                    };
                    let streaming_resp = service
                        .measurement_tag_keys(tonic::Request::new(request))
                        .await
                        .unwrap();
                    Box::new(streaming_resp) as _
                }
                Self::MeasurementTagValues => {
                    let request = MeasurementTagValuesRequest {
                        measurement: "TheMeasurement".into(),
                        source: source.clone(),
                        range: Some(make_timestamp_range(150, 2000)),
                        predicate: Some(make_state_eq_ma_predicate()),
                        tag_key: "state".into(),
                    };
                    let streaming_resp = service
                        .measurement_tag_values(tonic::Request::new(request))
                        .await
                        .unwrap();
                    Box::new(streaming_resp) as _
                }
                Self::ReadFilter => {
                    let request = ReadFilterRequest {
                        read_source: source.clone(),
                        range: Some(make_timestamp_range(0, 10000)),
                        ..Default::default()
                    };
                    let streaming_resp = service
                        .read_filter(tonic::Request::new(request))
                        .await
                        .unwrap();
                    Box::new(streaming_resp) as _
                }
                Self::ReadGroup => {
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
                    let streaming_resp = service
                        .read_group(tonic::Request::new(request))
                        .await
                        .unwrap();
                    Box::new(streaming_resp) as _
                }
                Self::ReadWindowAggregate => {
                    let request = ReadWindowAggregateRequest {
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
                        tag_key_meta_names: TagKeyMetaNames::Text as i32,
                    };
                    let streaming_resp = service
                        .read_window_aggregate(tonic::Request::new(request))
                        .await
                        .unwrap();
                    Box::new(streaming_resp) as _
                }
                Self::TagKeys => {
                    let request = TagKeysRequest {
                        tags_source: source.clone(),
                        range: Some(make_timestamp_range(0, 2000)),
                        predicate: Some(make_state_eq_ma_predicate()),
                    };
                    let streaming_resp = service
                        .tag_keys(tonic::Request::new(request))
                        .await
                        .unwrap();
                    Box::new(streaming_resp) as _
                }
                Self::TagValues => {
                    let request = TagValuesRequest {
                        tags_source: source.clone(),
                        range: Some(make_timestamp_range(0, 2000)),
                        predicate: Some(make_state_eq_ma_predicate()),
                        tag_key: [255].into(),
                    };
                    let streaming_resp = service
                        .tag_values(tonic::Request::new(request))
                        .await
                        .unwrap();
                    Box::new(streaming_resp) as _
                }
                Self::TagValuesGroupedByMeasurementAndTagKey => {
                    let request = TagValuesGroupedByMeasurementAndTagKeyRequest {
                        source: source.clone(),
                        measurement_patterns: vec![],
                        tag_key_predicate: Some(TagKeyPredicate {
                            value: Some(Value::Eq("state".into())),
                        }),
                        condition: None,
                    };
                    let streaming_resp = service
                        .tag_values_grouped_by_measurement_and_tag_key(tonic::Request::new(request))
                        .await
                        .unwrap();
                    Box::new(streaming_resp) as _
                }
            }
        }

        fn all() -> Vec<Self> {
            vec![
                Self::MeasurementFields,
                Self::MeasurementNames,
                Self::MeasurementTagKeys,
                Self::MeasurementTagValues,
                Self::ReadFilter,
                Self::ReadGroup,
                Self::ReadWindowAggregate,
                Self::TagKeys,
                Self::TagValues,
                Self::TagValuesGroupedByMeasurementAndTagKey,
            ]
        }
    }

    #[tokio::test]
    async fn test_query_semaphore() {
        maybe_start_logging();
        let semaphore_size = 2;
        let test_storage = Arc::new(TestDatabaseStore::new_with_semaphore_size(semaphore_size));

        // add some data
        let db_info = org_and_bucket();
        let chunk = TestChunk::new("TheMeasurement")
            .with_time_column()
            .with_tag_column("state")
            .with_one_row_of_data();
        test_storage
            .db_or_create(db_info.db_name())
            .await
            .add_chunk("my_partition_key", Arc::new(chunk));

        // construct request
        for t in SemaphoredRequest::all() {
            println!("Testing with request: {t:?}");
            let service = StorageService {
                db_store: Arc::clone(&test_storage),
            };

            assert_semaphore_metric(
                &test_storage.metric_registry,
                "iox_async_semaphore_permits_total",
                2,
            );
            assert_semaphore_metric(
                &test_storage.metric_registry,
                "iox_async_semaphore_permits_pending",
                0,
            );
            assert_semaphore_metric(
                &test_storage.metric_registry,
                "iox_async_semaphore_permits_acquired",
                0,
            );

            let streaming_resp1 = t.request(&service).await;

            assert_semaphore_metric(
                &test_storage.metric_registry,
                "iox_async_semaphore_permits_total",
                2,
            );
            assert_semaphore_metric(
                &test_storage.metric_registry,
                "iox_async_semaphore_permits_pending",
                0,
            );
            assert_semaphore_metric(
                &test_storage.metric_registry,
                "iox_async_semaphore_permits_acquired",
                1,
            );

            let streaming_resp2 = t.request(&service).await;

            assert_semaphore_metric(
                &test_storage.metric_registry,
                "iox_async_semaphore_permits_total",
                2,
            );
            assert_semaphore_metric(
                &test_storage.metric_registry,
                "iox_async_semaphore_permits_pending",
                0,
            );
            assert_semaphore_metric(
                &test_storage.metric_registry,
                "iox_async_semaphore_permits_acquired",
                2,
            );

            // 3rd request is pending
            let fut = t.request(&service);
            pin!(fut);
            assert_fut_pending(&mut fut).await;

            assert_semaphore_metric(
                &test_storage.metric_registry,
                "iox_async_semaphore_permits_total",
                2,
            );
            assert_semaphore_metric(
                &test_storage.metric_registry,
                "iox_async_semaphore_permits_pending",
                1,
            );
            assert_semaphore_metric(
                &test_storage.metric_registry,
                "iox_async_semaphore_permits_acquired",
                2,
            );

            // free permit
            drop(streaming_resp1);
            let streaming_resp3 = fut.await;

            assert_semaphore_metric(
                &test_storage.metric_registry,
                "iox_async_semaphore_permits_total",
                2,
            );
            assert_semaphore_metric(
                &test_storage.metric_registry,
                "iox_async_semaphore_permits_pending",
                0,
            );
            assert_semaphore_metric(
                &test_storage.metric_registry,
                "iox_async_semaphore_permits_acquired",
                2,
            );

            drop(streaming_resp2);
            drop(streaming_resp3);

            assert_semaphore_metric(
                &test_storage.metric_registry,
                "iox_async_semaphore_permits_total",
                2,
            );
            assert_semaphore_metric(
                &test_storage.metric_registry,
                "iox_async_semaphore_permits_pending",
                0,
            );
            assert_semaphore_metric(
                &test_storage.metric_registry,
                "iox_async_semaphore_permits_acquired",
                0,
            );
        }
    }

    #[tokio::test]
    // ensure that the expected IOx header is included with successes
    async fn test_headers() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let fixture = Fixture::new().await.expect("Connecting to test server");

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
            .add_chunk("my_partition_key", Arc::new(chunk));

        // use the raw gRPR client to examine the headers
        let mut storage_client = storage_client::StorageClient::new(
            fixture.client_connection.clone().into_grpc_connection(),
        );

        let source = Some(StorageClient::read_source(&db_info, 1));

        let request = ReadFilterRequest {
            read_source: source.clone(),
            range: None,
            predicate: None,
            ..Default::default()
        };

        let response = storage_client.read_filter(request).await.unwrap();

        println!("Result is {response:?}");
        assert_eq!(response.metadata().get("storage-type").unwrap(), "iox");
    }

    #[tokio::test]
    // ensure that the expected IOx header is included with errors
    async fn test_headers_error() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        let chunk = TestChunk::new("my_table").with_error("Sugar we are going down");

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .add_chunk("my_partition_key", Arc::new(chunk));

        // use the raw gRPR client to examine the headers
        let mut storage_client = storage_client::StorageClient::new(
            fixture.client_connection.clone().into_grpc_connection(),
        );

        let source = Some(StorageClient::read_source(&db_info, 1));

        let request = ReadFilterRequest {
            read_source: source.clone(),
            range: None,
            predicate: None,
            ..Default::default()
        };

        let response = storage_client.read_filter(request).await.unwrap_err();

        println!("Result is {response:?}");
        assert_eq!(response.metadata().get("storage-type").unwrap(), "iox");
    }

    #[tokio::test]
    async fn test_marshal_errors() {
        test_helpers::maybe_start_logging();
        // Start a test gRPC server on a randomally allocated port
        let fixture = Fixture::new().await.expect("Connecting to test server");

        let db_info = org_and_bucket();

        // Add a chunk with a field
        let chunk = TestChunk::new("TheMeasurement")
            .with_time_column()
            .with_string_field_column_with_stats("str", None, None)
            .with_tag_column("state")
            .with_one_row_of_data();

        fixture
            .test_storage
            .db_or_create(db_info.db_name())
            .await
            .add_chunk("my_partition_key", Arc::new(chunk));

        let mut storage_client = storage_client::StorageClient::new(
            fixture.client_connection.clone().into_grpc_connection(),
        );

        let source = Some(StorageClient::read_source(&db_info, 1));

        let request = ReadWindowAggregateRequest {
            read_source: source.clone(),
            range: Some(make_timestamp_range(1000, 2000)),
            predicate: None,
            window_every: 0,
            offset: 0,
            aggregate: vec![Aggregate {
                r#type: aggregate::AggregateType::Mean as i32,
            }],
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
            tag_key_meta_names: TagKeyMetaNames::Text as i32,
        };

        let tonic_status = storage_client
            .read_window_aggregate(request)
            .await
            .unwrap_err();
        assert!(tonic_status
            .message()
            .contains("Avg does not support inputs of type Utf8"));
        assert_eq!(tonic::Code::InvalidArgument, tonic_status.code());

        let mut rpc_status = GrpcStatus::decode(tonic_status.details()).unwrap();
        assert!(rpc_status
            .message
            .contains("Avg does not support inputs of type Utf8"));
        assert_eq!(tonic::Code::InvalidArgument as i32, rpc_status.code);
        assert_eq!(1, rpc_status.details.len());

        let detail = rpc_status.details.pop().unwrap();
        let influx_err = InfluxDbError::decode(detail.value).unwrap();
        assert_eq!("invalid", influx_err.code);
        assert!(influx_err
            .message
            .contains("Avg does not support inputs of type Utf8"));
        assert_eq!("iox/influxrpc", influx_err.op);
        assert_eq!(None, influx_err.error);
    }

    fn make_timestamp_range(start: i64, end: i64) -> TimestampRange {
        TimestampRange { start, end }
    }

    /// return a gRPC predicate like
    ///
    /// state="MA"
    fn make_state_eq_ma_predicate() -> generated_types::Predicate {
        make_state_predicate(node::Comparison::Equal)
    }

    /// return a gRPC predicate like
    ///
    /// state != "MA"
    fn make_state_neq_ma_predicate() -> generated_types::Predicate {
        make_state_predicate(node::Comparison::NotEqual)
    }

    /// return a gRPC predicate like
    ///
    /// state >= "MA"
    fn make_state_geq_ma_predicate() -> generated_types::Predicate {
        make_state_predicate(node::Comparison::Gte)
    }

    fn make_state_predicate(op: node::Comparison) -> generated_types::Predicate {
        make_tag_predicate("state", "MA", op)
    }

    /// TagRef(tag_name) op tag_value
    fn make_tag_predicate(
        tag_name: impl Into<String>,
        tag_value: impl Into<String>,
        op: node::Comparison,
    ) -> generated_types::Predicate {
        use node::{Type, Value};
        let root = Node {
            node_type: Type::ComparisonExpression as i32,
            value: Some(Value::Comparison(op as i32)),
            children: vec![
                Node {
                    node_type: Type::TagRef as i32,
                    value: Some(Value::TagRefValue(tag_name.into().into_bytes())),
                    children: vec![],
                },
                Node {
                    node_type: Type::Literal as i32,
                    value: Some(Value::StringValue(tag_value.into())),
                    children: vec![],
                },
            ],
        };
        generated_types::Predicate { root: Some(root) }
    }

    /// Convert to a Vec<String> to facilitate comparison with results of client
    fn to_string_vec(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    /// Assert that given future is pending.
    ///
    /// This will try to poll the future a bit to ensure that it is not stuck in tokios task preemption.
    async fn assert_fut_pending<F>(fut: &mut F)
    where
        F: Future + Send + Unpin,
    {
        tokio::select! {
            _ = fut => panic!("future is not pending, yielded"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {},
        };
    }

    fn assert_semaphore_metric(registry: &metric::Registry, name: &'static str, expected: u64) {
        let actual = registry
            .get_instrument::<Metric<U64Gauge>>(name)
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[("semaphore", "query_execution")]))
            .expect("failed to get observer")
            .fetch();
        assert_eq!(actual, expected);
    }
}
