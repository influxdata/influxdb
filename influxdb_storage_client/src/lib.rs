//! An InfluxDB gRPC storage API client
#![deny(
    rustdoc::broken_intra_doc_links,
    rustdoc::bare_urls,
    rust_2018_idioms,
    missing_debug_implementations,
    unreachable_pub
)]
#![warn(
    missing_docs,
    clippy::todo,
    clippy::dbg_macro,
    clippy::clone_on_ref_ptr,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]
#![allow(clippy::missing_docs_in_private_items)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use client_util::connection::GrpcConnection;
use futures_util::TryStreamExt;
use prost::Message;
use std::collections::HashMap;
use std::fmt::Debug;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::platform::storage::*;
}

pub use client_util::connection;

use self::connection::Connection;
use self::generated_types::*;
use ::generated_types::google::protobuf::*;
use observability_deps::tracing::{debug, trace};
use std::num::NonZeroU64;

/// InfluxDB IOx deals with namespace names. The gRPC interface deals with org_id and bucket_id
/// represented as 16 digit hex values. This struct manages creating the org_id, bucket_id, and
/// namespace names to be consistent with the implementation.
#[derive(Debug, Clone)]
pub struct OrgAndBucket {
    org_id: NonZeroU64,
    bucket_id: NonZeroU64,
    db_name: String,
}

impl OrgAndBucket {
    /// Create a new `OrgAndBucket` from the provided `org_id` and `bucket_id`
    pub fn new(org_id: NonZeroU64, bucket_id: NonZeroU64) -> Self {
        let db_name = format!("{org_id:016x}_{bucket_id:016x}");

        Self {
            org_id,
            bucket_id,
            db_name,
        }
    }

    /// Get the `org_id`
    pub fn org_id(&self) -> NonZeroU64 {
        self.org_id
    }

    /// Get the `bucket_id`
    pub fn bucket_id(&self) -> NonZeroU64 {
        self.bucket_id
    }

    /// Get the `db_name` generated from the provided org and bucket ids
    pub fn db_name(&self) -> &str {
        &self.db_name
    }
}

/// A client for the InfluxDB gRPC storage API
#[derive(Debug)]
pub struct Client {
    inner: storage_client::StorageClient<GrpcConnection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(connection: Connection) -> Self {
        Self {
            inner: storage_client::StorageClient::new(
                connection
                    .log_debug("creating with connection")
                    .into_grpc_connection(),
            ),
        }
    }

    /// Create a ReadSource suitable for constructing messages
    pub fn read_source(bucket: &OrgAndBucket, partition_id: u64) -> Any {
        let read_source = ReadSource {
            org_id: bucket.org_id.get(),
            bucket_id: bucket.bucket_id.get(),
            partition_id,
        };
        let mut d = prost::bytes::BytesMut::new();
        read_source
            .encode(&mut d)
            .expect("encoded read source appropriately");
        Any {
            type_url: "type.googleapis.com/influxdata.platform.storage.read.ReadSource".to_string(),
            value: d.freeze(),
        }
    }

    /// return the capabilities of the server as a hash map
    pub async fn capabilities(&mut self) -> Result<HashMap<String, Vec<String>>, tonic::Status> {
        let response = self
            .inner
            .capabilities(Empty {})
            .await
            .log_trace("capabilities response")?
            .into_inner();

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
    pub async fn measurement_names(
        &mut self,
        request: MeasurementNamesRequest,
    ) -> Result<Vec<String>, tonic::Status> {
        let request = request.log_trace("measurement_names request");
        let responses = self
            .inner
            .measurement_names(request)
            .await
            .log_trace("measurement_names response")?
            .into_inner()
            .try_collect()
            .await?;

        Ok(Self::collect_strings(responses))
    }

    /// Make a request to query::read_window_aggregate and do the
    /// required async dance to flatten the resulting stream to Strings
    pub async fn read_window_aggregate(
        &mut self,
        request: ReadWindowAggregateRequest,
    ) -> Result<Vec<read_response::frame::Data>, tonic::Status> {
        let request = request.log_trace("read_window_aggregate request");
        let responses: Vec<_> = self
            .inner
            .read_window_aggregate(request)
            .await
            .log_trace("read_window_aggregate response")?
            .into_inner()
            .try_collect()
            .await?;

        Ok(Self::collect_data(responses))
    }

    /// Make a request to query::tag_keys and do the
    /// required async dance to flatten the resulting stream to Strings
    pub async fn tag_keys(
        &mut self,
        request: TagKeysRequest,
    ) -> Result<Vec<String>, tonic::Status> {
        let request = request.log_trace("tag_keys request");
        let responses = self
            .inner
            .tag_keys(request)
            .await
            .log_trace("tag_keys response")?
            .into_inner()
            .try_collect()
            .await?;

        Ok(Self::collect_strings(responses))
    }

    /// Make a request to query::measurement_tag_keys and do the
    /// required async dance to flatten the resulting stream to Strings
    pub async fn measurement_tag_keys(
        &mut self,
        request: MeasurementTagKeysRequest,
    ) -> Result<Vec<String>, tonic::Status> {
        let request = request.log_trace("measurement_tag_keys request");
        let responses = self
            .inner
            .measurement_tag_keys(request)
            .await
            .log_trace("measurement_tag_keys response")?
            .into_inner()
            .try_collect()
            .await?;

        Ok(Self::collect_strings(responses))
    }

    /// Make a request to query::tag_values and do the
    /// required async dance to flatten the resulting stream to Strings
    pub async fn tag_values(
        &mut self,
        request: TagValuesRequest,
    ) -> Result<Vec<String>, tonic::Status> {
        let request = request.log_trace("tag_values request");
        let responses = self
            .inner
            .tag_values(request)
            .await
            .log_trace("tag_values response")?
            .into_inner()
            .try_collect()
            .await?;

        Ok(Self::collect_strings(responses))
    }

    /// Make a request to query::tag_values_grouped_by_measurement_and_tag_key
    /// and do the required async dance to flatten the resulting stream
    pub async fn tag_values_grouped_by_measurement_and_tag_key(
        &mut self,
        request: TagValuesGroupedByMeasurementAndTagKeyRequest,
    ) -> Result<Vec<TagValuesResponse>, tonic::Status> {
        let request = request.log_trace("tag_values_grouped_by_measurement_and_tag_key request");
        let responses: Vec<_> = self
            .inner
            .tag_values_grouped_by_measurement_and_tag_key(request)
            .await
            .log_trace("tag_values_grouped_by_measurement_and_tag_key response")?
            .into_inner()
            .try_collect()
            .await?;

        Ok(responses)
    }

    /// Make a request to query::measurement_tag_values and do the
    /// required async dance to flatten the resulting stream to Strings
    pub async fn measurement_tag_values(
        &mut self,
        request: MeasurementTagValuesRequest,
    ) -> Result<Vec<String>, tonic::Status> {
        let request = request.log_trace("measurement_tag_values request");
        let responses = self
            .inner
            .measurement_tag_values(request)
            .await
            .log_trace("measurement_tag_values response")?
            .into_inner()
            .try_collect()
            .await?;

        Ok(Self::collect_strings(responses))
    }

    /// Make a request to query::read_filter and do the
    /// required async dance to flatten the resulting stream
    pub async fn read_filter(
        &mut self,
        request: ReadFilterRequest,
    ) -> Result<Vec<read_response::frame::Data>, tonic::Status> {
        let request = request.log_trace("read_filter request");
        let responses: Vec<_> = self
            .inner
            .read_filter(request)
            .await
            .log_trace("read_filter response")?
            .into_inner()
            .try_collect()
            .await?;

        Ok(Self::collect_data(responses))
    }

    /// Make a request to query::query_groups and do the
    /// required async dance to flatten the resulting stream
    pub async fn read_group(
        &mut self,
        request: ReadGroupRequest,
    ) -> Result<Vec<read_response::frame::Data>, tonic::Status> {
        let request = request.log_trace("read_group request");
        let responses: Vec<_> = self
            .inner
            .read_group(request)
            .await
            .log_trace("read_group response")?
            .into_inner()
            .try_collect()
            .await?;

        Ok(Self::collect_data(responses))
    }

    /// Make a request to query::measurement_fields and do the
    /// required async dance to flatten the resulting stream to Strings
    pub async fn measurement_fields(
        &mut self,
        request: MeasurementFieldsRequest,
    ) -> Result<Vec<String>, tonic::Status> {
        let request = request.log_trace("measurement_fields request");
        let measurement_fields_response = self.inner.measurement_fields(request).await?;

        let responses: Vec<_> = measurement_fields_response
            .into_inner()
            .try_collect::<Vec<_>>()
            .await
            .log_trace("measurement_fields response")?
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

    /// Extract the data frames from the list of ReadResponse
    fn collect_data(responses: Vec<ReadResponse>) -> Vec<read_response::frame::Data> {
        responses
            .into_iter()
            .flat_map(|r| r.frames)
            .flat_map(|f| f.data)
            .collect()
    }

    /// Convert the StringValueResponses into rust Strings, sorting the
    /// values to ensure consistency.
    fn collect_strings(responses: Vec<StringValuesResponse>) -> Vec<String> {
        let mut strings = responses
            .into_iter()
            .flat_map(|r| r.values.into_iter())
            .map(tag_key_bytes_to_strings)
            .collect::<Vec<_>>();

        strings.sort();

        strings
    }
}

/// Converts bytes representing tag_keys values to Rust strings,
/// handling the special case `_m(0x00)` and `_f(0xff)` values. Other
/// than `0xff` panics on any non-utf8 string.
pub fn tag_key_bytes_to_strings(bytes: Vec<u8>) -> String {
    match bytes.as_slice() {
        [0] => "_m(0x00)".into(),
        // note this isn't valid UTF8 and thus would assert below
        [255] => "_f(0xff)".into(),
        _ => String::from_utf8(bytes).expect("string value response was not utf8"),
    }
}

/// Logs the specific item
trait Loggable {
    fn log_trace(self, msg: &'static str) -> Self;
    fn log_debug(self, msg: &'static str) -> Self;
}

impl<T: Debug> Loggable for T {
    fn log_trace(self, msg: &'static str) -> Self {
        trace!(data=?self, "{}", msg);
        self
    }

    fn log_debug(self, msg: &'static str) -> Self {
        debug!(data=?self, "{}", msg);
        self
    }
}
