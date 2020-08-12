//! This module contains gRPC service implementatations

// Something about how `tracing::instrument` works triggers a clippy
// warning about complex types
#![allow(clippy::type_complexity)]

use tracing::error;

use delorean::generated_types::{
    delorean_server::Delorean,
    measurement_fields_response::MessageField,
    read_response::{
        frame::Data, DataType, FloatPointsFrame, Frame, GroupFrame, IntegerPointsFrame, SeriesFrame,
    },
    storage_server::Storage,
    CapabilitiesResponse, CreateBucketRequest, CreateBucketResponse, DeleteBucketRequest,
    DeleteBucketResponse, GetBucketsResponse, MeasurementFieldsRequest, MeasurementFieldsResponse,
    MeasurementNamesRequest, MeasurementTagKeysRequest, MeasurementTagValuesRequest, Organization,
    Predicate, ReadFilterRequest, ReadGroupRequest, ReadResponse, ReadSource, StringValuesResponse,
    Tag, TagKeysRequest, TagValuesRequest, TimestampRange,
};
use delorean::id::Id;
use delorean::storage::{
    partitioned_store::{PartitionKeyValues, ReadValues},
    SeriesDataType,
};

use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryInto;
use std::sync::Arc;

use tokio::sync::mpsc;
use tonic::Status;

use crate::server::App;

#[derive(Debug)]
pub struct GrpcServer {
    pub app: Arc<App>,
}

#[tonic::async_trait]
impl Delorean for GrpcServer {
    // TODO: Do we want to keep this gRPC request?
    #[tracing::instrument(level = "debug")]
    async fn create_bucket(
        &self,
        req: tonic::Request<CreateBucketRequest>,
    ) -> Result<tonic::Response<CreateBucketResponse>, Status> {
        let create_bucket_request = req.into_inner();

        let org_id = create_bucket_request
            .org_id
            .try_into()
            .map_err(|_| Status::invalid_argument("org_id did not fit in a u64"))?;
        let bucket = create_bucket_request
            .bucket
            .ok_or_else(|| Status::invalid_argument("missing bucket argument"))?;

        self.app
            .db
            .create_bucket_if_not_exists(org_id, bucket)
            .await
            .map_err(|err| Status::internal(format!("error creating bucket: {}", err)))?;

        Ok(tonic::Response::new(CreateBucketResponse {}))
    }

    // Something in instrument is causing lint warnings about unused braces
    #[allow(unused_braces)]
    #[tracing::instrument(level = "debug")]
    async fn delete_bucket(
        &self,
        _req: tonic::Request<DeleteBucketRequest>,
    ) -> Result<tonic::Response<DeleteBucketResponse>, Status> {
        Ok(tonic::Response::new(DeleteBucketResponse {}))
    }

    #[tracing::instrument(level = "debug")]
    async fn get_buckets(
        &self,
        req: tonic::Request<Organization>,
    ) -> Result<tonic::Response<GetBucketsResponse>, Status> {
        let org = req.into_inner();

        let org_id = org
            .id
            .try_into()
            .map_err(|_| Status::invalid_argument("org_id did not fit in a u64"))?;

        let buckets = self
            .app
            .db
            .buckets(org_id)
            .await
            .map_err(|err| Status::internal(format!("error reading db: {}", err)))?;

        Ok(tonic::Response::new(GetBucketsResponse { buckets }))
    }
}

/// This trait implements extraction of information from all storage gRPC requests. The only method
/// required to implement is `read_source_field` because for some requests the field is named
/// `read_source` and for others it is `tags_source`.
trait GrpcInputs {
    fn read_source_field(&self) -> Option<&prost_types::Any>;

    fn read_source_raw(&self) -> Result<&prost_types::Any, Status> {
        Ok(self
            .read_source_field()
            .ok_or_else(|| Status::invalid_argument("missing read_source"))?)
    }

    fn read_source(&self) -> Result<ReadSource, Status> {
        let raw = self.read_source_raw()?;
        let val = &raw.value[..];
        Ok(prost::Message::decode(val).map_err(|_| {
            Status::invalid_argument("value could not be parsed as a ReadSource message")
        })?)
    }

    fn org_id(&self) -> Result<Id, Status> {
        Ok(self
            .read_source()?
            .org_id
            .try_into()
            .map_err(|_| Status::invalid_argument("org_id did not fit in a u64"))?)
    }

    fn bucket_name(&self) -> Result<String, Status> {
        let bucket: Id = self
            .read_source()?
            .bucket_id
            .try_into()
            .map_err(|_| Status::invalid_argument("bucket_id did not fit in a u64"))?;
        Ok(bucket.to_string())
    }
}

impl GrpcInputs for ReadFilterRequest {
    fn read_source_field(&self) -> Option<&prost_types::Any> {
        self.read_source.as_ref()
    }
}

impl GrpcInputs for ReadGroupRequest {
    fn read_source_field(&self) -> Option<&prost_types::Any> {
        self.read_source.as_ref()
    }
}

impl GrpcInputs for TagKeysRequest {
    fn read_source_field(&self) -> Option<&prost_types::Any> {
        self.tags_source.as_ref()
    }
}

impl GrpcInputs for TagValuesRequest {
    fn read_source_field(&self) -> Option<&prost_types::Any> {
        self.tags_source.as_ref()
    }
}

impl GrpcInputs for MeasurementNamesRequest {
    fn read_source_field(&self) -> Option<&prost_types::Any> {
        self.source.as_ref()
    }
}

impl GrpcInputs for MeasurementTagKeysRequest {
    fn read_source_field(&self) -> Option<&prost_types::Any> {
        self.source.as_ref()
    }
}

impl GrpcInputs for MeasurementTagValuesRequest {
    fn read_source_field(&self) -> Option<&prost_types::Any> {
        self.source.as_ref()
    }
}

impl GrpcInputs for MeasurementFieldsRequest {
    fn read_source_field(&self) -> Option<&prost_types::Any> {
        self.source.as_ref()
    }
}

#[tonic::async_trait]
impl Storage for GrpcServer {
    type ReadFilterStream = mpsc::Receiver<Result<ReadResponse, Status>>;

    #[tracing::instrument(level = "debug")]
    async fn read_filter(
        &self,
        req: tonic::Request<ReadFilterRequest>,
    ) -> Result<tonic::Response<Self::ReadFilterStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);

        let read_filter_request = req.into_inner();

        let org_id = read_filter_request.org_id()?;
        let bucket_name = read_filter_request.bucket_name()?;

        let predicate = read_filter_request.predicate;
        let range = read_filter_request.range;

        let app = Arc::clone(&self.app);

        // TODO: is this blocking because of the blocking calls to the database...?
        tokio::spawn(async move {
            let predicate = predicate.as_ref().expect("TODO: must have a predicate");
            // TODO: The call to read_series_matching_predicate_and_range takes an optional range,
            // but read_f64_range requires a range-- should this route require a range or use a
            // default or something else?
            let range = range.as_ref().expect("TODO: Must have a range?");

            if let Err(e) =
                send_series_filters(tx.clone(), app, org_id, &bucket_name, predicate, &range).await
            {
                tx.send(Err(e)).await.unwrap();
            }
        });

        Ok(tonic::Response::new(rx))
    }

    type ReadGroupStream = mpsc::Receiver<Result<ReadResponse, Status>>;

    #[tracing::instrument(level = "debug")]
    async fn read_group(
        &self,
        req: tonic::Request<ReadGroupRequest>,
    ) -> Result<tonic::Response<Self::ReadGroupStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);

        let read_group_request = req.into_inner();

        let org_id = read_group_request.org_id()?;
        let bucket_name = read_group_request.bucket_name()?;
        let predicate = read_group_request.predicate;
        let range = read_group_request.range;
        let group_keys = read_group_request.group_keys;
        // TODO: handle Group::None
        let _group = read_group_request.group;
        // TODO: handle aggregate values, especially whether None is the same as
        // Some(AggregateType::None) or not
        let _aggregate = read_group_request.aggregate;

        let app = Arc::clone(&self.app);

        // TODO: is this blocking because of the blocking calls to the database...?
        tokio::spawn(async move {
            let predicate = predicate.as_ref().expect("TODO: must have a predicate");
            let range = range.as_ref().expect("TODO: Must have a range?");

            if let Err(e) = send_groups(
                tx.clone(),
                app,
                org_id,
                &bucket_name,
                predicate,
                range,
                group_keys,
            )
            .await
            {
                tx.send(Err(e)).await.unwrap();
            }
        });

        Ok(tonic::Response::new(rx))
    }

    type TagKeysStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    #[tracing::instrument(level = "debug")]
    async fn tag_keys(
        &self,
        req: tonic::Request<TagKeysRequest>,
    ) -> Result<tonic::Response<Self::TagKeysStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);

        let tag_keys_request = req.into_inner();

        let org_id = tag_keys_request.org_id()?;
        let bucket_name = tag_keys_request.bucket_name()?;
        let predicate = tag_keys_request.predicate;
        let range = tag_keys_request.range;

        let app = self.app.clone();

        let bucket_id = app
            .db
            .get_bucket_id_by_name(org_id, &bucket_name)
            .await
            .map_err(|err| Status::internal(format!("error reading db: {}", err)))?
            .ok_or_else(|| Status::internal("bucket not found"))?;

        tokio::spawn(async move {
            match app
                .db
                .get_tag_keys(org_id, bucket_id, predicate.as_ref(), range.as_ref())
                .await
            {
                Err(e) => {
                    error!("Error retrieving tag keys: {:?}", e);
                    tx.send(Err(Status::internal("could not query for tag keys")))
                        .await
                        .unwrap()
                }
                Ok(tag_keys) => {
                    // TODO: Should these be batched? If so, how?
                    let tag_keys: Vec<_> = tag_keys
                        .into_iter()
                        .map(|s| transform_key_to_long_form_bytes(&s))
                        .collect();
                    tx.send(Ok(StringValuesResponse { values: tag_keys }))
                        .await
                        .unwrap();
                }
            }
        });

        Ok(tonic::Response::new(rx))
    }

    type TagValuesStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    #[tracing::instrument(level = "debug")]
    async fn tag_values(
        &self,
        req: tonic::Request<TagValuesRequest>,
    ) -> Result<tonic::Response<Self::TagValuesStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);

        let tag_values_request = req.into_inner();

        let org_id = tag_values_request.org_id()?;
        let bucket_name = tag_values_request.bucket_name()?;
        let predicate = tag_values_request.predicate;
        let range = tag_values_request.range;

        let tag_key = tag_values_request.tag_key;

        let app = self.app.clone();

        let bucket_id = app
            .db
            .get_bucket_id_by_name(org_id, &bucket_name)
            .await
            .map_err(|err| Status::internal(format!("error reading db: {}", err)))?
            .ok_or_else(|| Status::internal("bucket not found"))?;

        tokio::spawn(async move {
            match app
                .db
                .get_tag_values(
                    org_id,
                    bucket_id,
                    &tag_key,
                    predicate.as_ref(),
                    range.as_ref(),
                )
                .await
            {
                Err(_) => tx
                    .send(Err(Status::internal("could not query for tag values")))
                    .await
                    .unwrap(),
                Ok(tag_values) => {
                    // TODO: Should these be batched? If so, how?
                    let tag_values: Vec<_> =
                        tag_values.into_iter().map(|s| s.into_bytes()).collect();
                    tx.send(Ok(StringValuesResponse { values: tag_values }))
                        .await
                        .unwrap();
                }
            }
        });

        Ok(tonic::Response::new(rx))
    }

    #[tracing::instrument(level = "debug")]
    async fn capabilities(
        &self,
        req: tonic::Request<()>,
    ) -> Result<tonic::Response<CapabilitiesResponse>, Status> {
        // Full list of go capabilities in idpe/storage/read/capabilities.go (aka window aggregate / pushdown)
        //
        // For now, do not claim to support any capabilities
        let caps = CapabilitiesResponse {
            caps: std::collections::HashMap::new(),
        };
        Ok(tonic::Response::new(caps))
    }

    type MeasurementNamesStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    #[tracing::instrument(level = "debug")]
    async fn measurement_names(
        &self,
        req: tonic::Request<MeasurementNamesRequest>,
    ) -> Result<tonic::Response<Self::MeasurementNamesStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);

        let measurement_names_request = req.into_inner();

        let org_id = measurement_names_request.org_id()?;
        let bucket_name = measurement_names_request.bucket_name()?;
        let range = measurement_names_request.range;

        let app = self.app.clone();

        let bucket_id = app
            .db
            .get_bucket_id_by_name(org_id, &bucket_name)
            .await
            .map_err(|err| Status::internal(format!("error reading db: {}", err)))?
            .ok_or_else(|| Status::internal("bucket not found"))?;

        tokio::spawn(async move {
            match app
                .db
                .get_measurement_names(org_id, bucket_id, range.as_ref())
                .await
            {
                Err(_) => tx
                    .send(Err(Status::internal(
                        "could not query for measurement names",
                    )))
                    .await
                    .unwrap(),
                Ok(measurement_names) => {
                    // TODO: Should these be batched? If so, how?
                    let measurement_names: Vec<_> = measurement_names
                        .into_iter()
                        .map(|s| transform_key_to_long_form_bytes(&s))
                        .collect();
                    tx.send(Ok(StringValuesResponse {
                        values: measurement_names,
                    }))
                    .await
                    .unwrap();
                }
            }
        });

        Ok(tonic::Response::new(rx))
    }

    type MeasurementTagKeysStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    #[tracing::instrument(level = "debug")]
    async fn measurement_tag_keys(
        &self,
        req: tonic::Request<MeasurementTagKeysRequest>,
    ) -> Result<tonic::Response<Self::MeasurementTagKeysStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);

        let measurement_tag_keys_request = req.into_inner();

        let org_id = measurement_tag_keys_request.org_id()?;
        let bucket_name = measurement_tag_keys_request.bucket_name()?;
        let measurement = measurement_tag_keys_request.measurement;
        let predicate = measurement_tag_keys_request.predicate;
        let range = measurement_tag_keys_request.range;

        let app = self.app.clone();

        let bucket_id = app
            .db
            .get_bucket_id_by_name(org_id, &bucket_name)
            .await
            .map_err(|err| Status::internal(format!("error reading db: {}", err)))?
            .ok_or_else(|| Status::internal("bucket not found"))?;

        tokio::spawn(async move {
            match app
                .db
                .get_measurement_tag_keys(
                    org_id,
                    bucket_id,
                    &measurement,
                    predicate.as_ref(),
                    range.as_ref(),
                )
                .await
            {
                Err(_) => tx
                    .send(Err(Status::internal(
                        "could not query for measurement tag keys",
                    )))
                    .await
                    .unwrap(),
                Ok(measurement_tag_keys) => {
                    // TODO: Should these be batched? If so, how?
                    let measurement_tag_keys: Vec<_> = measurement_tag_keys
                        .into_iter()
                        .map(|s| transform_key_to_long_form_bytes(&s))
                        .collect();
                    tx.send(Ok(StringValuesResponse {
                        values: measurement_tag_keys,
                    }))
                    .await
                    .unwrap();
                }
            }
        });

        Ok(tonic::Response::new(rx))
    }

    type MeasurementTagValuesStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    #[tracing::instrument(level = "debug")]
    async fn measurement_tag_values(
        &self,
        req: tonic::Request<MeasurementTagValuesRequest>,
    ) -> Result<tonic::Response<Self::MeasurementTagValuesStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);

        let measurement_tag_values_request = req.into_inner();

        let org_id = measurement_tag_values_request.org_id()?;
        let bucket_name = measurement_tag_values_request.bucket_name()?;
        let measurement = measurement_tag_values_request.measurement;
        let tag_key = measurement_tag_values_request.tag_key;
        let predicate = measurement_tag_values_request.predicate;
        let range = measurement_tag_values_request.range;

        let app = self.app.clone();

        let bucket_id = app
            .db
            .get_bucket_id_by_name(org_id, &bucket_name)
            .await
            .map_err(|err| Status::internal(format!("error reading db: {}", err)))?
            .ok_or_else(|| Status::internal("bucket not found"))?;

        tokio::spawn(async move {
            match app
                .db
                .get_measurement_tag_values(
                    org_id,
                    bucket_id,
                    &measurement,
                    &tag_key,
                    predicate.as_ref(),
                    range.as_ref(),
                )
                .await
            {
                Err(_) => tx
                    .send(Err(Status::internal(
                        "could not query for measurement tag values",
                    )))
                    .await
                    .unwrap(),
                Ok(measurement_tag_values) => {
                    // TODO: Should these be batched? If so, how?
                    let measurement_tag_values: Vec<_> = measurement_tag_values
                        .into_iter()
                        .map(|s| transform_key_to_long_form_bytes(&s))
                        .collect();
                    tx.send(Ok(StringValuesResponse {
                        values: measurement_tag_values,
                    }))
                    .await
                    .unwrap();
                }
            }
        });

        Ok(tonic::Response::new(rx))
    }

    type MeasurementFieldsStream = mpsc::Receiver<Result<MeasurementFieldsResponse, Status>>;

    #[tracing::instrument(level = "debug")]
    async fn measurement_fields(
        &self,
        req: tonic::Request<MeasurementFieldsRequest>,
    ) -> Result<tonic::Response<Self::MeasurementFieldsStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);

        let measurement_fields_request = req.into_inner();

        let org_id = measurement_fields_request.org_id()?;
        let bucket_name = measurement_fields_request.bucket_name()?;
        let measurement = measurement_fields_request.measurement;
        let predicate = measurement_fields_request.predicate;
        let range = measurement_fields_request.range;

        let app = self.app.clone();

        let bucket_id = app
            .db
            .get_bucket_id_by_name(org_id, &bucket_name)
            .await
            .map_err(|err| Status::internal(format!("error reading db: {}", err)))?
            .ok_or_else(|| Status::internal("bucket not found"))?;

        tokio::spawn(async move {
            match app
                .db
                .get_measurement_fields(
                    org_id,
                    bucket_id,
                    &measurement,
                    predicate.as_ref(),
                    range.as_ref(),
                )
                .await
            {
                Err(_) => tx
                    .send(Err(Status::internal(
                        "could not query for measurement fields",
                    )))
                    .await
                    .unwrap(),
                Ok(measurement_fields) => {
                    // TODO: Should these be batched? If so, how?
                    let measurement_fields: Vec<_> = measurement_fields
                        .into_iter()
                        .map(|(field_key, field_type, timestamp)| {
                            let field_type = match field_type {
                                SeriesDataType::F64 => DataType::Float,
                                SeriesDataType::I64 => DataType::Integer,
                            } as _;

                            MessageField {
                                key: field_key,
                                r#type: field_type,
                                timestamp,
                            }
                        })
                        .collect();
                    tx.send(Ok(MeasurementFieldsResponse {
                        fields: measurement_fields,
                    }))
                    .await
                    .unwrap();
                }
            }
        });

        Ok(tonic::Response::new(rx))
    }
}

#[tracing::instrument(level = "debug")]
async fn send_series_filters(
    mut tx: mpsc::Sender<Result<ReadResponse, Status>>,
    app: Arc<App>,
    org_id: Id,
    bucket_name: &str,
    predicate: &Predicate,
    range: &TimestampRange,
) -> Result<(), Status> {
    let bucket_id = app
        .db
        .get_bucket_id_by_name(org_id, bucket_name)
        .await
        .map_err(|err| Status::internal(format!("error reading db: {}", err)))?
        .ok_or_else(|| Status::internal("bucket not found"))?;

    let batches = app
        .db
        .read_points(org_id, bucket_id, predicate, range)
        .await
        .map_err(|err| Status::internal(format!("error reading db: {}", err)))?;

    let mut last_frame_key = String::new();

    for batch in batches {
        // only send the series frame header if we haven't sent it for this key. We have to do
        // this because a single series key can be spread out over multiple ReadBatches, which
        // should each be sent as their own data frames
        if last_frame_key != batch.key {
            last_frame_key = batch.key.clone();

            let tags = batch
                .tags()
                .into_iter()
                .map(|(key, value)| {
                    let key = transform_key_to_long_form_bytes(&key);
                    Tag {
                        key,
                        value: value.bytes().collect(),
                    }
                })
                .collect();

            let data_type = match batch.values {
                ReadValues::F64(_) => DataType::Float,
                ReadValues::I64(_) => DataType::Integer,
            } as _;

            let series_frame_response_header = Ok(ReadResponse {
                frames: vec![Frame {
                    data: Some(Data::Series(SeriesFrame { data_type, tags })),
                }],
            });

            tx.send(series_frame_response_header).await.unwrap();
        }

        if let Err(e) = send_points(tx.clone(), batch.values).await {
            tx.send(Err(e)).await.unwrap();
        }
    }

    Ok(())
}

#[tracing::instrument(level = "debug")]
async fn send_groups(
    mut tx: mpsc::Sender<Result<ReadResponse, Status>>,
    app: Arc<App>,
    org_id: Id,
    bucket_name: &str,
    predicate: &Predicate,
    range: &TimestampRange,
    group_keys: Vec<String>,
) -> Result<(), Status> {
    let bucket_id = app
        .db
        .get_bucket_id_by_name(org_id, bucket_name)
        .await
        .map_err(|err| Status::internal(format!("error reading db: {}", err)))?
        .ok_or_else(|| Status::internal("bucket not found"))?;

    // Query for all the batches that should be returned.
    let batches = app
        .db
        .read_points(org_id, bucket_id, predicate, range)
        .await
        .map_err(|err| Status::internal(format!("error reading db: {}", err)))?;

    // Group the batches by the values they have for the group_keys.
    let mut batches_by_group = BTreeMap::new();
    for batch in batches {
        let partition_key_values = PartitionKeyValues::new(&group_keys, &batch);

        let entry = batches_by_group
            .entry(partition_key_values)
            .or_insert_with(Vec::new);
        entry.push(batch);
    }

    for (partition_key_values, batches) in batches_by_group {
        // Unify all the tag keys present in all of the batches in this group.
        let tag_keys: BTreeSet<_> = batches
            .iter()
            .map(|batch| batch.tag_keys())
            .flatten()
            .collect();

        let group_frame = ReadResponse {
            frames: vec![Frame {
                data: Some(Data::Group(GroupFrame {
                    tag_keys: tag_keys
                        .iter()
                        .map(|tk| transform_key_to_long_form_bytes(tk))
                        .collect(),
                    partition_key_vals: partition_key_values
                        .values
                        .iter()
                        .map(|tv| {
                            tv.as_ref()
                                .map(|opt| opt.as_bytes().to_vec())
                                .unwrap_or_else(Vec::new)
                        })
                        .collect(),
                })),
            }],
        };

        tx.send(Ok(group_frame)).await.unwrap();

        for batch in batches {
            if let Err(e) = send_points(tx.clone(), batch.values).await {
                tx.send(Err(e)).await.unwrap();
            }
        }
    }

    Ok(())
}

async fn send_points(
    mut tx: mpsc::Sender<Result<ReadResponse, Status>>,
    read_values: ReadValues,
) -> Result<(), Status> {
    match read_values {
        ReadValues::F64(values) => {
            let (timestamps, values) = values.into_iter().map(|p| (p.time, p.value)).unzip();
            let data_frame_response = Ok(ReadResponse {
                frames: vec![Frame {
                    data: Some(Data::FloatPoints(FloatPointsFrame { timestamps, values })),
                }],
            });

            tx.send(data_frame_response).await.unwrap();
        }
        ReadValues::I64(values) => {
            let (timestamps, values) = values.into_iter().map(|p| (p.time, p.value)).unzip();
            let data_frame_response = Ok(ReadResponse {
                frames: vec![Frame {
                    data: Some(Data::IntegerPoints(IntegerPointsFrame {
                        timestamps,
                        values,
                    })),
                }],
            });

            tx.send(data_frame_response).await.unwrap();
        }
    }

    Ok(())
}

fn transform_key_to_long_form_bytes(key: &str) -> Vec<u8> {
    match key {
        "_f" => b"_field".to_vec(),
        "_m" => b"_measurement".to_vec(),
        other => other.bytes().collect(),
    }
}
