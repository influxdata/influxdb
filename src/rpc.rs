use delorean::delorean::{
    delorean_server::Delorean, storage_server::Storage, Bucket, CapabilitiesResponse,
    CreateBucketRequest, CreateBucketResponse, DeleteBucketRequest, DeleteBucketResponse,
    GetBucketsResponse, Organization, ReadFilterRequest, ReadGroupRequest, ReadResponse,
    ReadSource, StringValuesResponse, TagKeysRequest, TagValuesRequest,
};
use delorean::storage::database::Database;

use std::convert::TryInto;
use std::sync::Arc;

use tokio::sync::mpsc;
use tonic::Status;

use crate::App;

pub struct GrpcServer {
    pub app: Arc<App>,
}

#[tonic::async_trait]
impl Delorean for GrpcServer {
    async fn create_bucket(
        &self,
        _req: tonic::Request<CreateBucketRequest>,
    ) -> Result<tonic::Response<CreateBucketResponse>, Status> {
        Ok(tonic::Response::new(CreateBucketResponse {}))
    }

    async fn delete_bucket(
        &self,
        _req: tonic::Request<DeleteBucketRequest>,
    ) -> Result<tonic::Response<DeleteBucketResponse>, Status> {
        Ok(tonic::Response::new(DeleteBucketResponse {}))
    }

    async fn get_buckets(
        &self,
        _req: tonic::Request<Organization>,
    ) -> Result<tonic::Response<GetBucketsResponse>, Status> {
        Ok(tonic::Response::new(GetBucketsResponse { buckets: vec![] }))
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

    fn org_id(&self) -> Result<u32, Status> {
        Ok(self
            .read_source()?
            .org_id
            .try_into()
            .map_err(|_| Status::invalid_argument("org_id did not fit in a u32"))?)
    }

    fn bucket(&self, db: &Database) -> Result<Arc<Bucket>, Status> {
        let bucket_id = self
            .read_source()?
            .bucket_id
            .try_into()
            .map_err(|_| Status::invalid_argument("bucket_id did not fit in a u32"))?;

        let maybe_bucket = db
            .get_bucket_by_id(bucket_id)
            .map_err(|_| Status::internal("could not query for bucket"))?;

        Ok(maybe_bucket
            .ok_or_else(|| Status::not_found(&format!("bucket {} not found", bucket_id)))?)
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

#[tonic::async_trait]
impl Storage for GrpcServer {
    type ReadFilterStream = mpsc::Receiver<Result<ReadResponse, Status>>;

    async fn read_filter(
        &self,
        _req: tonic::Request<ReadFilterRequest>,
    ) -> Result<tonic::Response<Self::ReadFilterStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type ReadGroupStream = mpsc::Receiver<Result<ReadResponse, Status>>;

    async fn read_group(
        &self,
        _req: tonic::Request<ReadGroupRequest>,
    ) -> Result<tonic::Response<Self::ReadGroupStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type TagKeysStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    async fn tag_keys(
        &self,
        req: tonic::Request<TagKeysRequest>,
    ) -> Result<tonic::Response<Self::TagKeysStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);

        let tag_keys_request = req.get_ref();

        let _org_id = tag_keys_request.org_id()?;
        let bucket = tag_keys_request.bucket(&self.app.db)?;
        let predicate = tag_keys_request.predicate.clone();
        let _range = tag_keys_request.range.as_ref();

        let app = self.app.clone();

        tokio::spawn(async move {
            match app.db.get_tag_keys(&bucket, predicate.as_ref()) {
                Err(_) => tx
                    .send(Err(Status::internal("could not query for tag keys")))
                    .await
                    .unwrap(),
                Ok(tag_keys_iter) => {
                    // TODO: Should these be batched? If so, how?
                    let tag_keys: Vec<_> = tag_keys_iter.map(|s| s.into_bytes()).collect();
                    tx.send(Ok(StringValuesResponse { values: tag_keys }))
                        .await
                        .unwrap();
                }
            }
        });

        Ok(tonic::Response::new(rx))
    }

    type TagValuesStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    async fn tag_values(
        &self,
        _req: tonic::Request<TagValuesRequest>,
    ) -> Result<tonic::Response<Self::TagValuesStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn capabilities(
        &self,
        _: tonic::Request<()>,
    ) -> Result<tonic::Response<CapabilitiesResponse>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}
