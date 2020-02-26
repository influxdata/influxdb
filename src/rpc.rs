use delorean::delorean::{
    delorean_server::Delorean, storage_server::Storage, CapabilitiesResponse, CreateBucketRequest,
    CreateBucketResponse, DeleteBucketRequest, DeleteBucketResponse, GetBucketsResponse,
    Organization, ReadFilterRequest, ReadGroupRequest, ReadResponse, StringValuesResponse,
    TagKeysRequest, TagValuesRequest,
};

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
        _req: tonic::Request<TagKeysRequest>,
    ) -> Result<tonic::Response<Self::TagKeysStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
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
