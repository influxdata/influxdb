use delorean::delorean::{
    delorean_server::Delorean, CreateBucketRequest, CreateBucketResponse, DeleteBucketRequest,
    DeleteBucketResponse, GetBucketsResponse, Organization,
};

use std::sync::Arc;

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
