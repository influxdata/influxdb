use data_types::server_id::ServerId;
use generated_types::google::ResourceType;
use generated_types::{
    google::{FieldViolation, NotFound},
    influxdata::iox::deployment::v1::*,
};
use router::server::RouterServer;
use std::{convert::TryFrom, sync::Arc};
use tonic::{Request, Response, Status};

struct DeploymentService {
    server: Arc<RouterServer>,
    serving_readiness: ServingReadiness,
}

use crate::influxdb_ioxd::serving_readiness::ServingReadiness;

#[tonic::async_trait]
impl deployment_service_server::DeploymentService for DeploymentService {
    async fn get_server_id(
        &self,
        _: Request<GetServerIdRequest>,
    ) -> Result<Response<GetServerIdResponse>, Status> {
        match self.server.server_id() {
            Some(id) => Ok(Response::new(GetServerIdResponse { id: id.get_u32() })),
            None => return Err(NotFound::new(ResourceType::ServerId, Default::default()).into()),
        }
    }

    async fn update_server_id(
        &self,
        request: Request<UpdateServerIdRequest>,
    ) -> Result<Response<UpdateServerIdResponse>, Status> {
        use router::server::SetServerIdError;

        let id =
            ServerId::try_from(request.get_ref().id).map_err(|_| FieldViolation::required("id"))?;

        match self.server.set_server_id(id) {
            Ok(_) => Ok(Response::new(UpdateServerIdResponse {})),
            Err(e @ SetServerIdError::AlreadySet { .. }) => {
                return Err(FieldViolation {
                    field: "id".to_string(),
                    description: e.to_string(),
                }
                .into())
            }
        }
    }

    async fn set_serving_readiness(
        &self,
        request: Request<SetServingReadinessRequest>,
    ) -> Result<Response<SetServingReadinessResponse>, Status> {
        let SetServingReadinessRequest { ready } = request.into_inner();
        self.serving_readiness.set(ready.into());
        Ok(Response::new(SetServingReadinessResponse {}))
    }

    async fn get_serving_readiness(
        &self,
        _request: Request<GetServingReadinessRequest>,
    ) -> Result<Response<GetServingReadinessResponse>, Status> {
        Ok(Response::new(GetServingReadinessResponse {
            ready: self.serving_readiness.get().into(),
        }))
    }
}

pub fn make_server(
    server: Arc<RouterServer>,
    serving_readiness: ServingReadiness,
) -> deployment_service_server::DeploymentServiceServer<
    impl deployment_service_server::DeploymentService,
> {
    deployment_service_server::DeploymentServiceServer::new(DeploymentService {
        server,
        serving_readiness,
    })
}
