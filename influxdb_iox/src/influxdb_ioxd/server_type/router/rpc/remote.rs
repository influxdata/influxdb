use std::convert::TryFrom;
use std::sync::Arc;

use data_types::server_id::ServerId;
use generated_types::google::{FieldViolation, NotFound, ResourceType};
use generated_types::influxdata::iox::remote::v1::*;
use router::server::RouterServer;
use tonic::{Request, Response, Status};

struct RemoteService {
    server: Arc<RouterServer>,
}

#[tonic::async_trait]
impl remote_service_server::RemoteService for RemoteService {
    async fn list_remotes(
        &self,
        _: Request<ListRemotesRequest>,
    ) -> Result<Response<ListRemotesResponse>, Status> {
        let remotes = self
            .server
            .resolver()
            .remotes()
            .into_iter()
            .map(|(id, connection_string)| Remote {
                id: id.get_u32(),
                connection_string,
            })
            .collect();

        Ok(Response::new(ListRemotesResponse { remotes }))
    }

    async fn update_remote(
        &self,
        request: Request<UpdateRemoteRequest>,
    ) -> Result<Response<UpdateRemoteResponse>, Status> {
        let remote = request
            .into_inner()
            .remote
            .ok_or_else(|| FieldViolation::required("remote"))?;
        let remote_id = ServerId::try_from(remote.id)
            .map_err(|_| FieldViolation::required("id").scope("remote"))?;

        self.server
            .resolver()
            .update_remote(remote_id, remote.connection_string);

        Ok(Response::new(UpdateRemoteResponse {}))
    }

    async fn delete_remote(
        &self,
        request: Request<DeleteRemoteRequest>,
    ) -> Result<Response<DeleteRemoteResponse>, Status> {
        let request = request.into_inner();
        let remote_id =
            ServerId::try_from(request.id).map_err(|_| FieldViolation::required("id"))?;

        if self.server.resolver().delete_remote(remote_id) {
            Ok(Response::new(DeleteRemoteResponse {}))
        } else {
            Err(NotFound::new(ResourceType::ServerId, remote_id.to_string()).into())
        }
    }
}

pub fn make_server(
    server: Arc<RouterServer>,
) -> remote_service_server::RemoteServiceServer<impl remote_service_server::RemoteService> {
    remote_service_server::RemoteServiceServer::new(RemoteService { server })
}
