use std::convert::TryFrom;
use std::fmt::Debug;
use std::sync::Arc;

use data_types::server_id::ServerId;
use generated_types::google::{FieldViolation, NotFound};
use generated_types::influxdata::iox::remote::v1::*;
use server::{connection::ConnectionManager, Server};
use tonic::{Request, Response, Status};

struct RemoteService<M: ConnectionManager> {
    server: Arc<Server<M>>,
}

#[tonic::async_trait]
impl<M> remote_service_server::RemoteService for RemoteService<M>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    async fn list_remotes(
        &self,
        _: Request<ListRemotesRequest>,
    ) -> Result<Response<ListRemotesResponse>, Status> {
        let remotes = self
            .server
            .remotes_sorted()
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

        match self.server.delete_remote(remote_id) {
            Some(_) => {}
            None => return Err(NotFound::default().into()),
        }

        Ok(Response::new(DeleteRemoteResponse {}))
    }
}

pub fn make_server<M>(
    server: Arc<Server<M>>,
) -> remote_service_server::RemoteServiceServer<impl remote_service_server::RemoteService>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    remote_service_server::RemoteServiceServer::new(RemoteService { server })
}
