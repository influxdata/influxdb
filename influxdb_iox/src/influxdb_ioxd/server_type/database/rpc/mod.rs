use std::sync::Arc;

use server::connection::ConnectionManager;

use crate::influxdb_ioxd::{
    rpc::{add_gated_service, add_service, serve_builder, setup_builder, RpcBuilderInput},
    server_type::{database::DatabaseServerType, RpcError},
};

mod delete;
mod flight;
mod management;
mod operations;
mod remote;
mod storage;
mod write;
mod write_pb;

pub async fn server_grpc<M>(
    server_type: Arc<DatabaseServerType<M>>,
    builder_input: RpcBuilderInput,
) -> Result<(), RpcError>
where
    M: ConnectionManager + std::fmt::Debug + Send + Sync + 'static,
{
    let builder = setup_builder!(builder_input, server_type);

    add_gated_service!(
        builder,
        storage::make_server(Arc::clone(&server_type.server),)
    );
    add_gated_service!(
        builder,
        flight::make_server(Arc::clone(&server_type.server))
    );
    add_gated_service!(
        builder,
        delete::make_server(Arc::clone(&server_type.server))
    );
    add_gated_service!(builder, write::make_server(Arc::clone(&server_type.server)));
    add_gated_service!(
        builder,
        write_pb::make_server(Arc::clone(&server_type.server))
    );
    // Also important this is not behind a readiness check (as it is
    // used to change the check!)
    add_service!(
        builder,
        management::make_server(
            Arc::clone(&server_type.application),
            Arc::clone(&server_type.server),
            server_type.serving_readiness.clone(),
        )
    );
    add_service!(
        builder,
        operations::make_server(Arc::clone(server_type.application.job_registry()))
    );

    // remotes can be managed even if the server is not serving
    add_service!(
        builder,
        remote::make_server(Arc::clone(&server_type.server))
    );

    serve_builder!(builder);

    Ok(())
}
