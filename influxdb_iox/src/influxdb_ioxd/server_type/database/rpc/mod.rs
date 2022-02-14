use std::sync::Arc;

use crate::influxdb_ioxd::{
    rpc::{add_gated_service, add_service, serve_builder, setup_builder, RpcBuilderInput},
    server_type::{database::DatabaseServerType, RpcError},
};

mod delete;
mod deployment;
mod error;
mod flight;
mod management;
mod operations;
mod storage;
mod write_pb;

pub async fn server_grpc(
    server_type: Arc<DatabaseServerType>,
    builder_input: RpcBuilderInput,
) -> Result<(), RpcError> {
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
            server_type.config_immutable,
        )
    );
    add_service!(
        builder,
        deployment::make_server(
            Arc::clone(&server_type.server),
            server_type.serving_readiness.clone(),
        )
    );
    add_service!(
        builder,
        operations::make_server(Arc::clone(server_type.application.job_registry()))
    );

    serve_builder!(builder);

    Ok(())
}
