use std::sync::Arc;

use crate::DatabaseServerType;
use ioxd_common::{
    add_service, rpc::RpcBuilderInput, serve_builder, server_type::RpcError, setup_builder,
};

mod delete;
mod deployment;
mod error;
mod management;
mod operations;
mod query;
mod write_pb;

pub async fn server_grpc(
    server_type: Arc<DatabaseServerType>,
    builder_input: RpcBuilderInput,
) -> Result<(), RpcError> {
    let builder = setup_builder!(builder_input, server_type);

    add_service!(
        builder,
        query::make_storage_server(Arc::clone(&server_type.server),)
    );
    add_service!(
        builder,
        query::make_flight_server(Arc::clone(&server_type.server))
    );
    add_service!(
        builder,
        delete::make_server(Arc::clone(&server_type.server))
    );
    add_service!(
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
        deployment::make_server(Arc::clone(&server_type.server),)
    );
    add_service!(
        builder,
        operations::make_server(Arc::clone(server_type.application.job_registry()))
    );

    serve_builder!(builder);

    Ok(())
}
