use std::sync::Arc;

use crate::influxdb_ioxd::{
    rpc::{add_service, serve_builder, setup_builder, RpcBuilderInput},
    server_type::RpcError,
};

use super::RouterServerType;

mod deployment;
mod router;

pub async fn server_grpc(
    server_type: Arc<RouterServerType>,
    builder_input: RpcBuilderInput,
) -> Result<(), RpcError> {
    let builder = setup_builder!(builder_input, server_type);

    add_service!(
        builder,
        deployment::make_server(
            Arc::clone(&server_type.server),
            server_type.serving_readiness.clone(),
        )
    );
    add_service!(
        builder,
        router::make_server(Arc::clone(&server_type.server),)
    );

    serve_builder!(builder);

    Ok(())
}
