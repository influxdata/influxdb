use std::sync::Arc;

use ioxd_common::{
    add_service, rpc::RpcBuilderInput, serve_builder, server_type::RpcError, setup_builder,
};

use super::RouterServerType;

mod delete;
mod deployment;
mod remote;
mod router;
mod write_pb;

pub async fn server_grpc(
    server_type: Arc<RouterServerType>,
    builder_input: RpcBuilderInput,
) -> Result<(), RpcError> {
    let builder = setup_builder!(builder_input, server_type);

    add_service!(
        builder,
        deployment::make_server(Arc::clone(&server_type.server),)
    );
    add_service!(
        builder,
        remote::make_server(Arc::clone(&server_type.server))
    );
    add_service!(
        builder,
        router::make_server(
            Arc::clone(&server_type.server),
            server_type.config_immutable
        )
    );
    add_service!(
        builder,
        delete::make_server(Arc::clone(&server_type.server))
    );
    add_service!(
        builder,
        write_pb::make_server(Arc::clone(&server_type.server))
    );

    serve_builder!(builder);

    Ok(())
}
