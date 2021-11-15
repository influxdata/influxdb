use std::sync::Arc;

use generated_types::{google::FromOptionalField, influxdata::iox::router::v1::*};
use router::server::RouterServer;
use tonic::{Request, Response, Status};

struct RouterService {
    server: Arc<RouterServer>,
}

#[tonic::async_trait]
impl router_service_server::RouterService for RouterService {
    async fn list_routers(
        &self,
        _: Request<ListRoutersRequest>,
    ) -> Result<Response<ListRoutersResponse>, Status> {
        Ok(Response::new(ListRoutersResponse {
            routers: self
                .server
                .routers()
                .into_iter()
                .map(|router| router.config().clone().into())
                .collect(),
        }))
    }

    async fn update_router(
        &self,
        request: Request<UpdateRouterRequest>,
    ) -> Result<Response<UpdateRouterResponse>, Status> {
        use data_types::router::Router as RouterConfig;

        let UpdateRouterRequest { router } = request.into_inner();
        let cfg: RouterConfig = router.required("router")?;
        self.server.update_router(cfg);
        Ok(Response::new(UpdateRouterResponse {}))
    }

    async fn delete_router(
        &self,
        request: Request<DeleteRouterRequest>,
    ) -> Result<Response<DeleteRouterResponse>, Status> {
        let DeleteRouterRequest { router_name } = request.into_inner();
        self.server.delete_router(&router_name);
        Ok(Response::new(DeleteRouterResponse {}))
    }
}

pub fn make_server(
    server: Arc<RouterServer>,
) -> router_service_server::RouterServiceServer<impl router_service_server::RouterService> {
    router_service_server::RouterServiceServer::new(RouterService { server })
}
