use std::sync::Arc;

use async_trait::async_trait;
use hyper::{Body, Method, Request, Response};
use ioxd_common::{
    http::error::{HttpApiError, HttpApiErrorExt, HttpApiErrorSource},
    rpc::RpcBuilderInput,
    serve_builder, setup_builder,
};
use metric::Registry;
use snafu::Snafu;
use tokio_util::sync::CancellationToken;
use trace::TraceCollector;

use ioxd_common::server_type::{RpcError, ServerType};

#[derive(Debug, Snafu)]
pub enum ApplicationError {
    #[snafu(display("No handler for {:?} {}", method, path))]
    RouteNotFound { method: Method, path: String },
}

impl HttpApiErrorSource for ApplicationError {
    fn to_http_api_error(&self) -> HttpApiError {
        match self {
            e @ Self::RouteNotFound { .. } => e.not_found(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, clap::ArgEnum)]
pub enum TestAction {
    None,
    EarlyReturnFromGrpcWorker,
    EarlyReturnFromServerWorker,
    PanicInGrpcWorker,
    PanicInServerWorker,
}

#[derive(Debug)]
pub struct TestServerType {
    metric_registry: Arc<Registry>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
    shutdown: CancellationToken,
    test_action: TestAction,
}

impl TestServerType {
    pub fn new(
        metric_registry: Arc<Registry>,
        trace_collector: Option<Arc<dyn TraceCollector>>,
        test_action: TestAction,
    ) -> Self {
        Self {
            metric_registry,
            trace_collector,
            shutdown: CancellationToken::new(),
            test_action,
        }
    }
}

#[async_trait]
impl ServerType for TestServerType {
    fn metric_registry(&self) -> Arc<Registry> {
        Arc::clone(&self.metric_registry)
    }

    fn trace_collector(&self) -> Option<Arc<dyn TraceCollector>> {
        self.trace_collector.clone()
    }

    async fn route_http_request(
        &self,
        req: Request<Body>,
    ) -> Result<Response<Body>, Box<dyn HttpApiErrorSource>> {
        Err(Box::new(ApplicationError::RouteNotFound {
            method: req.method().clone(),
            path: req.uri().path().to_string(),
        }))
    }

    async fn server_grpc(self: Arc<Self>, builder_input: RpcBuilderInput) -> Result<(), RpcError> {
        match self.test_action {
            TestAction::PanicInGrpcWorker => panic!("Test panic in gRPC worker"),
            TestAction::EarlyReturnFromGrpcWorker => Ok(()),
            _ => {
                let builder = setup_builder!(builder_input, self);
                serve_builder!(builder);

                Ok(())
            }
        }
    }

    async fn join(self: Arc<Self>) {
        if self.test_action == TestAction::PanicInServerWorker {
            panic!("Test panic in server worker");
        }
        if self.test_action == TestAction::EarlyReturnFromServerWorker {
            return;
        }

        self.shutdown.cancelled().await;
    }

    fn shutdown(&self) {
        self.shutdown.cancel();
    }
}
