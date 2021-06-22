use futures::Stream;
use std::pin::Pin;
use thiserror::Error;
use tonic::{Response, Status, Streaming};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Cannot route request: {0}")]
    RoutingError(#[from] Box<dyn std::error::Error + Sync + Send>),
}

impl From<Error> for Status {
    fn from(error: Error) -> Self {
        Self::internal(error.to_string())
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A gRPC router containing an invocation of the [`grpc_router`] macro must
/// implement this trait.   
#[tonic::async_trait]
pub trait Router<R, S, C> {
    /// For a given request return the routing decision for the call.
    async fn route_for(&self, request: &R) -> Result<RoutingDestination<'_, S, C>>;
}

/// A [`RoutingDestination`] is either a local in-process grpc Service or a remote grpc Client for that service.
///
/// Unfortunately tonic clients and servers don't share any traits, so it's up to
/// you to ensure that C is a client for service S.
#[derive(Debug)]
pub enum RoutingDestination<'a, S, C> {
    /// Reference to an implementation of a gRPC service trait. This causes the router to
    /// transfer control to an in-process implementation of a service, effectively zero cost routing for
    /// a local service.
    Local(&'a S),
    /// Routing to a remote service via a gRPC client instance connected to the remote endpoint.
    Remote(C),
}

/// Needs to be public because it's used by the [`grpc_router`] macro.
pub type PinnedStream<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + Sync>>;

/// Needs to be public because it's used by the [`grpc_router`] macro.
pub fn pinned_response<T: 'static>(res: Response<Streaming<T>>) -> Response<PinnedStream<T>> {
    tonic::Response::new(Box::pin(res.into_inner()))
}

/// This is a macro that parses a quasi gRPC protobuf service declaration and emits the required
/// boilerplate to implement a service trait that forwards the calls to a remote gRPC service
/// and to a local service depending on what the [`Router`] trait for `self` says for a given request.
///
/// Currently it cannot parse the gRPC IDL syntax (TODO): you have to explicitly specify `unary` or `stream`.
///
/// # Examples
///
/// ```ignore
/// impl Foo for FooRouter
///  {
///      grpc_router! {
///          rpc Bar (unary BarRequest) returns (unary BarResponse);
///          rpc Baz (unary BazRequest) returns (stream BazResponse);
///      }
///  }
/// ```
#[macro_export]
macro_rules! grpc_router {
    { $( rpc $method:ident ( $request_kind:ident $request:ty ) returns ( $response_kind:ident $response:expr ) ; )* }  => {
        $( grpc_router! { @ $request_kind $response_kind $method, $request, $response } )*
    };

    { @ unary unary $method:ident, $request:ty, $response:expr }  => {
        paste::paste! {
            // NOTE: we cannot emit an `async fn` from this macro since #[tonic::async_trait] transforms
            // the body of the trait/impl before macros get expanded.
            // Thus, we have to do what async_trait would have done:
            //
            // ```
            // #[tonic::async_trait]
            // async fn [<$method:snake>](
            //     &self,
            //     req: Request<$request>,
            // ) -> Result<tonic::Response<$response>, Status> {
            //     ...
            // }
            // ```
            //
            // -->
            //
            fn [<$method:snake>]<'a, 'async_trait>(
                &'a self,
                req: tonic::Request<$request>,
            ) -> std::pin::Pin<
                Box<
                    dyn futures::Future<Output = Result<tonic::Response<$response>, tonic::Status>>
                        + Send
                        + 'async_trait,
                >,
            >
            where
                'a: 'async_trait,
                Self: 'async_trait,
            {
                Box::pin(async move {
                    match self.route_for(&req).await? {
                        $crate::RoutingDestination::Local(svc) => svc.[<$method:snake>](req).await,
                        $crate::RoutingDestination::Remote(mut svc) => svc.[<$method:snake>](req).await,
                    }
                })
            }
        }
    };

    { @ unary stream $method:ident, $request:ty, $response:expr }  => {
        paste::paste! {
            type [<$method:camel Stream>] = $crate::router::PinnedStream<$response>;

            fn [<$method:snake>]<'a, 'async_trait>(
                &'a self,
                req: tonic::Request<$request>,
            ) -> std::pin::Pin<
                Box<
                    dyn futures::Future<Output = Result<tonic::Response<Self::[<$method:camel Stream>]>, tonic::Status>>
                        + Send
                        + 'async_trait,
                >,
            >
            where
                'a: 'async_trait,
                Self: 'async_trait,
            {
                Box::pin(async move {
                    use $crate::router::pinned_response;
                    match self.route_for(&req).await? {
                        $crate::RoutingDestination::Local(svc) => svc.[<$method:snake>](req).await,
                        $crate::RoutingDestination::Remote(mut svc) => Ok(pinned_response(svc.[<$method:snake>](req).await?)),
                    }
                })
            }
        }
    };
}

#[cfg(test)]
pub mod tests {
    // intentionally doesn't use super::* in order to use the public interface only
    use super::PinnedStream; // just a utility
    use crate::connection_manager::{CachingConnectionManager, ConnectionManager};
    use crate::{grpc_router, router, Router, RoutingDestination};
    use futures::{FutureExt, StreamExt};
    use grpc_router_test_gen::test_proto::{test_client::TestClient, test_server::TestServer, *};
    use std::net::SocketAddr;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::{Channel, Server};
    use tonic::{Request, Response, Status};

    #[derive(Clone)]
    struct TestService {
        base: u64,
    }

    #[tonic::async_trait]
    impl test_server::Test for TestService {
        async fn test_unary(
            &self,
            _request: Request<TestRequest>,
        ) -> Result<Response<TestUnaryResponse>, Status> {
            Ok(tonic::Response::new(TestUnaryResponse {
                answer: self.base + 1,
            }))
        }

        type TestServerStreamStream = PinnedStream<TestServerStreamResponse>;

        async fn test_server_stream(
            &self,
            _request: Request<TestRequest>,
        ) -> Result<Response<Self::TestServerStreamStream>, Status> {
            let it = (self.base + 1..=self.base + 2)
                .map(|answer| Ok(TestServerStreamResponse { answer }));
            Ok(tonic::Response::new(Box::pin(futures::stream::iter(it))))
        }
    }

    #[derive(Debug)]
    pub struct Fixture {
        pub local_addr: String,
        pub client: TestClient<Channel>,
        shutdown_tx: tokio::sync::oneshot::Sender<()>,
    }

    impl Fixture {
        /// Start up a grpc server listening on `port`, returning
        /// a fixture with the server and client.
        async fn new<T>(svc: T) -> Result<Self, Box<dyn std::error::Error>>
        where
            T: test_server::Test,
        {
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
            let addr: SocketAddr = "127.0.0.1:0".parse()?;
            let listener = tokio::net::TcpListener::bind(addr).await?;
            let local_addr = listener.local_addr()?;
            let local_addr = format!("http://{}", local_addr.to_string());

            tokio::spawn(async move {
                Server::builder()
                    .add_service(TestServer::new(svc))
                    .serve_with_incoming_shutdown(
                        TcpListenerStream::new(listener),
                        shutdown_rx.map(drop),
                    )
                    .await
                    .unwrap();
            });

            // Give the test server a few ms to become available
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            // Construct client and send request, extract response
            let client = TestClient::connect(local_addr.clone())
                .await
                .expect("connect");

            Ok(Self {
                local_addr,
                client,
                shutdown_tx,
            })
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let (tmp_tx, _) = tokio::sync::oneshot::channel();
            let shutdown_tx = std::mem::replace(&mut self.shutdown_tx, tmp_tx);
            if let Err(e) = shutdown_tx.send(()) {
                eprintln!("error shutting down text fixture: {:?}", e);
            }
        }
    }

    async fn test(mut client: TestClient<Channel>, route_me: bool, base: u64) {
        let res = client
            .test_unary(TestRequest { route_me })
            .await
            .expect("call");
        assert_eq!(res.into_inner().answer, base + 1);

        let res = client
            .test_server_stream(TestRequest { route_me })
            .await
            .expect("call");
        let res: Vec<_> = res
            .into_inner()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()
            .expect("success");
        assert_eq!(
            res,
            vec![
                TestServerStreamResponse { answer: base + 1 },
                TestServerStreamResponse { answer: base + 2 }
            ]
        );
    }

    #[tokio::test]
    async fn test_local() {
        const BASE: u64 = 40;
        let fixture = Fixture::new(TestService { base: BASE })
            .await
            .expect("fixture");

        test(fixture.client.clone(), false, BASE).await;
    }

    #[tokio::test]
    async fn test_routed() {
        const REMOTE_BASE: u64 = 20;
        const LOCAL_BASE: u64 = 40;

        struct TestRouter<M>
        where
            M: ConnectionManager<TestClient<Channel>> + Send + Sync + 'static,
        {
            remote_addr: String,
            local: TestService,
            connection_manager: M,
        }

        impl<M> test_server::Test for TestRouter<M>
        where
            M: ConnectionManager<TestClient<Channel>> + Send + Sync + 'static,
        {
            grpc_router! {
                rpc TestUnary (unary TestRequest) returns (unary TestUnaryResponse);
                rpc TestServerStream (unary TestRequest) returns (stream TestServerStreamResponse);
            }
        }

        #[tonic::async_trait]
        impl<M> Router<Request<TestRequest>, TestService, test_client::TestClient<Channel>>
            for TestRouter<M>
        where
            M: ConnectionManager<TestClient<Channel>> + Send + Sync + 'static,
        {
            async fn route_for(
                &self,
                request: &Request<TestRequest>,
            ) -> router::Result<RoutingDestination<'_, TestService, test_client::TestClient<Channel>>>
            {
                Ok(if request.get_ref().route_me {
                    RoutingDestination::Remote(
                        self.connection_manager
                            .remote_server(self.remote_addr.clone())
                            .await?,
                    )
                } else {
                    RoutingDestination::Local(&self.local)
                })
            }
        }

        // a connection manager for TestClient
        let connection_manager = CachingConnectionManager::builder()
            .with_make_client(|dst| Box::pin(TestClient::connect(dst)))
            .build();

        // a remote TestService
        let remote_fixture = Fixture::new(TestService { base: REMOTE_BASE })
            .await
            .expect("remote fixture");

        // a router that can route to a remote TestService or serve from a local TestService
        let router = TestRouter {
            remote_addr: remote_fixture.local_addr.clone(),
            local: TestService { base: LOCAL_BASE },
            connection_manager,
        };
        let router_fixture = Fixture::new(router).await.expect("router fixture");

        test(router_fixture.client.clone(), false, LOCAL_BASE).await;
        test(router_fixture.client.clone(), true, REMOTE_BASE).await;
    }
}
