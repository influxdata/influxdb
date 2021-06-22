//! The grpc-router crate helps creating gRPC routers/forwarders for existing gRPC services.
//!
//! The router implements a gRPC service trait and forwards requests to a local struct implementing
//! that same service interface or to a remote service. The tonic gRPC client used to talk to the
//! remote service is provided by the user by implementing the [`Router`] trait for the router service type.
//! The [`Router`] trait allows the user to provide a different gRPC client per request, or to just
//! fall-back to serving the request from a local service implementation (without any further gRPC overhead).   
//!
//! This crate also offers an optional caching [`connection_manager`], which can be useful for
//! implementing the [`Router`] trait.
//!
//! # Examples
//!
//! ## Simple introductory example:
//!
//! ```
//! # use std::pin::Pin;
//! # use futures::Stream;
//! use grpc_router_test_gen::test_proto::{test_server::Test, test_client::TestClient, *};
//! use grpc_router::{grpc_router, router, Router, RoutingDestination};
//! use tonic::{Request, Response, transport::Channel, Status};
//!
//! /// A router is a service ...
//! struct TestRouter {}
//!
//! /// ... like any other gRPC service it must implement the service's trait.
//! impl Test for TestRouter {
//!     /// The [`grpc_router`] macro takes care of implementing the methods.
//!     /// But it needs some help from you: you need to list the signatures of all routed methods.
//!     /// Luckily you can (almost) copy&paste the protobuf IDL code in it:
//!     grpc_router! {
//!         rpc TestUnary (unary TestRequest) returns (unary TestUnaryResponse);
//!         rpc TestServerStream (unary TestRequest) returns (stream TestServerStreamResponse);
//!     }
//! }
//!
//! # fn is_local<R>(request: &Request<R>) -> bool { true }
//! #
//! #[tonic::async_trait]
//! impl Router<Request<TestRequest>, TestService, TestClient<Channel>> for TestRouter {
//!     async fn route_for(
//!         &self,
//!         request: &Request<TestRequest>,
//!     ) -> router::Result<RoutingDestination<TestService, TestClient<Channel>>> {
//!         /// use the request argument to decide where to route...
//!         Ok(if is_local(&request) {
//!             RoutingDestination::Local(&TestService{})
//!         } else {
//!             /// in the real world, a [`connection_manager::ConnectionManager`] can be used.
//!             RoutingDestination::Remote(
//!                 TestClient::connect("http:/1.2.3.4:1234".to_string()).await.unwrap(),
//!             )
//!         })
//!     }
//! }
//!
//! /// Where `TestService` is a concrete implementation of the `Test` service:
//!
//! struct TestService {
//!     // ...
//! }
//!
//! #[tonic::async_trait]
//! impl Test for TestService {
//!     // ...
//! #    async fn test_unary(&self, request: Request<TestRequest>) -> Result<Response<TestUnaryResponse>, Status> {
//! #        todo!()
//! #    }
//! #
//! #    type TestServerStreamStream = Pin<Box<dyn Stream<Item = Result<TestServerStreamResponse, tonic::Status>> + Send + Sync>>;
//! #
//! #    async fn test_server_stream(&self, request: Request<TestRequest>) -> Result<Response<Self::TestServerStreamStream>, Status> {
//! #        todo!()
//! #    }
//! }
//! ```
//!
//! ## Full example with a connection manager
//!
//! ```
//! # use std::pin::Pin;
//! # use futures::Stream;
//! use grpc_router_test_gen::test_proto::{test_server::{Test, TestServer}, test_client::TestClient, *};
//! use grpc_router::{grpc_router, router, Router, RoutingDestination};
//! use grpc_router::connection_manager::{ConnectionManager, CachingConnectionManager};
//! use tonic::{Request, Response, transport::Channel, Status};
//!
//! struct TestRouter<M>
//! where
//!     M: ConnectionManager<TestClient<Channel>> + Send + Sync + 'static,
//! {
//!     /// A router service can embed a connection manager.
//!     connection_manager: M,
//!     /// and the fall-back local instance of the service
//!     local: TestService,
//! }
//!
//! impl<M> Test for TestRouter<M>
//! where
//!     M: ConnectionManager<TestClient<Channel>> + Send + Sync + 'static,
//! {
//!     grpc_router! {
//!         rpc TestUnary (unary TestRequest) returns (unary TestUnaryResponse);
//!         rpc TestServerStream (unary TestRequest) returns (stream TestServerStreamResponse);
//!     }
//! }
//!
//! # fn address_for<R>(request: &Request<R>) -> Option<String> { None }
//! #
//! #[tonic::async_trait]
//! impl<M> Router<Request<TestRequest>, TestService, TestClient<Channel>> for TestRouter<M>
//! where
//!     M: ConnectionManager<TestClient<Channel>> + Send + Sync + 'static,
//! {
//!     async fn route_for(
//!         &self,
//!         request: &Request<TestRequest>,
//!     ) -> router::Result<RoutingDestination<TestService, TestClient<Channel>>> {
//!         /// Some custom logic to figure out the connection string for a remote server ...
//!         Ok(match address_for(&request) {
//!             None => RoutingDestination::Local(&self.local),
//!             Some(remote_addr) => RoutingDestination::Remote(
//!                 /// ... which the connection manager consumes to yield a client.
//!                 self.connection_manager
//!                     .remote_server(remote_addr.clone())
//!                     .await?,
//!             ),         
//!         })
//!     }
//! }
//!
//! # use std::net::SocketAddr;
//! /// And now we need to wire everything up together
//! async fn run(addr: SocketAddr) {
//! #    use tonic::transport::Server;
//!     /// A caching connection manager that builds instances of TestClient
//!     let connection_manager = CachingConnectionManager::builder()
//!         .with_make_client(|dst| Box::pin(TestClient::connect(dst)))
//!         .build();
//!     /// The fallback local service (used if the routing destination is Local).
//!     let local = TestService{};
//!
//!     let router = TestRouter { connection_manager, local };
//!
//!     /// the `router` implements `Test` and thus can be treated as any other
//!     /// implementation of a gRPC service.
//!     Server::builder()
//!         .add_service(TestServer::new(router))
//!         .serve(addr)
//!         .await
//!         .unwrap();
//! }
//!
//! # struct TestService {}
//! # #[tonic::async_trait]
//! # impl Test for TestService {
//! #    async fn test_unary(&self, request: Request<TestRequest>) -> Result<Response<TestUnaryResponse>, Status> {
//! #        todo!()
//! #    }
//! #
//! #    type TestServerStreamStream = Pin<Box<dyn Stream<Item = Result<TestServerStreamResponse, tonic::Status>> + Send + Sync>>;
//! #
//! #    async fn test_server_stream(&self, request: Request<TestRequest>) -> Result<Response<Self::TestServerStreamStream>, Status> {
//! #        todo!()
//! #    }
//! # }
//! ```

#![deny(broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

pub mod connection_manager;
#[macro_use]
pub mod router;

pub use router::{Router, RoutingDestination};
