use std::sync::Arc;

use async_trait::async_trait;
use data_types::DatabaseName;
use dml::DmlWrite;
use hyper::{Body, Method, Request, Response};
use snafu::{ResultExt, Snafu};

use crate::influxdb_ioxd::http::{
    error::{HttpApiError, HttpApiErrorExt, HttpApiErrorSource},
    metrics::LineProtocolMetrics,
    write::{HttpDrivenWrite, InnerWriteError, RequestOrResponse},
};

use super::RouterServerType;

#[derive(Debug, Snafu)]
pub enum ApplicationError {
    #[snafu(display("No handler for {:?} {}", method, path))]
    RouteNotFound { method: Method, path: String },

    #[snafu(display("Cannot write data: {}", source))]
    WriteError {
        source: crate::influxdb_ioxd::http::write::HttpWriteError,
    },
}

impl HttpApiErrorSource for ApplicationError {
    fn to_http_api_error(&self) -> HttpApiError {
        match self {
            e @ Self::RouteNotFound { .. } => e.not_found(),
            Self::WriteError { source } => source.to_http_api_error(),
        }
    }
}

#[async_trait]
impl HttpDrivenWrite for RouterServerType {
    fn max_request_size(&self) -> usize {
        self.max_request_size
    }

    fn lp_metrics(&self) -> Arc<LineProtocolMetrics> {
        Arc::clone(&self.lp_metrics)
    }

    async fn write(
        &self,
        db_name: &DatabaseName<'_>,
        write: DmlWrite,
    ) -> Result<(), InnerWriteError> {
        match self.server.router(db_name) {
            Some(router) => router
                .write(write)
                .await
                .map_err(|e| InnerWriteError::OtherError {
                    source: Box::new(e),
                }),
            None => Err(InnerWriteError::NotFound {
                db_name: db_name.to_string(),
            }),
        }
    }
}

#[allow(clippy::match_single_binding)]
pub async fn route_request(
    server_type: &RouterServerType,
    req: Request<Body>,
) -> Result<Response<Body>, ApplicationError> {
    match server_type
        .route_write_http_request(req)
        .await
        .context(WriteError)?
    {
        RequestOrResponse::Response(resp) => Ok(resp),
        RequestOrResponse::Request(req) => Err(ApplicationError::RouteNotFound {
            method: req.method().clone(),
            path: req.uri().path().to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use data_types::server_id::ServerId;
    use router::{grpc_client::MockClient, resolver::RemoteTemplate, server::RouterServer};
    use time::SystemProvider;
    use trace::RingBufferTraceCollector;

    use crate::influxdb_ioxd::{
        http::{
            test_utils::{assert_health, assert_metrics, assert_tracing, TestServer},
            write::test_utils::{
                assert_gzip_write, assert_write, assert_write_metrics,
                assert_write_to_invalid_database,
            },
        },
        server_type::common_state::CommonServerState,
    };

    use super::*;

    #[tokio::test]
    async fn test_health() {
        assert_health(test_server().await).await;
    }

    #[tokio::test]
    async fn test_metrics() {
        assert_metrics(test_server().await).await;
    }

    #[tokio::test]
    async fn test_tracing() {
        assert_tracing(test_server().await).await;
    }

    #[tokio::test]
    async fn test_write() {
        let test_server = test_server().await;
        let write = assert_write(&test_server).await;
        assert_dbwrite(test_server, write).await;
    }

    #[tokio::test]
    async fn test_gzip_write() {
        let test_server = test_server().await;
        let write = assert_gzip_write(&test_server).await;
        assert_dbwrite(test_server, write).await;
    }

    #[tokio::test]
    async fn test_write_metrics() {
        assert_write_metrics(test_server().await, false).await;
    }

    #[tokio::test]
    async fn test_write_to_invalid_database() {
        assert_write_to_invalid_database(test_server().await).await;
    }

    async fn test_server() -> TestServer<RouterServerType> {
        use data_types::router::{
            Matcher, MatcherToShard, Router, ShardConfig, ShardId, WriteSink, WriteSinkSet,
            WriteSinkVariant,
        };
        use regex::Regex;

        let common_state = CommonServerState::for_testing();
        let time_provider = Arc::new(SystemProvider::new());
        let server_id_1 = ServerId::try_from(1).unwrap();
        let remote_template = RemoteTemplate::new("{id}");

        let server = Arc::new(
            RouterServer::for_testing(
                Some(remote_template),
                Some(Arc::new(RingBufferTraceCollector::new(1))),
                time_provider,
            )
            .await,
        );
        server.update_router(Router {
            name: String::from("MyOrg_MyBucket"),
            write_sharder: ShardConfig {
                specific_targets: vec![MatcherToShard {
                    matcher: Matcher {
                        table_name_regex: Some(Regex::new(".*").unwrap()),
                    },
                    shard: ShardId::new(1),
                }],
                hash_ring: None,
            },
            write_sinks: BTreeMap::from([(
                ShardId::new(1),
                WriteSinkSet {
                    sinks: vec![WriteSink {
                        ignore_errors: false,
                        sink: WriteSinkVariant::GrpcRemote(server_id_1),
                    }],
                },
            )]),
            query_sinks: Default::default(),
        });

        let server_type = Arc::new(RouterServerType::new(server, &common_state));
        TestServer::new(server_type)
    }

    async fn assert_dbwrite(test_server: TestServer<RouterServerType>, write: DmlWrite) {
        let grpc_client = test_server
            .server_type()
            .server
            .connection_pool()
            .grpc_client("1")
            .await
            .unwrap();
        let grpc_client = grpc_client.as_any().downcast_ref::<MockClient>().unwrap();
        grpc_client.assert_writes(&[(String::from("MyOrg_MyBucket"), write)]);
    }
}
