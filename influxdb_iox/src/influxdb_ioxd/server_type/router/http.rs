use std::sync::Arc;

use async_trait::async_trait;
use data_types::DatabaseName;
use dml::DmlOperation;
use hyper::{Body, Method, Request, Response};
use snafu::{ResultExt, Snafu};

use crate::influxdb_ioxd::http::{
    dml::{HttpDrivenDml, InnerDmlError, RequestOrResponse},
    error::{HttpApiError, HttpApiErrorExt, HttpApiErrorSource},
    metrics::LineProtocolMetrics,
};

use super::RouterServerType;

#[derive(Debug, Snafu)]
pub enum ApplicationError {
    #[snafu(display("No handler for {:?} {}", method, path))]
    RouteNotFound { method: Method, path: String },

    #[snafu(display("Cannot write data: {}", source))]
    WriteError {
        source: crate::influxdb_ioxd::http::dml::HttpDmlError,
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
impl HttpDrivenDml for RouterServerType {
    fn max_request_size(&self) -> usize {
        self.max_request_size
    }

    fn lp_metrics(&self) -> Arc<LineProtocolMetrics> {
        Arc::clone(&self.lp_metrics)
    }

    async fn write(
        &self,
        db_name: &DatabaseName<'_>,
        op: DmlOperation,
    ) -> Result<(), InnerDmlError> {
        match self.server.router(db_name) {
            Some(router) => router
                .write(op)
                .await
                .map_err(|e| InnerDmlError::InternalError {
                    db_name: db_name.to_string(),
                    source: Box::new(e),
                }),
            None => Err(InnerDmlError::DatabaseNotFound {
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
        .route_dml_http_request(req)
        .await
        .context(WriteSnafu)?
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

    use data_types::{
        delete_predicate::{DeleteExpr, DeletePredicate},
        server_id::ServerId,
        timestamp::TimestampRange,
    };
    use dml::{DmlDelete, DmlMeta, DmlOperation};
    use http::StatusCode;
    use reqwest::Client;
    use router::{grpc_client::MockClient, resolver::RemoteTemplate, server::RouterServer};
    use time::SystemProvider;
    use trace::RingBufferTraceCollector;

    use crate::influxdb_ioxd::{
        http::{
            dml::test_utils::{
                assert_delete_bad_request, assert_delete_unknown_database, assert_gzip_write,
                assert_write, assert_write_metrics, assert_write_to_invalid_database,
            },
            test_utils::{
                assert_health, assert_metrics, assert_tracing, check_response, TestServer,
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
        assert_dbwrite(test_server, DmlOperation::Write(write)).await;
    }

    #[tokio::test]
    async fn test_gzip_write() {
        let test_server = test_server().await;
        let write = assert_gzip_write(&test_server).await;
        assert_dbwrite(test_server, DmlOperation::Write(write)).await;
    }

    #[tokio::test]
    async fn test_write_metrics() {
        assert_write_metrics(test_server().await, false).await;
    }

    #[tokio::test]
    async fn test_write_to_invalid_database() {
        assert_write_to_invalid_database(test_server().await).await;
    }

    #[tokio::test]
    async fn test_delete() {
        // Set up server
        let test_server = test_server().await;

        // Set up client
        let client = Client::new();
        let bucket_name = "MyBucket";
        let org_name = "MyOrg";

        // Client requests to delete data
        let delete_line = r#"{"start":"1","stop":"2", "predicate":"foo=1"}"#;
        let response = client
            .post(&format!(
                "{}/api/v2/delete?bucket={}&org={}",
                test_server.url(),
                bucket_name,
                org_name
            ))
            .body(delete_line)
            .send()
            .await;
        check_response("delete", response, StatusCode::NO_CONTENT, Some("")).await;

        let predicate = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![DeleteExpr {
                column: String::from("foo"),
                op: data_types::delete_predicate::Op::Eq,
                scalar: data_types::delete_predicate::Scalar::I64(1),
            }],
        };
        let delete = DmlDelete::new(predicate, None, DmlMeta::unsequenced(None));
        assert_dbwrite(test_server, DmlOperation::Delete(delete)).await;
    }

    #[tokio::test]
    async fn test_delete_unknown_database() {
        assert_delete_unknown_database(test_server().await).await;
    }

    #[tokio::test]
    async fn test_delete_bad_request() {
        assert_delete_bad_request(test_server().await).await;
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
                None,
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

    async fn assert_dbwrite(test_server: TestServer<RouterServerType>, write: DmlOperation) {
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
