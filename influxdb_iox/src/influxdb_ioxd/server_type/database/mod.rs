use crate::influxdb_ioxd::{
    http::metrics::LineProtocolMetrics,
    rpc::RpcBuilderInput,
    server_type::{RpcError, ServerType},
    serving_readiness::ServingReadiness,
};
use async_trait::async_trait;
use futures::{future::FusedFuture, FutureExt};
use hyper::{Body, Request, Response};
use metric::Registry;
use observability_deps::tracing::{error, info};
use server::{ApplicationState, Server};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use trace::TraceCollector;

mod http;
mod rpc;
pub mod setup;

pub use self::http::ApplicationError;
use super::common_state::CommonServerState;

#[derive(Debug)]
pub struct DatabaseServerType {
    pub application: Arc<ApplicationState>,
    pub server: Arc<Server>,
    pub lp_metrics: Arc<LineProtocolMetrics>,
    pub max_request_size: usize,
    pub serving_readiness: ServingReadiness,
    shutdown: CancellationToken,
}

impl DatabaseServerType {
    pub fn new(
        application: Arc<ApplicationState>,
        server: Arc<Server>,
        common_state: &CommonServerState,
    ) -> Self {
        let lp_metrics = Arc::new(LineProtocolMetrics::new(
            application.metric_registry().as_ref(),
        ));

        Self {
            application,
            server,
            lp_metrics,
            max_request_size: common_state.run_config().max_http_request_size,
            serving_readiness: common_state.serving_readiness().clone(),
            shutdown: CancellationToken::new(),
        }
    }
}

#[async_trait]
impl ServerType for DatabaseServerType {
    type RouteError = ApplicationError;

    fn metric_registry(&self) -> Arc<Registry> {
        Arc::clone(self.application.metric_registry())
    }

    fn trace_collector(&self) -> Option<Arc<dyn TraceCollector>> {
        self.application.trace_collector().clone()
    }

    async fn route_http_request(
        &self,
        req: Request<Body>,
    ) -> Result<Response<Body>, Self::RouteError> {
        self::http::route_request(self, req).await
    }

    async fn server_grpc(self: Arc<Self>, builder_input: RpcBuilderInput) -> Result<(), RpcError> {
        self::rpc::server_grpc(self, builder_input).await
    }

    async fn join(self: Arc<Self>) {
        let server_worker = self.server.join().fuse();
        futures::pin_mut!(server_worker);

        futures::select! {
            _ = server_worker => {},
            _ = self.shutdown.cancelled().fuse() => {},
        }

        self.server.shutdown();

        if !server_worker.is_terminated() {
            match server_worker.await {
                Ok(_) => info!("server worker shutdown"),
                Err(error) => error!(%error, "server worker error"),
            }
        }

        info!("server completed shutting down");

        self.application.join();
        info!("shared application state completed shutting down");
    }

    fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        clap_blocks::run_config::RunConfig,
        commands::run::database::Config,
        influxdb_ioxd::{
            grpc_listener, http_listener, serve,
            server_type::database::setup::{make_application, make_server},
        },
    };

    use super::*;
    use ::http::{header::HeaderName, HeaderValue};
    use clap::Parser;
    use data_types::{database_rules::DatabaseRules, DatabaseName};
    use futures::pin_mut;
    use influxdb_iox_client::{connection::Connection, flight::PerformQuery};
    use server::rules::ProvidedDatabaseRules;
    use std::{convert::TryInto, net::SocketAddr, num::NonZeroU64};
    use test_helpers::assert_error;
    use tokio::task::JoinHandle;
    use trace::{
        span::{Span, SpanStatus},
        RingBufferTraceCollector,
    };
    use trace_exporters::export::{AsyncExporter, TestAsyncExporter};

    fn test_config(server_id: Option<u32>) -> Config {
        let mut config = Config::parse_from(&[
            "run",
            "--api-bind",
            "127.0.0.1:0",
            "--grpc-bind",
            "127.0.0.1:0",
        ]);
        config.run_config.server_id_config.server_id = server_id.map(|x| x.try_into().unwrap());
        config
    }

    async fn test_serve(
        config: RunConfig,
        application: Arc<ApplicationState>,
        server: Arc<Server>,
    ) -> Result<(), crate::influxdb_ioxd::Error> {
        let grpc_listener = grpc_listener(config.grpc_bind_address.into())
            .await
            .unwrap();
        let http_listener = http_listener(config.grpc_bind_address.into())
            .await
            .unwrap();

        let common_state = CommonServerState::from_config(config).unwrap();
        let server_type = Arc::new(DatabaseServerType::new(application, server, &common_state));

        serve(common_state, grpc_listener, http_listener, server_type).await
    }

    #[tokio::test]
    async fn test_server_shutdown() {
        test_helpers::maybe_start_logging();

        // Create a server and wait for it to initialize
        let config = test_config(Some(23));
        let application = make_application(&config, None).await.unwrap();
        let server = make_server(Arc::clone(&application), &config);
        server.wait_for_init().await.unwrap();

        // Start serving
        let serve_fut = test_serve(config.run_config, application, Arc::clone(&server)).fuse();
        pin_mut!(serve_fut);

        // Nothing to trigger termination, so serve future should continue running
        futures::select! {
            _ = serve_fut => panic!("serve shouldn't finish"),
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)).fuse() => {}
        }

        // Trigger shutdown
        server.shutdown();

        // The serve future should now complete
        futures::select! {
            _ = serve_fut => {},
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)).fuse() => panic!("timeout shouldn't expire")
        }
    }

    #[tokio::test]
    async fn test_server_shutdown_uninit() {
        // Create a server but don't set a server id
        let config = test_config(None);
        let application = make_application(&config, None).await.unwrap();
        let server = make_server(Arc::clone(&application), &config);

        let serve_fut = test_serve(config.run_config, application, Arc::clone(&server)).fuse();
        pin_mut!(serve_fut);

        // Nothing should have triggered shutdown so serve shouldn't finish
        futures::select! {
            _ = serve_fut => panic!("serve shouldn't finish"),
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)).fuse() => {}
        }

        // We never set the server ID and so the server should not initialize
        assert!(
            server.wait_for_init().now_or_never().is_none(),
            "shouldn't have initialized"
        );

        // But it should still be possible to shut it down
        server.shutdown();

        futures::select! {
            _ = serve_fut => {},
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)).fuse() => panic!("timeout shouldn't expire")
        }
    }

    #[tokio::test]
    async fn test_server_panic() {
        // Create a server and wait for it to initialize
        let config = test_config(Some(999999999));
        let application = make_application(&config, None).await.unwrap();
        let server = make_server(Arc::clone(&application), &config);
        server.wait_for_init().await.unwrap();

        let serve_fut = test_serve(config.run_config, application, Arc::clone(&server)).fuse();
        pin_mut!(serve_fut);

        // Nothing should have triggered shutdown so serve shouldn't finish
        futures::select! {
            _ = serve_fut => panic!("serve shouldn't finish"),
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)).fuse() => {}
        }

        // Trigger a panic in the Server background worker
        db::utils::register_panic_key("server background worker: 999999999");

        // This should trigger shutdown
        futures::select! {
            _ = serve_fut => {},
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)).fuse() => panic!("timeout shouldn't expire")
        }
    }

    #[tokio::test]
    async fn test_database_panic() {
        // Create a server and wait for it to initialize
        let config = test_config(Some(23));
        let application = make_application(&config, None).await.unwrap();
        let server = make_server(Arc::clone(&application), &config);
        server.wait_for_init().await.unwrap();

        // Create a database that won't panic
        let other_db_name = DatabaseName::new("other").unwrap();
        server
            .create_database(make_rules(&other_db_name))
            .await
            .unwrap();

        let other_db = server.database(&other_db_name).unwrap();

        let serve_fut = test_serve(
            config.run_config,
            Arc::clone(&application),
            Arc::clone(&server),
        )
        .fuse();
        pin_mut!(serve_fut);

        // Nothing should have triggered shutdown so serve shouldn't finish
        futures::select! {
            _ = serve_fut => panic!("serve shouldn't finish"),
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)).fuse() => {}
        }

        // Configure a panic in the worker of the database we're about to create
        db::utils::register_panic_key("database background worker: panic_test");

        // Spawn a dummy job that will delay shutdown as it runs to completion
        let task = application
            .job_registry()
            .spawn_dummy_job(vec![1_000_000_000], None);

        // Create database that will panic in its worker loop
        server
            .create_database(make_rules("panic_test"))
            .await
            .unwrap();

        // The serve future shouldn't resolve until the dummy job finishes
        futures::select! {
            _ = serve_fut => panic!("should wait for jobs to finish"),
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)).fuse() => {}
        }

        assert!(!task.is_complete(), "task should still be running");

        // But the databases should have been shutdown
        assert!(
            other_db.join().now_or_never().is_some(),
            "database should have been terminated and have finished"
        );

        // Once the dummy job completes - the serve future should resolve
        futures::select! {
            _ = serve_fut => {},
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)).fuse() => panic!("timeout shouldn't expire")
        }

        assert_eq!(
            task.get_status().result().unwrap(),
            tracker::TaskResult::Success
        )
    }

    async fn jaeger_client(addr: SocketAddr, trace: &'static str) -> Connection {
        influxdb_iox_client::connection::Builder::default()
            .header(
                HeaderName::from_static("uber-trace-id"),
                HeaderValue::from_static(trace),
            )
            .build(format!("http://{}", addr))
            .await
            .unwrap()
    }

    async fn tracing_server<T: TraceCollector + 'static>(
        collector: &Arc<T>,
    ) -> (
        SocketAddr,
        Arc<Server>,
        JoinHandle<crate::influxdb_ioxd::Result<()>>,
    ) {
        let config = test_config(Some(23));
        let application = make_application(&config, Some(Arc::<T>::clone(collector)))
            .await
            .unwrap();
        let server = make_server(Arc::clone(&application), &config);
        server.wait_for_init().await.unwrap();

        let grpc_listener = grpc_listener(config.run_config.grpc_bind_address.into())
            .await
            .unwrap();
        let http_listener = http_listener(config.run_config.grpc_bind_address.into())
            .await
            .unwrap();

        let addr = grpc_listener.local_addr().unwrap();

        let common_state = CommonServerState::from_config(config.run_config.clone()).unwrap();
        let server_type = Arc::new(DatabaseServerType::new(
            application,
            Arc::clone(&server),
            &common_state,
        ));

        let fut = serve(common_state, grpc_listener, http_listener, server_type);

        let join = tokio::spawn(fut);
        (addr, server, join)
    }

    #[tokio::test]
    async fn test_tracing() {
        let trace_collector = Arc::new(RingBufferTraceCollector::new(20));
        let (addr, server, join) = tracing_server(&trace_collector).await;

        let client = influxdb_iox_client::connection::Builder::default()
            .build(format!("http://{}", addr))
            .await
            .unwrap();

        let mut client = influxdb_iox_client::management::Client::new(client);

        client.list_database_names().await.unwrap();

        assert_eq!(trace_collector.spans().len(), 0);

        let b3_tracing_client = influxdb_iox_client::connection::Builder::default()
            .header(
                HeaderName::from_static("x-b3-sampled"),
                HeaderValue::from_static("1"),
            )
            .header(
                HeaderName::from_static("x-b3-traceid"),
                HeaderValue::from_static("fea24902"),
            )
            .header(
                HeaderName::from_static("x-b3-spanid"),
                HeaderValue::from_static("ab3409"),
            )
            .build(format!("http://{}", addr))
            .await
            .unwrap();

        let mut b3_tracing_client = influxdb_iox_client::management::Client::new(b3_tracing_client);

        b3_tracing_client.list_database_names().await.unwrap();
        b3_tracing_client.get_server_status().await.unwrap();

        let conn = jaeger_client(addr, "34f9495:30e34:0:1").await;
        influxdb_iox_client::management::Client::new(conn)
            .list_database_names()
            .await
            .unwrap();

        let spans = trace_collector.spans();
        assert_eq!(spans.len(), 3);

        assert_eq!(spans[0].name, "IOx");
        assert_eq!(spans[0].ctx.parent_span_id.unwrap().0.get(), 0xab3409);
        assert_eq!(spans[0].ctx.trace_id.0.get(), 0xfea24902);
        assert!(spans[0].start.is_some());
        assert!(spans[0].end.is_some());
        assert_eq!(spans[0].status, SpanStatus::Ok);

        assert_eq!(spans[1].name, "IOx");
        assert_eq!(spans[1].ctx.parent_span_id.unwrap().0.get(), 0xab3409);
        assert_eq!(spans[1].ctx.trace_id.0.get(), 0xfea24902);
        assert!(spans[1].start.is_some());
        assert!(spans[1].end.is_some());

        assert_eq!(spans[2].name, "IOx");
        assert_eq!(spans[2].ctx.parent_span_id.unwrap().0.get(), 0x30e34);
        assert_eq!(spans[2].ctx.trace_id.0.get(), 0x34f9495);
        assert!(spans[2].start.is_some());
        assert!(spans[2].end.is_some());

        assert_ne!(spans[0].ctx.span_id, spans[1].ctx.span_id);

        // shutdown server early
        server.shutdown();
        let res = join.await.unwrap();
        assert_error!(res, crate::influxdb_ioxd::Error::LostServer);
    }

    /// Ensure that query is fully executed.
    async fn consume_query(mut query: PerformQuery) {
        while query.next().await.unwrap().is_some() {}
    }

    #[tokio::test]
    async fn test_query_tracing() {
        let collector = Arc::new(RingBufferTraceCollector::new(1000));
        let (addr, server, join) = tracing_server(&collector).await;
        let conn = jaeger_client(addr, "34f8495:35e32:0:1").await;

        let db_info = influxdb_storage_client::OrgAndBucket::new(
            NonZeroU64::new(12).unwrap(),
            NonZeroU64::new(344).unwrap(),
        );

        // Perform a number of different requests to generate traces

        let mut management = influxdb_iox_client::management::Client::new(conn.clone());
        management
            .create_database(
                influxdb_iox_client::management::generated_types::DatabaseRules {
                    name: db_info.db_name().to_string(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let mut write = influxdb_iox_client::write::Client::new(conn.clone());
        write
            .write_lp(db_info.db_name(), "cpu,tag0=foo val=1 100\n", 0)
            .await
            .unwrap();

        let mut flight = influxdb_iox_client::flight::Client::new(conn.clone());
        consume_query(
            flight
                .perform_query(db_info.db_name(), "select * from cpu;")
                .await
                .unwrap(),
        )
        .await;

        flight
            .perform_query("nonexistent", "select * from cpu;")
            .await
            .unwrap_err();

        flight
            .perform_query(db_info.db_name(), "select * from nonexistent;")
            .await
            .unwrap_err();

        let mut storage = influxdb_storage_client::Client::new(conn);
        storage
            .tag_values(influxdb_storage_client::generated_types::TagValuesRequest {
                tags_source: Some(influxdb_storage_client::Client::read_source(&db_info, 1)),
                range: None,
                predicate: None,
                tag_key: "tag0".into(),
            })
            .await
            .unwrap();

        // early shutdown
        server.shutdown();
        let res = join.await.unwrap();
        assert_error!(res, crate::influxdb_ioxd::Error::LostServer);

        // Check generated traces

        let spans = collector.spans();

        let root_spans: Vec<_> = spans.iter().filter(|span| &span.name == "IOx").collect();
        // Made 6 requests
        assert_eq!(root_spans.len(), 6);

        let child = |parent: &Span, name: &'static str| -> Option<&Span> {
            spans.iter().find(|span| {
                span.ctx.parent_span_id == Some(parent.ctx.span_id) && span.name == name
            })
        };

        // Test SQL
        let sql_query_span = root_spans[2];
        assert_eq!(sql_query_span.status, SpanStatus::Ok);

        let ctx_span = child(sql_query_span, "Query Execution").unwrap();
        let planner_span = child(ctx_span, "Planner").unwrap();
        let sql_span = child(planner_span, "sql").unwrap();
        let prepare_sql_span = child(sql_span, "prepare_sql").unwrap();
        child(prepare_sql_span, "prepare_plan").unwrap();

        let execute_span = child(ctx_span, "execute_stream_partitioned").unwrap();

        // validate spans from DataFusion ExecutionPlan are present
        child(execute_span, "ProjectionExec: expr").unwrap();

        let database_not_found = root_spans[3];
        assert_eq!(database_not_found.status, SpanStatus::Err);
        assert!(database_not_found
            .events
            .iter()
            .any(|event| event.msg.as_ref() == "not found"));

        let table_not_found = root_spans[4];
        assert_eq!(table_not_found.status, SpanStatus::Err);
        assert!(table_not_found
            .events
            .iter()
            .any(|event| event.msg.as_ref() == "invalid argument"));

        // Test tag_values
        let storage_span = root_spans[5];
        let ctx_span = child(storage_span, "Query Execution").unwrap();
        child(ctx_span, "Planner").unwrap();

        let to_string_set = child(ctx_span, "to_string_set").unwrap();
        child(to_string_set, "run_logical_plans").unwrap();
    }

    #[tokio::test]
    async fn test_async_exporter() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(20);
        let collector = Arc::new(AsyncExporter::new(TestAsyncExporter::new(sender)));

        let (addr, server, join) = tracing_server(&collector).await;
        let conn = jaeger_client(addr, "34f8495:30e34:0:1").await;
        influxdb_iox_client::management::Client::new(conn)
            .list_database_names()
            .await
            .unwrap();

        collector.drain().await.unwrap();

        // early shutdown
        server.shutdown();
        let res = join.await.unwrap();
        assert_error!(res, crate::influxdb_ioxd::Error::LostServer);

        let span = receiver.recv().await.unwrap();
        assert_eq!(span.ctx.trace_id.get(), 0x34f8495);
        assert_eq!(span.ctx.parent_span_id.unwrap().get(), 0x30e34);
    }

    fn make_rules(db_name: impl Into<String>) -> ProvidedDatabaseRules {
        let db_name = DatabaseName::new(db_name.into()).unwrap();
        ProvidedDatabaseRules::new_rules(DatabaseRules::new(db_name).into())
            .expect("Tests should create valid DatabaseRules")
    }
}
