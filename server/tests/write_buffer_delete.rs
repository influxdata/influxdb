use arrow_util::assert_batches_eq;
use data_types::{
    delete_predicate::{DeleteExpr, DeletePredicate, Op, Scalar},
    router::{
        Matcher, MatcherToShard, QuerySinks, Router as RouterConfig, ShardConfig, ShardId,
        WriteSink, WriteSinkSet, WriteSinkVariant,
    },
    server_id::ServerId,
    timestamp::TimestampRange,
    DatabaseName,
};
use db::{test_helpers::wait_for_tables, Db};
use dml::{DmlDelete, DmlOperation, DmlWrite};
use generated_types::influxdata::iox::{
    management::v1::DatabaseRules, write_buffer::v1::WriteBufferConnection,
};
use mutable_batch_lp::lines_to_batches;
use query::{exec::ExecutionContextProvider, frontend::sql::SqlQueryPlanner};
use regex::Regex;
use router::{router::Router, server::RouterServer};
use server::{
    rules::ProvidedDatabaseRules,
    test_utils::{make_application, make_initialized_server},
    Server,
};
use std::{collections::BTreeMap, num::NonZeroU32, sync::Arc};
use write_buffer::mock::MockBufferSharedState;

/// A distributed IOx topology consisting of a router and a database, separated by a write buffer
///
/// There is some overlap with `ReplayTest` in `server` and `ServerFixture` in the end-to-end
/// tests. The former is primarily concerned with the interaction of replay and persistence,
/// whilst the latter is concerned with the behaviour of the process as a whole.
///
/// `DistributedTest` sits somewhere in the middle, it is not concerned with the details of
/// persistence or replay, but is still at a low enough level that it can manipulate the server
/// APIs directly and is not restricted to what is exposed over gRPC.
///
/// It primarily exists to test the routing logic.
///
struct DistributedTest {
    router: Arc<Router>,

    consumer: Arc<Server>,
    consumer_db: Arc<Db>,
}

impl DistributedTest {
    /// Create a new DistributedTest
    pub async fn new(db_name: &DatabaseName<'static>) -> Self {
        let write_buffer_state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::new(1).unwrap());

        let application = make_application();
        application
            .write_buffer_factory()
            .register_mock("my_mock".to_string(), write_buffer_state);

        let write_buffer_connection = WriteBufferConnection {
            r#type: "mock".to_string(),
            connection: "my_mock".to_string(),
            connection_config: Default::default(),
            creation_config: None,
        };

        // Create a router
        let router_server = RouterServer::for_testing(
            None,
            None,
            Arc::clone(application.time_provider()),
            Some(Arc::clone(application.write_buffer_factory())),
        )
        .await;
        let router_id = ServerId::new(NonZeroU32::new(1).unwrap());
        router_server.set_server_id(router_id).unwrap();

        router_server.update_router(RouterConfig {
            name: db_name.to_string(),
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
                        sink: WriteSinkVariant::WriteBuffer(
                            write_buffer_connection.clone().try_into().unwrap(),
                        ),
                        ignore_errors: false,
                    }],
                },
            )]),
            query_sinks: QuerySinks::default(),
        });
        let router = router_server.router(db_name).unwrap();

        // Create a consumer
        let consumer_id = ServerId::new(NonZeroU32::new(2).unwrap());
        let consumer = make_initialized_server(consumer_id, Arc::clone(&application)).await;

        let consumer_db = consumer
            .create_database(
                ProvidedDatabaseRules::new_rules(DatabaseRules {
                    name: db_name.to_string(),
                    write_buffer_connection: Some(write_buffer_connection.clone()),
                    ..Default::default()
                })
                .unwrap(),
            )
            .await
            .unwrap()
            .initialized_db()
            .unwrap();

        Self {
            router,
            consumer,
            consumer_db,
        }
    }

    /// Wait for the consumer to have the following tables
    pub async fn wait_for_tables(&self, expected_tables: &[&str]) {
        wait_for_tables(&self.consumer_db, expected_tables).await
    }

    /// Write line protocol
    pub async fn write(&self, lp: &str) {
        self.router
            .write(DmlOperation::Write(DmlWrite::new(
                lines_to_batches(lp, 0).unwrap(),
                Default::default(),
            )))
            .await
            .unwrap();
    }

    pub async fn delete(&self, delete: DmlDelete) {
        // TODO: Write to router not Db (#2980)
        self.router
            .write(DmlOperation::Delete(delete))
            .await
            .unwrap();
    }

    /// Perform a query and assert the result
    pub async fn query(&self, query: &str, expected: &[&'static str]) {
        let ctx = self.consumer_db.new_query_context(None);
        let physical_plan = SqlQueryPlanner::new().query(query, &ctx).await.unwrap();

        let batches = ctx.collect(physical_plan).await.unwrap();

        assert_batches_eq!(expected, &batches);
    }

    /// Shuts down the fixture and waits for the servers to exit
    pub async fn drain(&self) {
        self.consumer.shutdown();
        self.consumer.join().await.unwrap();
    }
}

#[tokio::test]
async fn write_buffer_deletes() {
    let db_name = DatabaseName::new("distributed").unwrap();
    let fixture = DistributedTest::new(&db_name).await;

    // Write some data
    fixture.write("foo x=1 1").await;
    fixture.write("foo x=3 2").await;

    // Send a delete over the write buffer
    fixture
        .delete(DmlDelete::new(
            DeletePredicate {
                range: TimestampRange::new(0, 20),
                exprs: vec![DeleteExpr {
                    column: "x".to_string(),
                    op: Op::Eq,
                    scalar: Scalar::I64(1),
                }],
            },
            None,
            Default::default(),
        ))
        .await;

    // Use a write to a different table to signal consumption has completed by waiting
    // for the this new table to exist in the consumer database
    fixture.write("bar x=2 1").await;

    // Wait for consumer to catch up
    fixture.wait_for_tables(&["bar", "foo"]).await;

    fixture
        .query(
            "select * from foo;",
            &[
                "+--------------------------------+---+",
                "| time                           | x |",
                "+--------------------------------+---+",
                "| 1970-01-01T00:00:00.000000002Z | 3 |",
                "+--------------------------------+---+",
            ],
        )
        .await;

    fixture
        .query(
            "select * from bar;",
            &[
                "+--------------------------------+---+",
                "| time                           | x |",
                "+--------------------------------+---+",
                "| 1970-01-01T00:00:00.000000001Z | 2 |",
                "+--------------------------------+---+",
            ],
        )
        .await;

    fixture.drain().await;
}
