use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_util::assert_batches_eq;
use data_types::delete_predicate::{DeleteExpr, DeletePredicate, Op, Scalar};
use data_types::server_id::ServerId;
use data_types::timestamp::TimestampRange;
use data_types::DatabaseName;
use dml::{DmlDelete, DmlOperation, DmlWrite};
use generated_types::influxdata::iox::{
    management::v1::{DatabaseRules, LifecycleRules},
    write_buffer::v1::{write_buffer_connection::Direction, WriteBufferConnection},
};
use mutable_batch_lp::lines_to_batches;
use query::exec::ExecutionContextProvider;
use query::frontend::sql::SqlQueryPlanner;
use server::connection::test_helpers::TestConnectionManager;
use server::rules::ProvidedDatabaseRules;
use server::test_utils::{make_application, make_initialized_server};
use server::{Db, Server};
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
    db_name: DatabaseName<'static>,
    router: Arc<Server<TestConnectionManager>>,
    // TODO: Replace with router (#2980)
    router_db: Arc<Db>,

    consumer: Arc<Server<TestConnectionManager>>,
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

        let mut write_buffer_connection = WriteBufferConnection {
            direction: Direction::Write as _,
            r#type: "mock".to_string(),
            connection: "my_mock".to_string(),
            connection_config: Default::default(),
            creation_config: None,
        };

        // Create a router
        let router_id = ServerId::new(NonZeroU32::new(1).unwrap());
        let router = make_initialized_server(router_id, Arc::clone(&application)).await;

        let router_db = router
            .create_database(
                ProvidedDatabaseRules::new_rules(DatabaseRules {
                    name: db_name.to_string(),
                    write_buffer_connection: Some(write_buffer_connection.clone()),
                    lifecycle_rules: Some(LifecycleRules {
                        immutable: true,
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .unwrap(),
            )
            .await
            .unwrap()
            .initialized_db()
            .unwrap();

        // Create a consumer
        let consumer_id = ServerId::new(NonZeroU32::new(2).unwrap());
        let consumer = make_initialized_server(consumer_id, Arc::clone(&application)).await;

        write_buffer_connection.direction = Direction::Read as _;

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
            db_name: db_name.clone(),
            router,
            router_db,
            consumer,
            consumer_db,
        }
    }

    /// Wait for the consumer to have the following tables
    pub async fn wait_for_tables(&self, expected_tables: &[String]) {
        let now = Instant::now();
        loop {
            if now.elapsed() > Duration::from_secs(10) {
                panic!("consumer failed to receive write");
            }

            let mut tables = self.consumer_db.table_names();
            tables.sort_unstable();

            if tables.len() != expected_tables.len() {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            assert_eq!(&tables, expected_tables);
            break;
        }
    }

    /// Write line protocol
    pub async fn write(&self, lp: &str) {
        self.router
            .write(
                &self.db_name,
                DmlWrite::new(lines_to_batches(lp, 0).unwrap(), Default::default()),
            )
            .await
            .unwrap();
    }

    pub async fn delete(&self, delete: DmlDelete) {
        // TODO: Write to router not Db (#2980)
        self.router_db
            .route_operation(&DmlOperation::Delete(delete))
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
        self.router.shutdown();
        self.consumer.shutdown();
        self.router.join().await.unwrap();
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
                range: TimestampRange { start: 0, end: 20 },
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
    fixture
        .wait_for_tables(&["bar".to_string(), "foo".to_string()])
        .await;

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
