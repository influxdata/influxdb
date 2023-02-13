use arrow_flight::{error::FlightError, Ticket};
use arrow_util::assert_batches_sorted_eq;
use data_types::{NamespaceId, TableId};
use futures::FutureExt;
use generated_types::{influxdata::iox::ingester::v1 as proto, ingester::IngesterQueryRequest};
use http::StatusCode;
use prost::Message;
use test_helpers_end_to_end::{maybe_skip_integration, MiniCluster, Step, StepTest, StepTestState};

#[tokio::test]
async fn persist_on_demand() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "mytable";
    let mut cluster = MiniCluster::create_shared2_never_persist(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::RecordNumParquetFiles,
            Step::WriteLineProtocol(format!("{table_name},tag1=A,tag2=B val=42i 123456")),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    // query the ingester
                    let query = IngesterQueryRequest::new(
                        state.cluster().namespace_id().await,
                        state.cluster().table_id(table_name).await,
                        vec![],
                        Some(::predicate::EMPTY_PREDICATE),
                    );
                    let query: proto::IngesterQueryRequest = query.try_into().unwrap();
                    let ingester_response = state.cluster().query_ingester(query).await.unwrap();

                    let ingester_uuid = ingester_response.app_metadata.ingester_uuid.clone();
                    assert!(!ingester_uuid.is_empty());

                    let expected = [
                        "+------+------+--------------------------------+-----+",
                        "| tag1 | tag2 | time                           | val |",
                        "+------+------+--------------------------------+-----+",
                        "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                        "+------+------+--------------------------------+-----+",
                    ];
                    assert_batches_sorted_eq!(&expected, &ingester_response.record_batches);
                }
                .boxed()
            })),
            Step::Persist,
            Step::WaitForPersisted2 {
                expected_increase: 1,
            },
            // Ensure the ingester responds with the correct file count to tell the querier
            // it needs to expire its catalog cache
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let query = IngesterQueryRequest::new(
                        state.cluster().namespace_id().await,
                        state.cluster().table_id(table_name).await,
                        vec![],
                        Some(::predicate::EMPTY_PREDICATE),
                    );
                    let query: proto::IngesterQueryRequest = query.try_into().unwrap();
                    let ingester_response =
                        state.cluster().query_ingester(query.clone()).await.unwrap();

                    let num_files_persisted =
                        ingester_response.app_metadata.completed_persistence_count;
                    assert_eq!(num_files_persisted, 1);
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn ingester_flight_api() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "mytable";

    // Set up cluster
    // Don't use a shared cluster because the ingester is going to be restarted
    let mut cluster = MiniCluster::create_non_shared2(database_url).await;

    // Write some data into the v2 HTTP API to set up the namespace and schema ==============
    let lp = format!("{table_name},tag1=A,tag2=B val=42i 123456");
    let response = cluster.write_to_router(lp).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Write some data directly into the ingester through its gRPC API
    let lp = format!("{table_name},tag1=B,tag2=A val=84i 1234567");
    cluster.write_to_ingester(lp, table_name).await;

    // query the ingester
    let query = IngesterQueryRequest::new(
        cluster.namespace_id().await,
        cluster.table_id(table_name).await,
        vec![],
        Some(::predicate::EMPTY_PREDICATE),
    );
    let query: proto::IngesterQueryRequest = query.try_into().unwrap();
    let ingester_response = cluster.query_ingester(query.clone()).await.unwrap();

    let ingester_uuid = ingester_response.app_metadata.ingester_uuid.clone();
    assert!(!ingester_uuid.is_empty());

    let schema = ingester_response.schema.unwrap();

    let expected = [
        "+------+------+--------------------------------+-----+",
        "| tag1 | tag2 | time                           | val |",
        "+------+------+--------------------------------+-----+",
        "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
        "| B    | A    | 1970-01-01T00:00:00.001234567Z | 84  |",
        "+------+------+--------------------------------+-----+",
    ];
    assert_batches_sorted_eq!(&expected, &ingester_response.record_batches);

    // Also ensure that the schema of the batches matches what is
    // reported by the performed_query.
    ingester_response
        .record_batches
        .iter()
        .enumerate()
        .for_each(|(i, b)| {
            assert_eq!(schema, b.schema(), "Schema mismatch for returned batch {i}");
        });

    // Ensure the ingester UUID is the same in the next query
    let ingester_response = cluster.query_ingester(query.clone()).await.unwrap();
    assert_eq!(ingester_response.app_metadata.ingester_uuid, ingester_uuid);

    // Restart the ingesters
    cluster.restart_ingesters().await;

    // Populate the ingester with some data so it returns a successful
    // response containing the UUID.
    let lp = format!("{table_name},tag1=A,tag2=B val=42i 123456");
    cluster.write_to_ingester(lp, table_name).await;

    // Query for the new UUID and assert it has changed.
    let ingester_response = cluster.query_ingester(query).await.unwrap();
    assert_ne!(ingester_response.app_metadata.ingester_uuid, ingester_uuid);
}

#[tokio::test]
async fn ingester_flight_api_namespace_not_found() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    // Set up cluster
    let cluster = MiniCluster::create_shared2(database_url).await;

    // query the ingester
    let query = IngesterQueryRequest::new(
        NamespaceId::new(i64::MAX),
        TableId::new(42),
        vec![],
        Some(::predicate::EMPTY_PREDICATE),
    );
    let query: proto::IngesterQueryRequest = query.try_into().unwrap();
    let err = cluster.query_ingester(query).await.unwrap_err();

    if let FlightError::Tonic(status) = err {
        assert_eq!(status.code(), tonic::Code::NotFound);
    } else {
        panic!("Wrong error variant: {err}")
    }
}

#[tokio::test]
async fn ingester_flight_api_table_not_found() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    // Set up cluster
    let cluster = MiniCluster::create_shared2(database_url).await;

    // Write some data into the v2 HTTP API ==============
    let lp = String::from("my_table,tag1=A,tag2=B val=42i 123456");
    let response = cluster.write_to_router(lp).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let mut querier_flight =
        influxdb_iox_client::flight::Client::new(cluster.ingester().ingester_grpc_connection())
            .into_inner();

    let query = IngesterQueryRequest::new(
        cluster.namespace_id().await,
        TableId::new(i64::MAX),
        vec![],
        Some(::predicate::EMPTY_PREDICATE),
    );
    let query: proto::IngesterQueryRequest = query.try_into().unwrap();

    let ticket = Ticket {
        ticket: query.encode_to_vec().into(),
    };

    let err = querier_flight.do_get(ticket).await.unwrap_err();
    if let FlightError::Tonic(status) = err {
        assert_eq!(status.code(), tonic::Code::NotFound);
    } else {
        panic!("Wrong error variant: {err}")
    }
}
