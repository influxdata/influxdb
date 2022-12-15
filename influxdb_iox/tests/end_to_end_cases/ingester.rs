use arrow_util::assert_batches_sorted_eq;
use data_types::{NamespaceId, TableId};
use generated_types::{
    influxdata::iox::ingester::v1::PartitionStatus, ingester::IngesterQueryRequest,
};
use http::StatusCode;
use influxdb_iox_client::flight::generated_types::IngesterQueryResponseMetadata;
use test_helpers_end_to_end::{
    get_write_token, maybe_skip_integration, wait_for_readable, MiniCluster,
};

#[tokio::test]
async fn ingester_flight_api() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "mytable";

    // Set up cluster
    let cluster = MiniCluster::create_shared(database_url).await;

    // Write some data into the v2 HTTP API ==============
    let lp = format!("{},tag1=A,tag2=B val=42i 123456", table_name);
    let response = cluster.write_to_router(lp).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // wait for the write to become visible
    let write_token = get_write_token(&response);
    wait_for_readable(write_token, cluster.ingester().ingester_grpc_connection()).await;

    let mut querier_flight = influxdb_iox_client::flight::low_level::Client::<
        influxdb_iox_client::flight::generated_types::IngesterQueryRequest,
    >::new(cluster.ingester().ingester_grpc_connection(), None);

    let query = IngesterQueryRequest::new(
        cluster.namespace_id().await,
        cluster.table_id(table_name).await,
        vec![],
        Some(::predicate::EMPTY_PREDICATE),
    );

    let mut performed_query = querier_flight
        .perform_query(query.try_into().unwrap())
        .await
        .unwrap();

    let (msg, app_metadata) = performed_query.next().await.unwrap().unwrap();
    msg.unwrap_none();
    let partition_id = app_metadata.partition_id;
    assert_eq!(
        app_metadata,
        IngesterQueryResponseMetadata {
            partition_id,
            status: Some(PartitionStatus {
                parquet_max_sequence_number: None,
            }),
            ingester_uuid: String::new(),
            completed_persistence_count: 0,
        },
    );

    let (msg, _) = performed_query.next().await.unwrap().unwrap();
    let schema = msg.unwrap_schema();

    let mut query_results = vec![];
    while let Some((msg, _md)) = performed_query.next().await.unwrap() {
        let batch = msg.unwrap_record_batch();
        query_results.push(batch);
    }

    let expected = [
        "+------+------+--------------------------------+-----+",
        "| tag1 | tag2 | time                           | val |",
        "+------+------+--------------------------------+-----+",
        "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
        "+------+------+--------------------------------+-----+",
    ];
    assert_batches_sorted_eq!(&expected, &query_results);

    // Also ensure that the schema of the batches matches what is
    // reported by the performed_query.
    query_results.iter().enumerate().for_each(|(i, b)| {
        assert_eq!(
            schema,
            b.schema(),
            "Schema mismatch for returned batch {}",
            i
        );
    });
}

#[tokio::test]
async fn ingester2_flight_api() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "mytable";

    // Set up cluster
    let mut cluster = MiniCluster::create_non_shared2(database_url).await;

    // Write some data into the v2 HTTP API ==============
    let lp = format!("{},tag1=A,tag2=B val=42i 123456", table_name);
    let response = cluster.write_to_router(lp).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let mut querier_flight = influxdb_iox_client::flight::low_level::Client::<
        influxdb_iox_client::flight::generated_types::IngesterQueryRequest,
    >::new(cluster.ingester().ingester_grpc_connection(), None);

    let query = IngesterQueryRequest::new(
        cluster.namespace_id().await,
        cluster.table_id(table_name).await,
        vec![],
        Some(::predicate::EMPTY_PREDICATE),
    );

    let mut performed_query = querier_flight
        .perform_query(query.clone().try_into().unwrap())
        .await
        .unwrap();

    let (msg, app_metadata) = performed_query.next().await.unwrap().unwrap();
    msg.unwrap_none();

    let ingester_uuid = app_metadata.ingester_uuid.clone();
    assert!(!ingester_uuid.is_empty());

    let (msg, _) = performed_query.next().await.unwrap().unwrap();
    let schema = msg.unwrap_schema();

    let mut query_results = vec![];
    while let Some((msg, _md)) = performed_query.next().await.unwrap() {
        let batch = msg.unwrap_record_batch();
        query_results.push(batch);
    }

    let expected = [
        "+------+------+--------------------------------+-----+",
        "| tag1 | tag2 | time                           | val |",
        "+------+------+--------------------------------+-----+",
        "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
        "+------+------+--------------------------------+-----+",
    ];
    assert_batches_sorted_eq!(&expected, &query_results);

    // Also ensure that the schema of the batches matches what is
    // reported by the performed_query.
    query_results.iter().enumerate().for_each(|(i, b)| {
        assert_eq!(
            schema,
            b.schema(),
            "Schema mismatch for returned batch {}",
            i
        );
    });

    // Ensure the ingester UUID is the same in the next query
    let mut performed_query = querier_flight
        .perform_query(query.clone().try_into().unwrap())
        .await
        .unwrap();
    let (msg, app_metadata) = performed_query.next().await.unwrap().unwrap();
    msg.unwrap_none();
    assert_eq!(app_metadata.ingester_uuid, ingester_uuid);

    // Restart the ingester and ensure it gets a new UUID
    cluster.restart_ingester().await;
    let mut performed_query = querier_flight
        .perform_query(query.try_into().unwrap())
        .await
        .unwrap();
    let (msg, app_metadata) = performed_query.next().await.unwrap().unwrap();
    msg.unwrap_none();
    assert_ne!(app_metadata.ingester_uuid, ingester_uuid);
}

#[tokio::test]
async fn ingester_flight_api_namespace_not_found() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    // Set up cluster
    let cluster = MiniCluster::create_shared(database_url).await;

    let mut querier_flight = influxdb_iox_client::flight::low_level::Client::<
        influxdb_iox_client::flight::generated_types::IngesterQueryRequest,
    >::new(cluster.ingester().ingester_grpc_connection(), None);

    let query = IngesterQueryRequest::new(
        NamespaceId::new(i64::MAX),
        TableId::new(42),
        vec![],
        Some(::predicate::EMPTY_PREDICATE),
    );

    let err = querier_flight
        .perform_query(query.try_into().unwrap())
        .await
        .unwrap_err();
    if let influxdb_iox_client::flight::Error::GrpcError(status) = err {
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
    let cluster = MiniCluster::create_shared(database_url).await;

    // Write some data into the v2 HTTP API ==============
    let lp = String::from("my_table,tag1=A,tag2=B val=42i 123456");
    let response = cluster.write_to_router(lp).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // wait for the write to become visible
    let write_token = get_write_token(&response);
    wait_for_readable(write_token, cluster.ingester().ingester_grpc_connection()).await;

    let mut querier_flight = influxdb_iox_client::flight::low_level::Client::<
        influxdb_iox_client::flight::generated_types::IngesterQueryRequest,
    >::new(cluster.ingester().ingester_grpc_connection(), None);

    let query = IngesterQueryRequest::new(
        cluster.namespace_id().await,
        TableId::new(i64::MAX),
        vec![],
        Some(::predicate::EMPTY_PREDICATE),
    );

    let err = querier_flight
        .perform_query(query.try_into().unwrap())
        .await
        .unwrap_err();
    if let influxdb_iox_client::flight::Error::GrpcError(status) = err {
        assert_eq!(status.code(), tonic::Code::NotFound);
    } else {
        panic!("Wrong error variant: {err}")
    }
}
