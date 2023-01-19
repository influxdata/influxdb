use arrow_util::assert_batches_sorted_eq;
use data_types::{NamespaceId, TableId};
use futures::FutureExt;
use generated_types::{influxdata::iox::ingester::v1 as proto, ingester::IngesterQueryRequest};
use http::StatusCode;
use influxdb_iox_client::flight::generated_types::IngesterQueryResponseMetadata;
use prost::Message;
use test_helpers_end_to_end::{
    get_write_token, maybe_skip_integration, wait_for_readable, MiniCluster, Step, StepTest,
    StepTestState,
};

/// Temporary duplication: These tests should be kept in sync (as far as what they're logically
/// testing, not the exact implementation) with the corresponding tests in the
/// `kafkaless_rpc_write` module.
///
/// When we switch to the RPC write path, this module can be deleted.
mod with_kafka {
    use arrow_flight::error::FlightError;

    use super::*;

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

        // query the ingester
        let query = IngesterQueryRequest::new(
            cluster.namespace_id().await,
            cluster.table_id(table_name).await,
            vec![],
            Some(::predicate::EMPTY_PREDICATE),
        );
        let query: proto::IngesterQueryRequest = query.try_into().unwrap();
        let ingester_response = cluster.query_ingester(query).await.unwrap();

        let partition_id = ingester_response.app_metadata.partition_id;
        assert_eq!(
            ingester_response.app_metadata,
            IngesterQueryResponseMetadata {
                partition_id,
                status: Some(proto::PartitionStatus {
                    parquet_max_sequence_number: None,
                }),
                ingester_uuid: String::new(),
                completed_persistence_count: 0,
            },
        );

        let schema = ingester_response.schema.unwrap();

        let expected = [
            "+------+------+--------------------------------+-----+",
            "| tag1 | tag2 | time                           | val |",
            "+------+------+--------------------------------+-----+",
            "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
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
                assert_eq!(
                    schema,
                    b.schema(),
                    "Schema mismatch for returned batch {}",
                    i
                );
            });
    }

    #[tokio::test]
    async fn ingester_flight_api_namespace_not_found() {
        test_helpers::maybe_start_logging();
        let database_url = maybe_skip_integration!();

        // Set up cluster
        let cluster = MiniCluster::create_shared(database_url).await;

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
        let cluster = MiniCluster::create_shared(database_url).await;

        // Write some data into the v2 HTTP API ==============
        let lp = String::from("my_table,tag1=A,tag2=B val=42i 123456");
        let response = cluster.write_to_router(lp).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // wait for the write to become visible
        let write_token = get_write_token(&response);
        wait_for_readable(write_token, cluster.ingester().ingester_grpc_connection()).await;

        let query = IngesterQueryRequest::new(
            cluster.namespace_id().await,
            TableId::new(i64::MAX),
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
}

/// Temporary duplication: These tests should be kept in sync (as far as what they're logically
/// testing, not the exact implementation) with the corresponding tests in the
/// `with_kafka` module.
///
/// When we switch to the RPC write path, the code in this module can be unwrapped into its super
/// scope and unindented.
mod kafkaless_rpc_write {
    use arrow_flight::{error::FlightError, Ticket};

    use super::*;

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
                        let ingester_response =
                            state.cluster().query_ingester(query).await.unwrap();

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

        // Write some data into the v2 HTTP API ==============
        let lp = format!("{},tag1=A,tag2=B val=42i 123456", table_name);
        let response = cluster.write_to_router(lp).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

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
                assert_eq!(
                    schema,
                    b.schema(),
                    "Schema mismatch for returned batch {}",
                    i
                );
            });

        // Ensure the ingester UUID is the same in the next query
        let ingester_response = cluster.query_ingester(query.clone()).await.unwrap();
        assert_eq!(ingester_response.app_metadata.ingester_uuid, ingester_uuid);

        // Restart the ingester
        cluster.restart_ingester().await;

        // Populate the ingester with some data so it returns a successful
        // response containing the UUID.
        let lp = format!("{},tag1=A,tag2=B val=42i 123456", table_name);
        let response = cluster.write_to_router(lp).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

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
}
