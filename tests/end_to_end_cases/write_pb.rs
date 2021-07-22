use super::scenario::{create_readable_database, rand_name};
use crate::common::server_fixture::ServerFixture;
use arrow_util::assert_batches_sorted_eq;
use generated_types::influxdata::transfer::column::v1 as pb;

#[tokio::test]
pub async fn test_write_pb() {
    let fixture = ServerFixture::create_single_use().await;
    fixture
        .management_client()
        .update_server_id(42)
        .await
        .expect("set ID failed");
    fixture.wait_server_initialized().await;

    let db_name = rand_name();
    create_readable_database(&db_name, fixture.grpc_channel()).await;

    let database_batch = pb::DatabaseBatch {
        database_name: db_name.clone(),
        table_batches: vec![pb::TableBatch {
            table_name: "mytable".to_string(),
            columns: vec![
                pb::Column {
                    column_name: "time".to_string(),
                    semantic_type: pb::column::SemanticType::Time as i32,
                    values: Some(pb::column::Values {
                        i64_values: vec![3],
                        f64_values: vec![],
                        u64_values: vec![],
                        string_values: vec![],
                        bool_values: vec![],
                        bytes_values: vec![],
                    }),
                    null_mask: vec![],
                },
                pb::Column {
                    column_name: "mycol1".to_string(),
                    semantic_type: pb::column::SemanticType::Iox as i32,
                    values: Some(pb::column::Values {
                        i64_values: vec![5],
                        f64_values: vec![],
                        u64_values: vec![],
                        string_values: vec![],
                        bool_values: vec![],
                        bytes_values: vec![],
                    }),
                    null_mask: vec![],
                },
            ],
            row_count: 1,
        }],
    };
    let write_request = pb::WriteRequest {
        database_batch: Some(database_batch),
    };

    fixture
        .write_client()
        .write_pb(write_request)
        .await
        .expect("cannot write");

    let mut query_results = fixture
        .flight_client()
        .perform_query(&db_name, "select * from mytable")
        .await
        .unwrap();

    let mut batches = Vec::new();
    while let Some(data) = query_results.next().await.unwrap() {
        batches.push(data);
    }

    let expected = vec![
        "+--------+--------------------------------+",
        "| mycol1 | time                           |",
        "+--------+--------------------------------+",
        "| 5      | 1970-01-01T00:00:00.000000003Z |",
        "+--------+--------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);
}
