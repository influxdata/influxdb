use super::scenario::{create_readable_database, create_router_to_write_buffer, rand_name};
use crate::common::server_fixture::{ServerFixture, ServerType};
use arrow_util::assert_batches_sorted_eq;
use dml::{test_util::assert_write_op_eq, DmlWrite};
use futures::StreamExt;
use generated_types::influxdata::pbdata::v1 as pb;
use mutable_batch_lp::lines_to_batches;

#[tokio::test]
pub async fn test_write_pb_database() {
    let fixture = ServerFixture::create_shared(ServerType::Database).await;

    let db_name = rand_name();
    create_readable_database(&db_name, fixture.grpc_channel()).await;

    fixture
        .write_client()
        .write_pb(write_request(&db_name))
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

#[tokio::test]
pub async fn test_write_pb_router() {
    let fixture = ServerFixture::create_shared(ServerType::Router).await;

    let db_name = rand_name();
    let (_tmpdir, mut write_buffer) = create_router_to_write_buffer(&fixture, &db_name).await;

    fixture
        .write_client()
        .write_pb(write_request(&db_name))
        .await
        .expect("cannot write");

    let mut stream = write_buffer.streams().into_values().next().unwrap();
    let write_actual = stream.stream.next().await.unwrap().unwrap();
    let write_expected = DmlWrite::new(
        lines_to_batches("mytable mycol1=5 3", 0).unwrap(),
        // We don't care about the metadata here, timestamps and sequence numbers are hard to guess
        write_actual.meta().clone(),
    );
    assert_write_op_eq(&write_actual, &write_expected);
}

fn write_request(db_name: &str) -> pb::WriteRequest {
    let database_batch = pb::DatabaseBatch {
        database_name: db_name.to_string(),
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
                        packed_string_values: None,
                        interned_string_values: None,
                    }),
                    null_mask: vec![],
                },
                pb::Column {
                    column_name: "mycol1".to_string(),
                    semantic_type: pb::column::SemanticType::Field as i32,
                    values: Some(pb::column::Values {
                        i64_values: vec![5],
                        f64_values: vec![],
                        u64_values: vec![],
                        string_values: vec![],
                        bool_values: vec![],
                        bytes_values: vec![],
                        packed_string_values: None,
                        interned_string_values: None,
                    }),
                    null_mask: vec![],
                },
            ],
            row_count: 1,
        }],
    };

    pb::WriteRequest {
        database_batch: Some(database_batch),
    }
}
