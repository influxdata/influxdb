use super::scenario::rand_name;
use crate::common::server_fixture::ServerFixture;
use arrow_util::assert_batches_sorted_eq;
use generated_types::influxdata::iox::management::v1::DatabaseRules;

type Result<T, E = Box<dyn std::error::Error>> = std::result::Result<T, E>;

#[tokio::test]
async fn test_querying_deleted_database() {
    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();
    let mut write_client = fixture.write_client();
    let mut flight_client = fixture.flight_client();

    let db_name = rand_name();
    let rules = DatabaseRules {
        name: db_name.clone(),
        ..Default::default()
    };

    // Add some data to the first generation of the database and
    // ensure the data is returned.

    management_client
        .create_database(rules.clone())
        .await
        .expect("create database failed");
    let num_lines_written = write_client
        .write(&db_name, "cpu,region=west user=12.3 100")
        .await
        .expect("cannot write");

    assert_eq!(num_lines_written, 1);

    let batches = query_cpu_to_batches(&mut flight_client, &db_name)
        .await
        .expect("Unable to query");

    let expected = [
        "+--------+--------------------------------+------+",
        "| region | time                           | user |",
        "+--------+--------------------------------+------+",
        "| west   | 1970-01-01T00:00:00.000000100Z | 12.3 |",
        "+--------+--------------------------------+------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    // Ensure we get an error after deleting the database

    management_client.delete_database(&db_name).await.unwrap();

    assert!(query_cpu_to_batches(&mut flight_client, &db_name)
        .await
        .is_err());

    // Create a database of the same name and ensure that only the new
    // data is returned.

    management_client
        .create_database(rules)
        .await
        .expect("create database failed");
    let num_lines_written = write_client
        .write(&db_name, "cpu,region=east user=99.9 200")
        .await
        .expect("cannot write");

    assert_eq!(num_lines_written, 1);

    let batches = query_cpu_to_batches(&mut flight_client, &db_name)
        .await
        .expect("Unable to query");

    let expected = [
        "+--------+--------------------------------+------+",
        "| region | time                           | user |",
        "+--------+--------------------------------+------+",
        "| east   | 1970-01-01T00:00:00.000000200Z | 99.9 |",
        "+--------+--------------------------------+------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);
}

async fn query_cpu_to_batches(
    flight_client: &mut influxdb_iox_client::flight::Client,
    db_name: &str,
) -> Result<Vec<arrow::record_batch::RecordBatch>> {
    let mut query_results = flight_client
        .perform_query(db_name, "select * from cpu")
        .await?;

    let mut batches = Vec::new();
    while let Some(data) = query_results.next().await? {
        batches.push(data);
    }

    Ok(batches)
}
