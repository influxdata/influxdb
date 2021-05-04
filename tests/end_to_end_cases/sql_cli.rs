use assert_cmd::Command;
use predicates::prelude::*;

use crate::common::server_fixture::ServerFixture;

use super::scenario::{create_readable_database, create_two_partition_database, rand_name};

#[tokio::test]
async fn test_error_connecting() {
    let addr = "http://hope_this_addr_does_not_exist";
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("sql")
        .arg("--host")
        .arg(addr)
        .write_stdin("exit")
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Error connecting to http://hope_this_addr_does_not_exist",
        ));
}

#[tokio::test]
async fn test_basic() {
    let fixture = ServerFixture::create_shared().await;
    let addr = fixture.grpc_base();
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("sql")
        .arg("--host")
        .arg(addr)
        .write_stdin("exit")
        .assert()
        .success()
        .stdout(
            predicate::str::contains("Ready for commands")
                .and(predicate::str::contains("Connected to IOx Server")),
        );
}

#[tokio::test]
async fn test_help() {
    let fixture = ServerFixture::create_shared().await;
    let addr = fixture.grpc_base();
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("sql")
        .arg("--host")
        .arg(addr)
        .write_stdin("help;")
        .assert()
        .success()
        .stdout(
            predicate::str::contains("# Basic IOx SQL Primer").and(predicate::str::contains(
                "SHOW DATABASES: List databases available on the server",
            )),
        );
}

#[tokio::test]
async fn test_exit() {
    let fixture = ServerFixture::create_shared().await;
    let addr = fixture.grpc_base();
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("sql")
        .arg("--host")
        .arg(addr)
        // help should not be run as it is after the exit command
        .write_stdin("exit;\nhelp;")
        .assert()
        .success()
        .stdout(predicate::str::contains("# Basic IOx SQL Primer").not());
}

#[tokio::test]
async fn test_sql_show_databases() {
    let fixture = ServerFixture::create_shared().await;
    let addr = fixture.grpc_base();

    let db_name1 = rand_name();
    create_readable_database(&db_name1, fixture.grpc_channel()).await;

    let db_name2 = rand_name();
    create_readable_database(&db_name2, fixture.grpc_channel()).await;

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("sql")
        .arg("--host")
        .arg(addr)
        .write_stdin("show databases;")
        .assert()
        .success()
        .stdout(
            predicate::str::contains("| db_name")
                .and(predicate::str::contains(&db_name1))
                .and(predicate::str::contains(&db_name2)),
        );
}

#[tokio::test]
async fn test_sql_use_database() {
    let fixture = ServerFixture::create_shared().await;
    let addr = fixture.grpc_base();

    let db_name = rand_name();
    create_two_partition_database(&db_name, fixture.grpc_channel()).await;

    let expected_output = r#"
+------+---------+----------+---------------------+-------+
| host | running | sleeping | time                | total |
+------+---------+----------+---------------------+-------+
| foo  | 4       | 514      | 2020-06-23 06:38:30 | 519   |
+------+---------+----------+---------------------+-------+
"#
    .trim();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("sql")
        .arg("--host")
        .arg(addr)
        .write_stdin(format!("use {};\n\nselect * from cpu;", db_name))
        .assert()
        .success()
        .stdout(predicate::str::contains(expected_output).and(predicate::str::contains("1 row")));
}

#[tokio::test]
async fn test_sql_format() {
    let fixture = ServerFixture::create_shared().await;
    let addr = fixture.grpc_base();

    let db_name = rand_name();
    create_two_partition_database(&db_name, fixture.grpc_channel()).await;

    let expected_output = r#"
host,running,sleeping,time,total
foo,4,514,2020-06-23T06:38:30.000000000,519
"#
    .trim();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("sql")
        .arg("--host")
        .arg(addr)
        // add flag to
        .arg("--format")
        .arg("csv")
        .write_stdin(format!("use {};\n\nselect * from cpu;", db_name))
        .assert()
        .success()
        .stdout(predicate::str::contains(expected_output));

    // Same command but use `set format` command rather than cli flag
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("sql")
        .arg("--host")
        .arg(addr)
        .write_stdin(format!(
            "use {};\n\nset format csv;\n\nselect * from cpu;",
            db_name
        ))
        .assert()
        .success()
        .stdout(
            predicate::str::contains(expected_output)
                .and(predicate::str::contains("Set output format format to csv")),
        );
}

#[tokio::test]
async fn test_sql_observer() {
    let fixture = ServerFixture::create_shared().await;
    let addr = fixture.grpc_base();

    let expected_output = "You are now in Observer mode";

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("sql")
        .arg("--host")
        .arg(addr)
        // write a query against the aggregated chunks
        .write_stdin("observer;\n\n")
        .assert()
        .success()
        .stdout(predicate::str::contains(expected_output));
}

#[tokio::test]
async fn test_sql_observer_chunks() {
    // test chunks aggregated table
    let fixture = ServerFixture::create_shared().await;
    let addr = fixture.grpc_base();

    let db_name1 = rand_name();
    create_two_partition_database(&db_name1, fixture.grpc_channel()).await;

    let db_name2 = rand_name();
    create_two_partition_database(&db_name2, fixture.grpc_channel()).await;

    let expected_output = r#"
+---------------+------------+
| partition_key | table_name |
+---------------+------------+
| cpu           | cpu        |
| cpu           | cpu        |
| mem           | mem        |
| mem           | mem        |
+---------------+------------+
"#
    .trim();

    let query = format!(
        r#"
select
  partition_key, table_name
from
  chunks
where
  database_name IN ('{}', '{}')
order by
  partition_key, table_name
"#,
        db_name1, db_name2
    );

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("sql")
        .arg("--host")
        .arg(addr)
        .write_stdin(format!("observer;\n\n{};\n", query))
        .assert()
        .success()
        .stdout(predicate::str::contains(expected_output));
}

#[tokio::test]
async fn test_sql_observer_columns() {
    // test columns aggregated table
    let fixture = ServerFixture::create_shared().await;
    let addr = fixture.grpc_base();

    let db_name = rand_name();
    create_two_partition_database(&db_name, fixture.grpc_channel()).await;

    let expected_output = r#"
+---------------+------------+--------------+
| partition_key | table_name | column_count |
+---------------+------------+--------------+
| cpu           | cpu        | 5            |
| mem           | mem        | 5            |
+---------------+------------+--------------+
"#
    .trim();

    let query = format!(
        r#"
select
  partition_key, table_name, count(*) as column_count
from
  columns
where
  database_name = '{}'
group by
  partition_key, table_name
order by
  partition_key, table_name, column_count
"#,
        db_name
    );

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("sql")
        .arg("--host")
        .arg(addr)
        .write_stdin(format!("observer;\n\n{};\n", query))
        .assert()
        .success()
        .stdout(predicate::str::contains(expected_output));
}

#[tokio::test]
async fn test_sql_observer_operations() {
    // test columns aggregated table
    let fixture = ServerFixture::create_shared().await;
    let addr = fixture.grpc_base();

    let db_name = rand_name();
    create_two_partition_database(&db_name, fixture.grpc_channel()).await;

    let mut management_client = fixture.management_client();
    let chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");
    println!("The chunks:\n{:?}", chunks);

    let partition_key = "cpu";
    let table_name = "cpu";
    // Move the chunk to read buffer
    let operation = management_client
        .close_partition_chunk(&db_name, partition_key, table_name, 0)
        .await
        .expect("new partition chunk");

    println!("Operation response is {:?}", operation);

    // wait for the job to be done
    fixture
        .operations_client()
        .wait_operation(operation.id(), Some(std::time::Duration::from_secs(1)))
        .await
        .expect("failed to wait operation");

    let expected_output = r#"
+---------------+----------+-----------------------------+
| partition_key | chunk_id | description                 |
+---------------+----------+-----------------------------+
| cpu           | 0        | Loading chunk to ReadBuffer |
+---------------+----------+-----------------------------+
"#
    .trim();

    let query = format!(
        r#"
select
  partition_key, chunk_id, description
from
  operations
where
  database_name = '{}'
order by
  partition_key, chunk_id, description
"#,
        db_name
    );

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("sql")
        .arg("--host")
        .arg(addr)
        .write_stdin(format!("observer;\n\n{};\n", query))
        .assert()
        .success()
        .stdout(predicate::str::contains(expected_output));
}
