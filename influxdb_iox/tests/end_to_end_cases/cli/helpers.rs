//! Helpful code when testing various CLIs. No tests in this module.

use arrow_util::assert_batches_sorted_eq;
use assert_cmd::Command;
use predicates::prelude::*;
use std::time::{Duration, Instant};
use test_helpers_end_to_end::{MiniCluster, StepTestState};

#[allow(dead_code)] // No tests are actually using the `InfluxQL` variant yet.
#[derive(Clone, Copy)]
pub enum QueryLanguage {
    Sql,
    InfluxQL,
}

impl ToString for QueryLanguage {
    fn to_string(&self) -> String {
        match self {
            Self::Sql => "sql".to_string(),
            Self::InfluxQL => "influxql".to_string(),
        }
    }
}

trait AddQueryLanguage {
    /// Add the query language option to the receiver.
    fn add_query_lang(&mut self, query_lang: Option<QueryLanguage>) -> &mut Self;
}

impl AddQueryLanguage for assert_cmd::Command {
    fn add_query_lang(&mut self, query_lang: Option<QueryLanguage>) -> &mut Self {
        match query_lang {
            Some(lang) => self.arg("--lang").arg(lang.to_string()),
            None => self,
        }
    }
}

pub async fn wait_for_query_result(
    state: &mut StepTestState<'_>,
    query_sql: &str,
    query_lang: Option<QueryLanguage>,
    expected: &str,
) {
    let namespace = state.cluster().namespace().to_owned();
    wait_for_query_result_with_namespace(namespace.as_str(), state, query_sql, query_lang, expected)
        .await
}

/// Runs the specified query in a loop for up to 10 seconds, waiting
/// for the specified output to appear
pub async fn wait_for_query_result_with_namespace(
    namespace: &str,
    state: &mut StepTestState<'_>,
    query_sql: &str,
    query_lang: Option<QueryLanguage>,
    expected: &str,
) {
    let querier_addr = state.cluster().querier().querier_grpc_base().to_string();

    let max_wait_time = Duration::from_secs(10);
    println!("Waiting for {expected}");

    // Validate the output of running the query CLI command appears after at most max_wait_time
    let end = Instant::now() + max_wait_time;
    while Instant::now() < end {
        let assert = Command::cargo_bin("influxdb_iox")
            .unwrap()
            .arg("-h")
            .arg(&querier_addr)
            .arg("query")
            .add_query_lang(query_lang)
            .arg(namespace)
            .arg(query_sql)
            .assert();

        let assert = match assert.try_success() {
            Err(e) => {
                println!("Got err running command: {e}, retrying");
                continue;
            }
            Ok(a) => a,
        };

        match assert.try_stdout(predicate::str::contains(expected)) {
            Err(e) => {
                println!("No match: {e}, retrying");
            }
            Ok(r) => {
                println!("Success: {r:?}");
                return;
            }
        }
        // sleep and try again
        tokio::time::sleep(Duration::from_secs(1)).await
    }
    panic!("Did not find expected output {expected} within {max_wait_time:?}");
}

pub async fn assert_ingester_contains_results(
    cluster: &MiniCluster,
    table_name: &str,
    expected: &[&str],
) {
    use ingester_query_grpc::{influxdata::iox::ingester::v1 as proto, IngesterQueryRequest};
    // query the ingester
    let query = IngesterQueryRequest::new(
        cluster.namespace_id().await,
        cluster.table_id(table_name).await,
        vec![],
        Some(::predicate::EMPTY_PREDICATE),
    );
    let query: proto::IngesterQueryRequest = query.try_into().unwrap();
    let ingester_response = cluster
        .query_ingester(query.clone(), cluster.ingester().ingester_grpc_connection())
        .await
        .unwrap();

    let ingester_partition = ingester_response
        .partitions
        .into_iter()
        .next()
        .expect("at least one ingester partition");
    let ingester_uuid = ingester_partition.app_metadata.ingester_uuid;
    assert!(!ingester_uuid.is_empty());

    assert_batches_sorted_eq!(expected, &ingester_partition.record_batches);
}
