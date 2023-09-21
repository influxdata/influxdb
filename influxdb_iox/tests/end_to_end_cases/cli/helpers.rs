//! Helpful code when testing various CLIs. No tests in this module.

use arrow_util::assert_batches_sorted_eq;
use assert_cmd::Command;
use predicates::{prelude::*, BoxPredicate};
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

#[derive(Debug, Clone)]
pub struct NamespaceCmd {
    subcommand: String,
    namespace_name: String,
    retention_hours: Option<i64>,
    max_tables: Option<i64>,
    max_columns_per_table: Option<i64>,
    partition_template: Option<String>,
}

impl NamespaceCmd {
    /// Build a new call to an `influxdb_iox namespace` command.
    pub fn new(subcommand: impl Into<String>, namespace_name: impl Into<String>) -> Self {
        Self {
            subcommand: subcommand.into(),
            namespace_name: namespace_name.into(),
            retention_hours: None,
            max_tables: None,
            max_columns_per_table: None,
            partition_template: None,
        }
    }

    /// Add the `--retention-hours` argument to this command with the specified value.
    pub fn with_retention_hours(mut self, retention_hours: i64) -> Self {
        self.retention_hours = Some(retention_hours);
        self
    }

    /// Add the `--max-tables` argument to this command with the specified value.
    pub fn with_max_tables(mut self, max_tables: i64) -> Self {
        self.max_tables = Some(max_tables);
        self
    }

    /// Add the `--max-columns-per-table` argument to this command with the specified value.
    pub fn with_max_columns_per_table(mut self, max_columns_per_table: i64) -> Self {
        self.max_columns_per_table = Some(max_columns_per_table);
        self
    }

    /// Add the `--partition-template` argument to this command with the specified value.
    pub fn with_partition_template(mut self, partition_template: impl Into<String>) -> Self {
        self.partition_template = Some(partition_template.into());
        self
    }

    /// Build the `Command` with the specified arguments, but don't run it. Useful for adding
    /// custom assertions such as expecting the command to fail.
    pub fn build_command(self, state: &mut StepTestState<'_>) -> Command {
        let addr = state.cluster().router().router_grpc_base().to_string();

        let mut command = Command::cargo_bin("influxdb_iox").unwrap();
        command
            .arg("-h")
            .arg(&addr)
            .arg("namespace")
            .arg(&self.subcommand);

        if self.subcommand != "list" {
            command.arg(self.namespace_name);
        }

        if let Some(retention_hours) = self.retention_hours {
            command
                .arg("--retention-hours")
                .arg(retention_hours.to_string());
        }

        if let Some(max_tables) = self.max_tables {
            command.arg("--max-tables").arg(max_tables.to_string());
        }

        if let Some(max_columns_per_table) = self.max_columns_per_table {
            command
                .arg("--max-columns-per-table")
                .arg(max_columns_per_table.to_string());
        }

        if let Some(partition_template) = self.partition_template {
            command.arg("--partition-template").arg(partition_template);
        }

        command
    }

    /// Build the predicates to test stdout with in the usual, happy-path case.
    fn build_expected_output(&self) -> BoxPredicate<str> {
        // All commands expect to see the specified namespace in their output.
        let mut expected_output =
            BoxPredicate::new(predicate::str::contains(self.namespace_name.to_string()));

        match self.subcommand.as_str() {
            // List doesn't expect to see anything other than the namespace we're interested in.
            "list" => {}
            // Delete expects to see the literal "Deleted namespace".
            "delete" => {
                expected_output = BoxPredicate::new(
                    expected_output.and(predicate::str::contains("Deleted namespace")),
                );
            }
            // This arm covers the remaining subcommands: "create", "retention", "update-limit"
            _ => {
                match self.retention_hours {
                    // If the retention is set to 0 or not specified, we shouldn't see retention
                    // in the output.
                    Some(0) | None => {
                        expected_output = BoxPredicate::new(
                            expected_output
                                .and(predicate::str::contains("retentionPeriodNs".to_string()))
                                .not(),
                        );
                    }
                    // If nonzero retention is specified, we should see that in the output.
                    Some(retention_hours) => {
                        let retention_period_ns = retention_hours * 60 * 60 * 1_000_000_000;

                        expected_output = BoxPredicate::new(
                            expected_output
                                .and(predicate::str::contains(retention_period_ns.to_string())),
                        );
                    }
                }

                let expected_max_tables = self.max_tables.unwrap_or(500);
                expected_output = BoxPredicate::new(expected_output.and(predicate::str::contains(
                    format!(r#""maxTables": {expected_max_tables}"#),
                )));

                let expected_max_columns_per_table = self.max_columns_per_table.unwrap_or(200);
                expected_output = BoxPredicate::new(expected_output.and(predicate::str::contains(
                    format!(r#""maxColumnsPerTable": {expected_max_columns_per_table}"#),
                )));

                match self.partition_template.clone() {
                    Some(_) => {
                        expected_output = BoxPredicate::new(expected_output.and(
                            predicate::str::contains(r#""partitionTemplate":"#.to_string()),
                        ));
                    }
                    None => {
                        expected_output = BoxPredicate::new(expected_output.and(
                            predicate::str::contains(r#""partitionTemplate":"#.to_string()).not(),
                        ));
                    }
                };
            }
        }

        expected_output
    }

    /// Build and run the command, then assert happy-path output is printed.
    pub fn run(self, state: &mut StepTestState<'_>) {
        let expected_output = self.build_expected_output();

        let mut command = self.build_command(state);

        command.assert().success().stdout(expected_output);
    }
}
