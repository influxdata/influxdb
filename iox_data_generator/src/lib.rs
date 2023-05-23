//! This crate contains structures and generators for specifying how to generate
//! historical and real-time test data for Delorean. The rules for how to
//! generate data and what shape it should take can be specified in a TOML file.
//!
//! Generators can output in line protocol, Parquet, or can be used to generate
//! real-time load on a server that implements the [InfluxDB 2.0 write
//! path][write-api].
//!
//! [write-api]: https://v2.docs.influxdata.com/v2.0/api/#tag/Write
//!
//! While this generator could be compared to [the Go based one that creates TSM
//! data][go-gen], its purpose is meant to be more far reaching. In addition to
//! generating historical data, it should be useful for generating data in a
//! sequence as you would expect it to arrive in a production environment. That
//! means many agents sending data with their different tags and timestamps.
//!
//! [go-gen]: https://github.com/influxdata/influxdb/pull/12710

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]

// Workaround for "unused crate" lint false positives.
use clap as _;
#[cfg(test)]
use criterion as _;
use tracing_subscriber as _;

use crate::{
    agent::{Agent, AgentGenerateStats},
    tag_set::GeneratedTagSets,
};
use snafu::{ResultExt, Snafu};
use std::{
    convert::TryFrom,
    sync::{atomic::AtomicU64, Arc},
    time::{SystemTime, UNIX_EPOCH},
};

pub mod agent;
pub mod field;
pub mod measurement;
pub mod specification;
pub mod substitution;
mod tag_pair;
pub mod tag_set;
pub mod write;

/// Errors that may happen while generating points.
#[derive(Snafu, Debug)]
pub enum Error {
    /// Error that may happen when waiting on a tokio task
    #[snafu(display("Could not join tokio task: {}", source))]
    TokioError {
        /// Underlying tokio error that caused this problem
        source: tokio::task::JoinError,
    },

    /// Error that may happen when constructing an agent name
    #[snafu(display("Could not create agent name, caused by:\n{}", source))]
    CouldNotCreateAgentName {
        /// Underlying `substitution` module error that caused this problem
        source: substitution::Error,
    },

    /// Error that may happen when an agent generates points
    #[snafu(display("Agent could not generate points, caused by:\n{}", source))]
    AgentCouldNotGeneratePoints {
        /// Underlying `agent` module error that caused this problem
        source: agent::Error,
    },

    /// Error that may happen when creating agents
    #[snafu(display("Could not create agents, caused by:\n{}", source))]
    CouldNotCreateAgent {
        /// Underlying `agent` module error that caused this problem
        source: agent::Error,
    },

    /// Error that may happen when constructing an agent's writer
    #[snafu(display("Could not create writer for agent, caused by:\n{}", source))]
    CouldNotCreateAgentWriter {
        /// Underlying `write` module error that caused this problem
        source: write::Error,
    },

    /// Error generating tags sets
    #[snafu(display("Error generating tag sets prior to creating agents: \n{}", source))]
    CouldNotGenerateTagSets {
        /// Underlying `tag_set` module error
        source: tag_set::Error,
    },

    /// Error splitting input buckets to agents that write to them
    #[snafu(display(
        "Error splitting input buckets into agents that write to them: {}",
        source
    ))]
    CouldNotAssignAgents {
        /// Underlying `specification` module error
        source: specification::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Generate data from the configuration in the spec.
///
/// Provide a writer that the line protocol should be written to.
///
/// If `start_datetime` or `end_datetime` are `None`,  the current datetime will
/// be used.
#[allow(clippy::too_many_arguments)]
pub async fn generate(
    spec: &specification::DataSpec,
    databases: Vec<String>,
    points_writer_builder: &mut write::PointsWriterBuilder,
    start_datetime: Option<i64>,
    end_datetime: Option<i64>,
    execution_start_time: i64,
    continue_on: bool,
    batch_size: usize,
    one_agent_at_a_time: bool, // run one agent after another, if printing to stdout
) -> Result<usize> {
    let mut handles = vec![];

    let database_agents = spec
        .database_split_to_agents(&databases)
        .context(CouldNotAssignAgentsSnafu)?;

    let generated_tag_sets =
        GeneratedTagSets::from_spec(spec).context(CouldNotGenerateTagSetsSnafu)?;

    let lock = Arc::new(tokio::sync::Mutex::new(()));

    let start = std::time::Instant::now();
    let total_rows = Arc::new(AtomicU64::new(0));
    let total_requests = Arc::new(AtomicU64::new(0));

    for database_assignments in &database_agents {
        let (org, bucket) = org_and_bucket_from_database(database_assignments.database);

        for agent_assignment in database_assignments.agent_assignments.iter() {
            let agents = Agent::from_spec(
                agent_assignment.spec,
                agent_assignment.count,
                agent_assignment.sampling_interval,
                start_datetime,
                end_datetime,
                execution_start_time,
                continue_on,
                &generated_tag_sets,
            )
            .context(CouldNotCreateAgentSnafu)?;

            println!(
                "Configuring {} agents of \"{}\" to write data \
                to org {} and bucket {} (database {})",
                agent_assignment.count,
                agent_assignment.spec.name,
                org,
                bucket,
                database_assignments.database,
            );

            let agent_points_writer = Arc::new(
                points_writer_builder
                    .build_for_agent(&agent_assignment.spec.name, org, bucket)
                    .context(CouldNotCreateAgentWriterSnafu)?,
            );

            for mut agent in agents.into_iter() {
                let lock_ref = Arc::clone(&lock);
                let agent_points_writer = Arc::clone(&agent_points_writer);

                let total_rows = Arc::clone(&total_rows);
                let total_requests = Arc::clone(&total_requests);
                handles.push(tokio::task::spawn(async move {
                    // did this weird hack because otherwise the stdout outputs would be jumbled
                    // together garbage
                    if one_agent_at_a_time {
                        let _l = lock_ref.lock().await;
                        agent
                            .generate_all(
                                agent_points_writer,
                                batch_size,
                                total_rows,
                                total_requests,
                            )
                            .await
                    } else {
                        agent
                            .generate_all(
                                agent_points_writer,
                                batch_size,
                                total_rows,
                                total_requests,
                            )
                            .await
                    }
                }));
            }
        }
    }

    let mut stats = vec![];
    for handle in handles {
        stats.push(
            handle
                .await
                .context(TokioSnafu)?
                .context(AgentCouldNotGeneratePointsSnafu)?,
        );
    }
    let stats = stats
        .into_iter()
        .fold(AgentGenerateStats::default(), |totals, res| {
            AgentGenerateStats {
                request_count: totals.request_count + res.request_count,
                error_count: totals.error_count + res.error_count,
                row_count: totals.row_count + res.row_count,
            }
        });

    println!("{}", stats.display_stats(start.elapsed()));

    Ok(stats.row_count)
}

/// Gets the current time in nanoseconds since the epoch
pub fn now_ns() -> i64 {
    let since_the_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    i64::try_from(since_the_epoch.as_nanos()).expect("Time does not fit")
}

fn org_and_bucket_from_database(database: &str) -> (&str, &str) {
    let parts = database.split('_').collect::<Vec<_>>();
    if parts.len() != 2 {
        panic!("error parsing org and bucket from {database}");
    }

    (parts[0], parts[1])
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::specification::*;
    use influxdb2_client::models::WriteDataPoint;
    use std::str::FromStr;
    use std::time::Duration;

    type Error = Box<dyn std::error::Error>;
    type Result<T = (), E = Error> = std::result::Result<T, E>;

    #[tokio::test]
    async fn historical_data_sampling_interval() -> Result<()> {
        let toml = r#"
name = "demo_schema"

[[agents]]
name = "foo"

[[agents.measurements]]
name = "cpu"

[[agents.measurements.fields]]
name = "val"
i64_range = [1, 1]

[[database_writers]]
agents = [{name = "foo", sampling_interval = "10s"}]
"#;
        let data_spec = DataSpec::from_str(toml).unwrap();
        let agent_spec = &data_spec.agents[0];

        let execution_start_time = now_ns();

        // imagine we've specified at the command line that we want to generate metrics
        // for 1970
        let start_datetime = Some(0);
        // for the first 15 seconds of the year
        let end_datetime = Some(15 * 1_000_000_000);

        let generated_tag_sets = GeneratedTagSets::default();

        let mut agent = agent::Agent::from_spec(
            agent_spec,
            1,
            Duration::from_secs(10),
            start_datetime,
            end_datetime,
            execution_start_time,
            false,
            &generated_tag_sets,
        )?;

        let data_points = agent[0].generate().await?.into_iter().flatten();
        let mut v = Vec::new();
        for data_point in data_points {
            data_point.write_data_point_to(&mut v).unwrap();
        }
        let line_protocol = String::from_utf8(v).unwrap();

        // Get a point for time 0
        let expected_line_protocol = "cpu val=1i 0\n";
        assert_eq!(line_protocol, expected_line_protocol);

        let data_points = agent[0].generate().await?.into_iter().flatten();
        let mut v = Vec::new();
        for data_point in data_points {
            data_point.write_data_point_to(&mut v).unwrap();
        }
        let line_protocol = String::from_utf8(v).unwrap();

        // Get a point for time 10s
        let expected_line_protocol = "cpu val=1i 10000000000\n";
        assert_eq!(line_protocol, expected_line_protocol);

        // Don't get any points anymore because we're past the ending datetime
        let data_points = agent[0].generate().await?.into_iter().flatten();
        let data_points: Vec<_> = data_points.collect();
        assert!(
            data_points.is_empty(),
            "expected no data points, got {data_points:?}"
        );

        Ok(())
    }
}
