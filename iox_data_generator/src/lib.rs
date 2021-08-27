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

#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

use crate::substitution::Substitute;
use rand::Rng;
use rand_seeder::Seeder;
use snafu::{ResultExt, Snafu};
use std::{
    convert::TryFrom,
    time::{SystemTime, UNIX_EPOCH},
};

pub mod agent;
pub mod field;
pub mod measurement;
pub mod specification;
pub mod substitution;
pub mod tag;
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
    #[snafu(display("Could not create agent `{}`, caused by:\n{}", name, source))]
    CouldNotCreateAgent {
        /// The name of the relevant agent
        name: String,
        /// Underlying `agent` module error that caused this problem
        source: agent::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Generate data from the configuration in the spec.
///
/// Provide a writer that the line protocol should be written to.
///
/// If `start_datetime` or `end_datetime` are `None`,  the current datetime will
/// be used.
pub async fn generate<T: DataGenRng>(
    spec: &specification::DataSpec,
    points_writer_builder: &mut write::PointsWriterBuilder,
    start_datetime: Option<i64>,
    end_datetime: Option<i64>,
    execution_start_time: i64,
    continue_on: bool,
    batch_size: usize,
) -> Result<usize> {
    let seed = spec.base_seed.to_owned().unwrap_or_else(|| {
        let mut rng = rand::thread_rng();
        format!("{:04}", rng.gen_range(0..10000))
    });

    let mut handles = vec![];

    // for each agent specification
    for agent_spec in &spec.agents {
        // create iterators to `cycle` through for `agent_spec.tags`
        let tag_set_iterator = tag::AgentTagIterator::new(&agent_spec.tags);

        // create `count` number of agent instances, or 1 agent if no count is specified
        let n_agents = agent_spec.count.unwrap_or(1);

        for (agent_id, mut agent_tags) in tag_set_iterator.take(n_agents).enumerate() {
            let agent_name =
                Substitute::once(&agent_spec.name, &[("agent_id", &agent_id.to_string())])
                    .context(CouldNotCreateAgentName)?;

            agent_tags.push(tag::Tag::new("data_spec", &spec.name));

            if let Some(name_tag_key) = &agent_spec.name_tag_key {
                agent_tags.push(tag::Tag::new(name_tag_key, &agent_name));
            }

            let mut agent = agent::Agent::<T>::new(
                agent_spec,
                &agent_name,
                agent_id,
                &seed,
                agent_tags,
                start_datetime,
                end_datetime,
                execution_start_time,
                continue_on,
            )
            .context(CouldNotCreateAgent { name: &agent_name })?;

            let agent_points_writer = points_writer_builder.build_for_agent(&agent_name);

            handles.push(tokio::task::spawn(async move {
                agent.generate_all(agent_points_writer, batch_size).await
            }));
        }
    }

    let mut total_points = 0;
    for handle in handles {
        total_points += handle
            .await
            .context(TokioError)?
            .context(AgentCouldNotGeneratePoints)?;
    }

    Ok(total_points)
}

/// Shorthand trait for the functionality this crate needs a random number generator to have
pub trait DataGenRng: rand::Rng + rand::SeedableRng + Send + 'static {}

impl<T: rand::Rng + rand::SeedableRng + Send + 'static> DataGenRng for T {}

/// Encapsulating the creation of an optionally-seedable random number generator
/// to make this easy to change. Uses a 4-digit number expressed as a `String`
/// as the seed type to enable easy creation of another instance using the same
/// seed.
#[derive(Debug)]
pub struct RandomNumberGenerator<T: DataGenRng> {
    rng: T,
    /// The seed used for this instance.
    pub seed: String,
}

impl<T: DataGenRng> Default for RandomNumberGenerator<T> {
    fn default() -> Self {
        let mut rng = rand::thread_rng();
        let seed = format!("{:04}", rng.gen_range(0..10000));
        Self::new(seed)
    }
}

impl<T: DataGenRng> RandomNumberGenerator<T> {
    /// Create a new instance using the specified seed.
    pub fn new(seed: impl Into<String>) -> Self {
        let seed = seed.into();
        Self {
            rng: Seeder::from(&seed).make_rng(),
            seed,
        }
    }

    /// Generate a random GUID
    pub fn guid(&mut self) -> uuid::Uuid {
        let mut bytes = [0u8; 16];
        self.rng.fill_bytes(&mut bytes);
        uuid::Builder::from_bytes(bytes)
            .set_variant(uuid::Variant::RFC4122)
            .set_version(uuid::Version::Random)
            .build()
    }
}

impl<T: DataGenRng> rand::RngCore for RandomNumberGenerator<T> {
    fn next_u32(&mut self) -> u32 {
        self.rng.next_u32()
    }

    fn next_u64(&mut self) -> u64 {
        self.rng.next_u64()
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        self.rng.fill_bytes(dest);
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> std::result::Result<(), rand::Error> {
        self.rng.try_fill_bytes(dest)
    }
}

/// Gets the current time in nanoseconds since the epoch
pub fn now_ns() -> i64 {
    let since_the_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    i64::try_from(since_the_epoch.as_nanos()).expect("Time does not fit")
}

// Always returns 0.
#[cfg(test)]
#[derive(Default)]
struct ZeroRng;

#[cfg(test)]
impl rand::RngCore for ZeroRng {
    fn next_u32(&mut self) -> u32 {
        self.next_u64() as u32
    }

    fn next_u64(&mut self) -> u64 {
        0
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        rand_core::impls::fill_bytes_via_next(self, dest)
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> std::result::Result<(), rand::Error> {
        self.fill_bytes(dest);
        Ok(())
    }
}

#[cfg(test)]
impl rand::SeedableRng for ZeroRng {
    type Seed = Vec<u8>;

    // Ignore the seed value
    fn from_seed(_seed: Self::Seed) -> Self {
        Self
    }
}

// The test rng ignores the seed anyway, so the seed specified doesn't matter.
#[cfg(test)]
const TEST_SEED: &str = "";

#[cfg(test)]
fn test_rng() -> RandomNumberGenerator<ZeroRng> {
    RandomNumberGenerator::<ZeroRng>::new(TEST_SEED)
}

// A random number type that does *not* have a predictable sequence of values for use in tests
// that assert on properties rather than exact values. Aliased for convenience in changing to
// a different Rng type.
#[cfg(test)]
type DynamicRng = rand::rngs::SmallRng;

#[cfg(test)]
mod test {
    use super::*;
    use crate::specification::*;
    use influxdb2_client::models::WriteDataPoint;
    use std::str::FromStr;

    type Error = Box<dyn std::error::Error>;
    type Result<T = (), E = Error> = std::result::Result<T, E>;

    #[tokio::test]
    async fn historical_data_sampling_interval() -> Result<()> {
        let toml = r#"
name = "demo_schema"

[[agents]]
name = "basic"
sampling_interval = "10s" # seconds

[[agents.measurements]]
name = "cpu"

[[agents.measurements.fields]]
name = "up"
bool = true"#;
        let data_spec = DataSpec::from_str(toml).unwrap();
        let agent_id = 0;
        let agent_spec = &data_spec.agents[0];
        // Take agent_tags out of the equation for the purposes of this test
        let agent_tags = vec![];

        let execution_start_time = now_ns();

        // imagine we've specified at the command line that we want to generate metrics
        // for 1970
        let start_datetime = Some(0);
        // for the first 15 seconds of the year
        let end_datetime = Some(15 * 1_000_000_000);

        let mut agent = agent::Agent::<ZeroRng>::new(
            agent_spec,
            &agent_spec.name,
            agent_id,
            TEST_SEED,
            agent_tags,
            start_datetime,
            end_datetime,
            execution_start_time,
            false,
        )?;

        let data_points = agent.generate().await?;
        let mut v = Vec::new();
        for data_point in data_points {
            data_point.write_data_point_to(&mut v).unwrap();
        }
        let line_protocol = String::from_utf8(v).unwrap();

        // Get a point for time 0
        let expected_line_protocol = "cpu up=f 0\n";
        assert_eq!(line_protocol, expected_line_protocol);

        let data_points = agent.generate().await?;
        let mut v = Vec::new();
        for data_point in data_points {
            data_point.write_data_point_to(&mut v).unwrap();
        }
        let line_protocol = String::from_utf8(v).unwrap();

        // Get a point for time 10s
        let expected_line_protocol = "cpu up=f 10000000000\n";
        assert_eq!(line_protocol, expected_line_protocol);

        // Don't get any points anymore because we're past the ending datetime
        let data_points = agent.generate().await?;
        assert!(
            data_points.is_empty(),
            "expected no data points, got {:?}",
            data_points
        );

        Ok(())
    }
}
