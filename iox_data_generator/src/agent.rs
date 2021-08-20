//! Agents responsible for generating points

use crate::{
    measurement::MeasurementGeneratorSet, now_ns, specification, tag::Tag, write::PointsWriter,
    DataGenRng, RandomNumberGenerator,
};

use influxdb2_client::models::DataPoint;
use snafu::{ResultExt, Snafu};
use std::{fmt, time::Duration};
use tracing::{debug, info};

/// Agent-specific Results
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors that may happen while creating points
#[derive(Snafu, Debug)]
pub enum Error {
    /// Error that may happen when generating points from measurements
    #[snafu(display("{}", source))]
    CouldNotGeneratePoint {
        /// Underlying `measurement` module error that caused this problem
        source: crate::measurement::Error,
    },

    /// Error that may happen when creating measurement generator sets
    #[snafu(display("Could not create measurement generator sets, caused by:\n{}", source))]
    CouldNotCreateMeasurementGeneratorSets {
        /// Underlying `measurement` module error that caused this problem
        source: crate::measurement::Error,
    },

    /// Error that may happen when writing points
    #[snafu(display("Could not write points, caused by:\n{}", source))]
    CouldNotWritePoints {
        /// Underlying `write` module error that caused this problem
        source: crate::write::Error,
    },
}

/// Each `AgentSpec` informs the instantiation of an `Agent`, which coordinates
/// the generation of the measurements in their specification.
#[derive(Debug)]
pub struct Agent<T: DataGenRng> {
    agent_id: usize,
    name: String,
    #[allow(dead_code)]
    rng: RandomNumberGenerator<T>,
    agent_tags: Vec<Tag>,
    measurement_generator_sets: Vec<MeasurementGeneratorSet<T>>,
    sampling_interval: Option<i64>,
    /// nanoseconds since the epoch, used as the timestamp for the next
    /// generated point
    current_datetime: i64,
    /// nanoseconds since the epoch, when current_datetime exceeds this, stop
    /// generating points
    end_datetime: i64,
    /// whether to continue generating points after reaching the current time
    continue_on: bool,
    /// whether this agent is done generating points or not
    finished: bool,
    /// Optional interval at which to re-run the agent if generating data in
    /// "continue" mode
    interval: Option<tokio::time::Interval>,
}

impl<T: DataGenRng> Agent<T> {
    /// Create a new agent that will generate data points according to these
    /// specs. Substitutions in `name` and `agent_tags` should be made
    /// before using them to instantiate an agent.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        agent_spec: &specification::AgentSpec,
        agent_name: impl Into<String>,
        agent_id: usize,
        parent_seed: impl fmt::Display,
        agent_tags: Vec<Tag>,
        start_datetime: Option<i64>, // in nanoseconds since the epoch, defaults to now
        end_datetime: Option<i64>,   // also in nanoseconds since the epoch, defaults to now
        execution_start_time: i64,
        continue_on: bool, // If true, run in "continue" mode after historical data is generated
    ) -> Result<Self> {
        let name = agent_name.into();
        // Will agents actually need rngs? Might just need seeds...
        let seed = format!("{}-{}", parent_seed, name);
        let rng = RandomNumberGenerator::<T>::new(&seed);

        let measurement_generator_sets = agent_spec
            .measurements
            .iter()
            .map(|spec| {
                MeasurementGeneratorSet::new(
                    &name,
                    agent_id,
                    spec,
                    &seed,
                    &agent_tags,
                    execution_start_time,
                )
            })
            .collect::<crate::measurement::Result<_>>()
            .context(CouldNotCreateMeasurementGeneratorSets)?;

        let current_datetime = start_datetime.unwrap_or_else(now_ns);
        let end_datetime = end_datetime.unwrap_or_else(now_ns);

        // Convert to nanoseconds
        let sampling_interval = agent_spec
            .sampling_interval
            .map(|s| s as i64 * 1_000_000_000);

        Ok(Self {
            agent_id,
            name,
            rng,
            agent_tags,
            measurement_generator_sets,
            sampling_interval,
            current_datetime,
            end_datetime,
            continue_on,
            finished: false,
            interval: None,
        })
    }

    /// Generate and write points in batches until `generate` doesn't return any
    /// points. Points will be written to the writer in batches where `generate` is
    /// called `batch_size` times before writing. Meant to be called in a `tokio::task`.
    pub async fn generate_all(
        &mut self,
        mut points_writer: PointsWriter,
        batch_size: usize,
    ) -> Result<usize> {
        let mut total_points = 0;

        let mut points = self.generate().await?;
        while !points.is_empty() {
            for _ in 0..batch_size {
                points.append(&mut self.generate().await?);
            }
            info!("[agent {}] sending {} points", self.name, points.len());
            total_points += points.len();
            points_writer
                .write_points(points)
                .await
                .context(CouldNotWritePoints)?;
            points = self.generate().await?;
        }
        Ok(total_points)
    }

    /// Generate data points from the configuration in this agent, one point per
    /// measurement contained in this agent's configuration.
    pub async fn generate(&mut self) -> Result<Vec<DataPoint>> {
        let mut points = Vec::new();

        debug!(
            "[agent {}]  generate more? {} current: {}, end: {}",
            self.name, self.finished, self.current_datetime, self.end_datetime
        );

        if !self.finished {
            // Save the current_datetime to use in the set of points that we're generating
            // because we might increment current_datetime to see if we're done
            // or not.
            let point_timestamp = self.current_datetime;

            if let Some(i) = &mut self.interval {
                i.tick().await;
                self.current_datetime = now_ns();
            } else if let Some(ns) = self.sampling_interval {
                self.current_datetime += ns;

                if self.current_datetime > self.end_datetime {
                    if self.continue_on {
                        let mut i = tokio::time::interval(Duration::from_nanos(ns as u64));
                        i.tick().await; // first tick completes immediately
                        self.current_datetime = now_ns();
                        self.interval = Some(i);
                    } else {
                        self.finished = true;
                    }
                }
            } else {
                self.finished = true;
            }

            for mgs in &mut self.measurement_generator_sets {
                for point in mgs
                    .generate(point_timestamp)
                    .context(CouldNotGeneratePoint)?
                {
                    points.push(point);
                }
            }
        }

        Ok(points)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{now_ns, specification::*, ZeroRng};
    use influxdb2_client::models::WriteDataPoint;

    type Error = Box<dyn std::error::Error>;
    type Result<T = (), E = Error> = std::result::Result<T, E>;

    impl<T: DataGenRng> Agent<T> {
        /// Instantiate an agent only with the parameters we're interested in
        /// testing, keeping everything else constant across different
        /// tests.
        fn test_instance(
            sampling_interval: Option<i64>,
            continue_on: bool,
            current_datetime: i64,
            end_datetime: i64,
        ) -> Self {
            let measurement_spec = MeasurementSpec {
                name: "measurement-{{agent_id}}-{{measurement_id}}".into(),
                count: Some(2),
                tags: vec![],
                fields: vec![FieldSpec {
                    name: "field-{{agent_id}}-{{measurement_id}}-{{field_id}}".into(),
                    field_value_spec: FieldValueSpec::I64 {
                        range: 0..60,
                        increment: false,
                        reset_after: None,
                    },
                    count: Some(2),
                }],
            };

            let measurement_generator_set =
                MeasurementGeneratorSet::new("test", 42, &measurement_spec, "spec-test", &[], 0)
                    .unwrap();

            Self {
                agent_id: 0,
                name: String::from("test"),
                rng: RandomNumberGenerator::<T>::new("spec-test"),
                agent_tags: vec![],
                measurement_generator_sets: vec![measurement_generator_set],
                finished: false,
                interval: None,

                sampling_interval,
                current_datetime,
                end_datetime,
                continue_on,
            }
        }
    }

    fn timestamps(points: &[influxdb2_client::models::DataPoint]) -> Result<Vec<i64>> {
        points
            .iter()
            .map(|point| {
                let mut v = Vec::new();
                point.write_data_point_to(&mut v)?;
                let line = String::from_utf8(v)?;

                Ok(line.split(' ').last().unwrap().trim().parse()?)
            })
            .collect()
    }

    #[rustfmt::skip]
    // # Summary: No Sampling Interval
    //
    // If there isn't a sampling interval, we don't know how often to run, so we can neither
    // generate historical data nor can we continue into the future. The only thing we'll do is
    // generate once then stop.
    //
    // | sampling_interval | continue | cmp(current_time, end_time) | expected outcome |
    // |-------------------+----------+-----------------------------+------------------|
    // | None              | false    | Less                        | gen 1x, stop     |
    // | None              | false    | Equal                       | gen 1x, stop     |
    // | None              | false    | Greater                     | gen 1x, stop     |
    // | None              | true     | Less                        | gen 1x, stop     |
    // | None              | true     | Equal                       | gen 1x, stop     |
    // | None              | true     | Greater                     | gen 1x, stop     |

    mod without_sampling_interval {
        use super::*;

        mod without_continue {
            use super::*;

            #[tokio::test]
            async fn current_time_less_than_end_time() -> Result<()> {
                let mut agent = Agent::<ZeroRng>::test_instance(None, false, 0, 10);

                let points = agent.generate().await?;
                assert_eq!(points.len(), 2);

                let points = agent.generate().await?;
                assert!(points.is_empty(), "expected no points, got {:?}", points);

                Ok(())
            }

            #[tokio::test]
            async fn current_time_equal_end_time() -> Result<()> {
                let mut agent = Agent::<ZeroRng>::test_instance(None, false, 10, 10);

                let points = agent.generate().await?;
                assert_eq!(points.len(), 2);

                let points = agent.generate().await?;
                assert!(points.is_empty(), "expected no points, got {:?}", points);

                Ok(())
            }

            #[tokio::test]
            async fn current_time_greater_than_end_time() -> Result<()> {
                let mut agent = Agent::<ZeroRng>::test_instance(None, false, 11, 10);

                let points = agent.generate().await?;
                assert_eq!(points.len(), 2);

                let points = agent.generate().await?;
                assert!(points.is_empty(), "expected no points, got {:?}", points);

                Ok(())
            }
        }

        mod with_continue {
            use super::*;

            #[tokio::test]
            async fn current_time_less_than_end_time() -> Result<()> {
                let mut agent = Agent::<ZeroRng>::test_instance(None, true, 0, 10);

                let points = agent.generate().await?;
                assert_eq!(points.len(), 2);

                let points = agent.generate().await?;
                assert!(points.is_empty(), "expected no points, got {:?}", points);

                Ok(())
            }

            #[tokio::test]
            async fn current_time_equal_end_time() -> Result<()> {
                let mut agent = Agent::<ZeroRng>::test_instance(None, true, 10, 10);

                let points = agent.generate().await?;
                assert_eq!(points.len(), 2);

                let points = agent.generate().await?;
                assert!(points.is_empty(), "expected no points, got {:?}", points);

                Ok(())
            }

            #[tokio::test]
            async fn current_time_greater_than_end_time() -> Result<()> {
                let mut agent = Agent::<ZeroRng>::test_instance(None, true, 11, 10);

                let points = agent.generate().await?;
                assert_eq!(points.len(), 2);

                let points = agent.generate().await?;
                assert!(points.is_empty(), "expected no points, got {:?}", points);

                Ok(())
            }
        }
    }

    mod with_sampling_interval {
        use super::*;

        // The tests take about 5 ms to run on my computer, so set the sampling interval
        // to 10 ms to be able to test that the delay is happening when
        // `continue` is true without making the tests too artificially slow.
        const TEST_SAMPLING_INTERVAL: i64 = 10_000_000;

        #[rustfmt::skip]
        // # Summary: Not continuing
        //
        // If there is a sampling interval but we're not continuing, we should generate points at
        // least once but if the current time is greater than the ending time (which might be set
        // to `now`), we've generated everything we need to and should stop.
        //
        // | sampling_interval | continue | cmp(current_time, end_time) | expected outcome |
        // |-------------------+----------+-----------------------------+------------------|
        // | Some(_)           | false    | Less                        | gen & increment  |
        // | Some(_)           | false    | Equal                       | gen 1x, stop     |
        // | Some(_)           | false    | Greater                     | gen 1x, stop     |

        mod without_continue {
            use super::*;

            #[tokio::test]
            async fn current_time_less_than_end_time() -> Result<()> {
                let current = 0;
                let end = TEST_SAMPLING_INTERVAL;

                let mut agent =
                    Agent::<ZeroRng>::test_instance(Some(TEST_SAMPLING_INTERVAL), false, current, end);

                let points = agent.generate().await?;
                assert_eq!(points.len(), 2);

                let points = agent.generate().await?;
                assert_eq!(points.len(), 2);

                let points = agent.generate().await?;
                assert!(points.is_empty(), "expected no points, got {:?}", points);

                Ok(())
            }

            #[tokio::test]
            async fn current_time_equal_end_time() -> Result<()> {
                let current = TEST_SAMPLING_INTERVAL;
                let end = current;

                let mut agent =
                    Agent::<ZeroRng>::test_instance(Some(TEST_SAMPLING_INTERVAL), false, current, end);

                let points = agent.generate().await?;
                assert_eq!(points.len(), 2);

                let points = agent.generate().await?;
                assert!(points.is_empty(), "expected no points, got {:?}", points);

                Ok(())
            }

            #[tokio::test]
            async fn current_time_greater_than_end_time() -> Result<()> {
                let current = 2 * TEST_SAMPLING_INTERVAL;
                let end = TEST_SAMPLING_INTERVAL;

                let mut agent =
                    Agent::<ZeroRng>::test_instance(Some(TEST_SAMPLING_INTERVAL), false, current, end);

                let points = agent.generate().await?;
                assert_eq!(points.len(), 2);

                let points = agent.generate().await?;
                assert!(points.is_empty(), "expected no points, got {:?}", points);

                Ok(())
            }
        }

        #[rustfmt::skip]
        // # Summary: After generating historical data, continue sampling in "real time"
        //
        // If there is a sampling interval and we are continuing, generate points as fast as
        // possible (but with timestamps separated by sampling_interval amounts) until we catch up
        // to `now`. Then add pauses of the sampling_interval's duration, generating points with
        // their timestamps set to the current time to simulate "real" point generation.
        //
        // | sampling_interval | continue | cmp(current_time, end_time) | expected outcome |
        // |-------------------+----------+-----------------------------+------------------|
        // | Some(_)           | true     | Less                        | gen, no delay    |
        // | Some(_)           | true     | Equal                       | gen, delay       |
        // | Some(_)           | true     | Greater                     | gen, delay       |

        mod with_continue {
            use super::*;

            #[tokio::test]
            async fn current_time_less_than_end_time() -> Result<()> {
                let end = now_ns();
                let current = end - TEST_SAMPLING_INTERVAL;

                let mut agent =
                    Agent::<ZeroRng>::test_instance(Some(TEST_SAMPLING_INTERVAL), true, current, end);

                let points = agent.generate().await?;
                assert_eq!(points.len(), 2);

                let times = timestamps(&points).unwrap();
                assert_eq!(vec![current, current], times);

                let points = agent.generate().await?;
                assert_eq!(points.len(), 2);

                let times = timestamps(&points).unwrap();
                assert_eq!(vec![end, end], times);

                Ok(())
            }

            #[tokio::test]
            async fn current_time_equal_end_time() -> Result<()> {
                let end = now_ns();
                let current = end;

                let mut agent =
                    Agent::<ZeroRng>::test_instance(Some(TEST_SAMPLING_INTERVAL), true, current, end);

                let points = agent.generate().await?;
                assert_eq!(points.len(), 2);

                let times = timestamps(&points).unwrap();
                assert_eq!(vec![end, end], times);

                let points = agent.generate().await?;
                assert_eq!(points.len(), 2);

                let real_now = now_ns();

                let times = timestamps(&points).unwrap();
                for time in times {
                    assert!(
                        time <= real_now,
                        "expected timestamp {} to be generated before now ({}); \
                        was {} nanoseconds greater",
                        time,
                        real_now,
                        time - real_now
                    );
                }

                Ok(())
            }

            #[tokio::test]
            async fn current_time_greater_than_end_time() -> Result<()> {
                let end = now_ns();
                let current = end + TEST_SAMPLING_INTERVAL;

                let mut agent =
                    Agent::<ZeroRng>::test_instance(Some(TEST_SAMPLING_INTERVAL), true, current, end);

                let points = agent.generate().await?;
                assert_eq!(points.len(), 2);

                let times = timestamps(&points).unwrap();
                assert_eq!(vec![current, current], times);

                let points = agent.generate().await?;
                assert_eq!(points.len(), 2);

                let real_now = now_ns();

                let times = timestamps(&points).unwrap();
                for time in times {
                    assert!(
                        time <= real_now,
                        "expected timestamp {} to be generated before now ({}); \
                        was {} nanoseconds greater",
                        time,
                        real_now,
                        time - real_now
                    );
                }

                Ok(())
            }
        }
    }
}
