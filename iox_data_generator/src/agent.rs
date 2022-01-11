//! Agents responsible for generating points

use crate::{
    measurement::{MeasurementGenerator, MeasurementLineIterator},
    now_ns, specification,
    tag_pair::TagPair,
    write::PointsWriter,
};

use crate::tag_set::GeneratedTagSets;
use serde_json::json;
use snafu::{ResultExt, Snafu};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tracing::{debug, info};

/// Agent-specific Results
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors that may happen while creating points
#[derive(Snafu, Debug)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("{}", source))]
    CouldNotGeneratePoint {
        /// Underlying `measurement` module error that caused this problem
        source: crate::measurement::Error,
    },

    #[snafu(display("Could not create measurement generators, caused by:\n{}", source))]
    CouldNotCreateMeasurementGenerators {
        /// Underlying `measurement` module error that caused this problem
        source: crate::measurement::Error,
    },

    #[snafu(display("Could not write points, caused by:\n{}", source))]
    CouldNotWritePoints {
        /// Underlying `write` module error that caused this problem
        source: crate::write::Error,
    },

    #[snafu(display("Error creating agent tag pairs: {}", source))]
    CouldNotCreateAgentTagPairs { source: crate::tag_pair::Error },
}

/// Each `AgentSpec` informs the instantiation of an `Agent`, which coordinates
/// the generation of the measurements in their specification.
#[derive(Debug)]
pub struct Agent {
    /// identifier for the agent. This can be used in generated tags and fields
    pub id: usize,
    /// name for the agent. This can be used in generated tags and fields
    pub name: String,
    measurement_generators: Vec<MeasurementGenerator>,
    sampling_interval: Option<Duration>,
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

impl Agent {
    /// Create agents that will generate data points according to these
    /// specs.
    #[allow(clippy::too_many_arguments)]
    pub fn from_spec(
        agent_spec: &specification::AgentSpec,
        count: usize,
        sampling_interval: Duration,
        start_datetime: Option<i64>, // in nanoseconds since the epoch, defaults to now
        end_datetime: Option<i64>,   // also in nanoseconds since the epoch, defaults to now
        execution_start_time: i64,
        continue_on: bool, // If true, run in "continue" mode after historical data is generated
        generated_tag_sets: &GeneratedTagSets,
    ) -> Result<Vec<Self>> {
        let agents: Vec<_> = (1..count + 1)
            .into_iter()
            .map(|agent_id| {
                let data = json!({"agent": {"id": agent_id, "name": agent_spec.name}});

                let agent_tag_pairs = TagPair::pairs_from_specs(&agent_spec.tag_pairs, data)
                    .context(CouldNotCreateAgentTagPairsSnafu)?;

                let measurement_generators = agent_spec
                    .measurements
                    .iter()
                    .map(|spec| {
                        MeasurementGenerator::from_spec(
                            agent_id,
                            spec,
                            execution_start_time,
                            generated_tag_sets,
                            &agent_tag_pairs,
                        )
                        .context(CouldNotCreateMeasurementGeneratorsSnafu)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let measurement_generators = measurement_generators.into_iter().flatten().collect();

                let current_datetime = start_datetime.unwrap_or_else(now_ns);
                let end_datetime = end_datetime.unwrap_or_else(now_ns);

                Ok(Self {
                    id: agent_id,
                    name: agent_spec.name.to_string(),
                    measurement_generators,
                    sampling_interval: Some(sampling_interval),
                    current_datetime,
                    end_datetime,
                    continue_on,
                    finished: false,
                    interval: None,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(agents)
    }

    /// Generate and write points in batches until `generate` doesn't return any
    /// points. Points will be written to the writer in batches where `generate` is
    /// called `batch_size` times before writing. Meant to be called in a `tokio::task`.
    pub async fn generate_all(
        &mut self,
        mut points_writer: PointsWriter,
        batch_size: usize,
        counter: Arc<AtomicU64>,
    ) -> Result<usize> {
        let mut points_this_batch = 1;
        let mut total_points = 0;
        let start = Instant::now();

        while points_this_batch != 0 {
            let batch_start = Instant::now();
            points_this_batch = 0;

            let mut streams = Vec::with_capacity(batch_size);
            for _ in 0..batch_size {
                let mut s = self.generate().await?;
                if s.is_empty() {
                    break;
                }
                streams.append(&mut s);
            }

            for s in &streams {
                points_this_batch += s.line_count();
                total_points += s.line_count();
            }

            points_writer
                .write_points(streams.into_iter().flatten())
                .await
                .context(CouldNotWritePointsSnafu)?;

            info!("wrote {} in {:?}", points_this_batch, batch_start.elapsed());
            let total = counter.fetch_add(points_this_batch as u64, Ordering::SeqCst);
            let secs = start.elapsed().as_secs();
            if secs != 0 {
                info!(
                    "Agent {} written {} in {:?} for {}/sec. Aggregate {} in {}/sec",
                    self.id,
                    total_points,
                    start.elapsed(),
                    total_points / secs as usize,
                    total,
                    total / secs,
                )
            }
        }

        Ok(total_points)
    }

    /// Generate data points from the configuration in this agent.
    pub async fn generate(&mut self) -> Result<Vec<MeasurementLineIterator>> {
        debug!(
            "[agent {}]  generate more? {} current: {}, end: {}",
            self.id, self.finished, self.current_datetime, self.end_datetime
        );

        if !self.finished {
            let mut measurement_streams = Vec::with_capacity(self.measurement_generators.len());

            // Save the current_datetime to use in the set of points that we're generating
            // because we might increment current_datetime to see if we're done
            // or not.
            let point_timestamp = self.current_datetime;

            if let Some(i) = &mut self.interval {
                i.tick().await;
                self.current_datetime = now_ns();
            } else if let Some(sampling_interval) = self.sampling_interval {
                self.current_datetime += sampling_interval.as_nanos() as i64;

                if self.current_datetime > self.end_datetime {
                    if self.continue_on {
                        let mut i = tokio::time::interval(sampling_interval);
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

            for mgs in &mut self.measurement_generators {
                measurement_streams.push(
                    mgs.generate(point_timestamp)
                        .context(CouldNotGeneratePointSnafu)?,
                );
            }

            Ok(measurement_streams)
        } else {
            Ok(Vec::new())
        }
    }

    /// Sets the current date and time for the agent and resets its finished state to false. Enables
    /// calling generate again during testing and benchmarking.
    pub fn reset_current_date_time(&mut self, current_datetime: i64) {
        self.finished = false;
        self.current_datetime = current_datetime;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::measurement::LineToGenerate;
    use crate::{now_ns, specification::*};
    use influxdb2_client::models::WriteDataPoint;

    type Error = Box<dyn std::error::Error>;
    type Result<T = (), E = Error> = std::result::Result<T, E>;

    impl Agent {
        /// Instantiate an agent only with the parameters we're interested in
        /// testing, keeping everything else constant across different
        /// tests.
        fn test_instance(
            sampling_interval: Option<Duration>,
            continue_on: bool,
            current_datetime: i64,
            end_datetime: i64,
        ) -> Self {
            let measurement_spec = MeasurementSpec {
                name: "measurement-{{agent.id}}-{{measurement.id}}".into(),
                count: Some(2),
                fields: vec![FieldSpec {
                    name: "field-{{agent.id}}-{{measurement.id}}-{{field.id}}".into(),
                    field_value_spec: FieldValueSpec::I64 {
                        range: 0..60,
                        increment: false,
                        reset_after: None,
                    },
                    count: Some(2),
                }],
                tag_pairs: vec![],
                tag_set: None,
            };

            let generated_tag_sets = GeneratedTagSets::default();

            let measurement_generators = MeasurementGenerator::from_spec(
                1,
                &measurement_spec,
                current_datetime,
                &generated_tag_sets,
                &[],
            )
            .unwrap();

            Self {
                id: 0,
                name: "foo".to_string(),
                finished: false,
                interval: None,

                sampling_interval,
                current_datetime,
                end_datetime,
                continue_on,
                measurement_generators,
            }
        }
    }

    fn timestamps(points: &[LineToGenerate]) -> Result<Vec<i64>> {
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
                let mut agent = Agent::test_instance(None, false, 0, 10);

                let points = agent.generate().await?.into_iter().flatten();
                assert_eq!(points.count(), 2);

                let points = agent.generate().await?.into_iter().flatten();
                let points: Vec<_> = points.collect();
                assert!(points.is_empty(), "expected no points, got {:?}", points);

                Ok(())
            }

            #[tokio::test]
            async fn current_time_equal_end_time() -> Result<()> {
                let mut agent = Agent::test_instance(None, false, 10, 10);

                let points = agent.generate().await?.into_iter().flatten();
                assert_eq!(points.count(), 2);

                let points = agent.generate().await?.into_iter().flatten();
                let points: Vec<_> = points.collect();
                assert!(points.is_empty(), "expected no points, got {:?}", points);

                Ok(())
            }

            #[tokio::test]
            async fn current_time_greater_than_end_time() -> Result<()> {
                let mut agent = Agent::test_instance(None, false, 11, 10);

                let points = agent.generate().await?.into_iter().flatten();
                assert_eq!(points.count(), 2);

                let points = agent.generate().await?.into_iter().flatten();
                let points: Vec<_> = points.collect();
                assert!(points.is_empty(), "expected no points, got {:?}", points);

                Ok(())
            }
        }

        mod with_continue {
            use super::*;

            #[tokio::test]
            async fn current_time_less_than_end_time() -> Result<()> {
                let mut agent = Agent::test_instance(None, true, 0, 10);

                let points = agent.generate().await?.into_iter().flatten();
                assert_eq!(points.count(), 2);

                let points = agent.generate().await?.into_iter().flatten();
                let points: Vec<_> = points.collect();
                assert!(points.is_empty(), "expected no points, got {:?}", points);

                Ok(())
            }

            #[tokio::test]
            async fn current_time_equal_end_time() -> Result<()> {
                let mut agent = Agent::test_instance(None, true, 10, 10);

                let points = agent.generate().await?.into_iter().flatten();
                assert_eq!(points.count(), 2);

                let points = agent.generate().await?.into_iter().flatten();
                let points: Vec<_> = points.collect();
                assert!(points.is_empty(), "expected no points, got {:?}", points);

                Ok(())
            }

            #[tokio::test]
            async fn current_time_greater_than_end_time() -> Result<()> {
                let mut agent = Agent::test_instance(None, true, 11, 10);

                let points = agent.generate().await?.into_iter().flatten();
                assert_eq!(points.count(), 2);

                let points = agent.generate().await?.into_iter().flatten();
                let points: Vec<_> = points.collect();
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
        const TEST_SAMPLING_INTERVAL: Duration = Duration::from_millis(10);

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
                let end = TEST_SAMPLING_INTERVAL.as_nanos() as i64;

                let mut agent =
                    Agent::test_instance(Some(TEST_SAMPLING_INTERVAL), false, current, end);

                let points = agent.generate().await?.into_iter().flatten();
                assert_eq!(points.count(), 2);

                let points = agent.generate().await?.into_iter().flatten();
                assert_eq!(points.count(), 2);

                let points = agent.generate().await?.into_iter().flatten();
                let points: Vec<_> = points.collect();
                assert!(points.is_empty(), "expected no points, got {:?}", points);

                Ok(())
            }

            #[tokio::test]
            async fn current_time_equal_end_time() -> Result<()> {
                let current = TEST_SAMPLING_INTERVAL.as_nanos() as i64;
                let end = current;

                let mut agent =
                    Agent::test_instance(Some(TEST_SAMPLING_INTERVAL), false, current, end);

                let points = agent.generate().await?.into_iter().flatten();
                assert_eq!(points.count(), 2);

                let points = agent.generate().await?.into_iter().flatten();
                let points: Vec<_> = points.collect();
                assert!(points.is_empty(), "expected no points, got {:?}", points);

                Ok(())
            }

            #[tokio::test]
            async fn current_time_greater_than_end_time() -> Result<()> {
                let current = 2 * TEST_SAMPLING_INTERVAL.as_nanos() as i64;
                let end = TEST_SAMPLING_INTERVAL.as_nanos() as i64;

                let mut agent =
                    Agent::test_instance(Some(TEST_SAMPLING_INTERVAL), false, current, end);

                let points = agent.generate().await?.into_iter().flatten();
                assert_eq!(points.count(), 2);

                let points = agent.generate().await?.into_iter().flatten();
                let points: Vec<_> = points.collect();
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
                let current = end - TEST_SAMPLING_INTERVAL.as_nanos() as i64;

                let mut agent =
                    Agent::test_instance(Some(TEST_SAMPLING_INTERVAL), true, current, end);

                let points = agent.generate().await?.into_iter().flatten();
                let points: Vec<_> = points.collect();
                assert_eq!(points.len(), 2);

                let times = timestamps(&points).unwrap();
                assert_eq!(vec![current, current], times);

                let points = agent.generate().await?.into_iter().flatten();
                let points: Vec<_> = points.collect();
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
                    Agent::test_instance(Some(TEST_SAMPLING_INTERVAL), true, current, end);

                let points = agent.generate().await?.into_iter().flatten();
                let points: Vec<_> = points.collect();
                assert_eq!(points.len(), 2);

                let times = timestamps(&points).unwrap();
                assert_eq!(vec![end, end], times);

                let points = agent.generate().await?.into_iter().flatten();
                let points: Vec<_> = points.collect();
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
                let current = end + TEST_SAMPLING_INTERVAL.as_nanos() as i64;

                let mut agent =
                    Agent::test_instance(Some(TEST_SAMPLING_INTERVAL), true, current, end);

                let points = agent.generate().await?.into_iter().flatten();
                let points: Vec<_> = points.collect();
                assert_eq!(points.len(), 2);

                let times = timestamps(&points).unwrap();
                assert_eq!(vec![current, current], times);

                let points = agent.generate().await?.into_iter().flatten();
                let points: Vec<_> = points.collect();
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
