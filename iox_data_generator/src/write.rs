//! Writing generated points

use futures::stream;
use influxdb2_client::models::{DataPoint, PostBucketRequest, WriteDataPoint};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
#[cfg(test)]
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use std::{
    fs,
    fs::OpenOptions,
    path::{Path, PathBuf},
};
use tracing::info;

/// Errors that may happen while writing points.
#[derive(Snafu, Debug)]
pub enum Error {
    /// Error that may happen when writing line protocol to a no-op sink
    #[snafu(display("Could not generate line protocol: {}", source))]
    CantWriteToNoOp {
        /// Underlying IO error that caused this problem
        source: std::io::Error,
    },

    /// Error that may happen when writing line protocol to a file
    #[snafu(display("Could not write line protocol to file: {}", source))]
    CantWriteToLineProtocolFile {
        /// Underlying IO error that caused this problem
        source: std::io::Error,
    },

    /// Error that may happen when creating a directory to store files to write
    /// to
    #[snafu(display("Could not create directory: {}", source))]
    CantCreateDirectory {
        /// Underlying IO error that caused this problem
        source: std::io::Error,
    },

    /// Error that may happen when checking a path's metadata to see if it's a
    /// directory
    #[snafu(display("Could not get metadata: {}", source))]
    CantGetMetadata {
        /// Underlying IO error that caused this problem
        source: std::io::Error,
    },

    /// Error that may happen if the path given to the file-based writer isn't a
    /// directory
    #[snafu(display("Expected to get a directory"))]
    MustBeDirectory,

    /// Error that may happen while writing points to the API
    #[snafu(display("Could not write points to API: {}", source))]
    CantWriteToApi {
        /// Underlying Influx client request error that caused this problem
        source: influxdb2_client::RequestError,
    },

    /// Error that may happen while trying to create a bucket via the API
    #[snafu(display("Could not create bucket: {}", source))]
    CantCreateBucket {
        /// Underlying Influx client request error that caused this problem
        source: influxdb2_client::RequestError,
    },

    /// Error that may happen if attempting to create a bucket without
    /// specifying the org ID
    #[snafu(display("Could not create a bucket without an `org_id`"))]
    OrgIdRequiredToCreateBucket,
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Responsible for holding shared configuration needed to construct per-agent
/// points writers
#[derive(Debug)]
pub struct PointsWriterBuilder {
    config: PointsWriterConfig,
}

#[derive(Debug)]
enum PointsWriterConfig {
    Api {
        client: influxdb2_client::Client,
        org: String,
        bucket: String,
    },
    Directory(PathBuf),
    NoOp {
        perform_write: bool,
    },
    #[cfg(test)]
    Vector(BTreeMap<String, Arc<Mutex<Vec<u8>>>>),
}

impl PointsWriterBuilder {
    /// Write points to the API at the specified host and put them in the
    /// specified org and bucket.
    pub async fn new_api(
        host: impl Into<String> + Send,
        org: impl Into<String> + Send,
        bucket: impl Into<String> + Send,
        token: impl Into<String> + Send,
        create_bucket: bool,
        org_id: Option<&str>,
    ) -> Result<Self> {
        let host = host.into();

        // Be somewhat lenient on what we accept as far as host; the client expects the
        // protocol to be included. We could pull in the url crate and do more
        // verification here.
        let host = if host.starts_with("http") {
            host
        } else {
            format!("http://{}", host)
        };

        let client = influxdb2_client::Client::new(host, token.into());
        let org = org.into();
        let bucket = bucket.into();

        if create_bucket {
            let org_id = org_id.context(OrgIdRequiredToCreateBucket)?.to_string();
            let bucket = PostBucketRequest {
                org_id,
                name: bucket.clone(),
                ..Default::default()
            };

            client
                .create_bucket(Some(bucket))
                .await
                .context(CantCreateBucket)?;
        }

        Ok(Self {
            config: PointsWriterConfig::Api {
                client,
                org,
                bucket,
            },
        })
    }

    /// Write points to a file in the directory specified.
    pub fn new_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        fs::create_dir_all(&path).context(CantCreateDirectory)?;
        let metadata = fs::metadata(&path).context(CantGetMetadata)?;
        ensure!(metadata.is_dir(), MustBeDirectory);

        Ok(Self {
            config: PointsWriterConfig::Directory(PathBuf::from(path.as_ref())),
        })
    }

    /// Generate points but do not write them anywhere
    pub fn new_no_op(perform_write: bool) -> Self {
        Self {
            config: PointsWriterConfig::NoOp { perform_write },
        }
    }

    /// Create a writer out of this writer's configuration for a particular
    /// agent that runs in a separate thread/task.
    pub fn build_for_agent(&mut self, agent_name: &str) -> PointsWriter {
        let inner_writer = match &mut self.config {
            PointsWriterConfig::Api {
                client,
                org,
                bucket,
            } => InnerPointsWriter::Api {
                client: client.clone(),
                org: org.clone(),
                bucket: bucket.clone(),
            },
            PointsWriterConfig::Directory(dir_path) => {
                let mut filename = dir_path.clone();
                filename.push(agent_name);
                filename.set_extension("txt");
                InnerPointsWriter::File(filename)
            }
            PointsWriterConfig::NoOp { perform_write } => InnerPointsWriter::NoOp {
                perform_write: *perform_write,
            },
            #[cfg(test)]
            PointsWriterConfig::Vector(ref mut agents_by_name) => {
                let v = agents_by_name
                    .entry(agent_name.to_string())
                    .or_insert_with(|| Arc::new(Mutex::new(Vec::new())));
                InnerPointsWriter::Vec(Arc::clone(v))
            }
        };

        PointsWriter { inner_writer }
    }
}

/// Responsible for writing points to the location it's been configured for.
#[derive(Debug)]
pub struct PointsWriter {
    inner_writer: InnerPointsWriter,
}

impl PointsWriter {
    /// Write these points
    pub async fn write_points(&mut self, points: Vec<DataPoint>) -> Result<()> {
        self.inner_writer.write_points(points).await
    }
}

#[derive(Debug)]
enum InnerPointsWriter {
    Api {
        client: influxdb2_client::Client,
        org: String,
        bucket: String,
    },
    File(PathBuf),
    NoOp {
        perform_write: bool,
    },
    #[cfg(test)]
    Vec(Arc<Mutex<Vec<u8>>>),
}

impl InnerPointsWriter {
    async fn write_points(&mut self, points: Vec<DataPoint>) -> Result<()> {
        match self {
            Self::Api {
                client,
                org,
                bucket,
            } => {
                client
                    .write(org, bucket, stream::iter(points))
                    .await
                    .context(CantWriteToApi)?;
            }
            Self::File(filename) => {
                info!("Opening file {:?}", filename);
                let num_points = points.len();
                let file = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(&filename)
                    .context(CantWriteToLineProtocolFile)?;

                let mut file = std::io::BufWriter::new(file);
                for point in points {
                    point
                        .write_data_point_to(&mut file)
                        .context(CantWriteToLineProtocolFile)?;
                }
                info!("Wrote {} points to {:?}", num_points, filename);
            }
            Self::NoOp { perform_write } => {
                if *perform_write {
                    let mut sink = std::io::sink();

                    for point in points {
                        point
                            .write_data_point_to(&mut sink)
                            .context(CantWriteToNoOp)?;
                    }
                }
            }
            #[cfg(test)]
            Self::Vec(ref mut vec) => {
                let vec_ref = Arc::clone(vec);
                let mut vec = vec_ref.lock().expect("Should be able to get lock");
                for point in points {
                    point
                        .write_data_point_to(&mut *vec)
                        .expect("Should be able to write to vec");
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{generate, now_ns, specification::*, ZeroRng};
    use std::str::FromStr;

    type Error = Box<dyn std::error::Error>;
    type Result<T = (), E = Error> = std::result::Result<T, E>;

    impl PointsWriterBuilder {
        fn new_vec() -> Self {
            Self {
                config: PointsWriterConfig::Vector(BTreeMap::new()),
            }
        }

        fn written_data(self, agent_name: &str) -> String {
            match self.config {
                PointsWriterConfig::Vector(agents_by_name) => {
                    let bytes_ref =
                        Arc::clone(agents_by_name.get(agent_name).expect(
                            "Should have written some data, did not find any for this agent",
                        ));
                    let bytes = bytes_ref
                        .lock()
                        .expect("Should have been able to get a lock");
                    String::from_utf8(bytes.to_vec()).expect("we should be generating valid UTF-8")
                }
                _ => unreachable!("this method is only valid when writing to a vector for testing"),
            }
        }
    }

    #[tokio::test]
    async fn test_generate() -> Result<()> {
        let toml = r#"
name = "demo_schema"
base_seed = "this is a demo"

[[agents]]
name = "basic"

[[agents.measurements]]
name = "cpu"

[[agents.measurements.fields]]
name = "up"
bool = true"#;

        let data_spec = DataSpec::from_str(toml).unwrap();
        let mut points_writer_builder = PointsWriterBuilder::new_vec();

        let now = now_ns();

        generate::<ZeroRng>(
            &data_spec,
            &mut points_writer_builder,
            Some(now),
            Some(now),
            now,
            false,
            1,
        )
        .await?;

        let line_protocol = points_writer_builder.written_data("basic");

        let expected_line_protocol = format!(
            r#"cpu,data_spec=demo_schema up=f {}
"#,
            now
        );
        assert_eq!(line_protocol, expected_line_protocol);

        Ok(())
    }

    #[tokio::test]
    async fn test_generate_batches() -> Result<()> {
        let toml = r#"
name = "demo_schema"
base_seed = "this is a demo"

[[agents]]
name = "basic"
sampling_interval = "1s" # seconds

[[agents.measurements]]
name = "cpu"

[[agents.measurements.fields]]
name = "up"
bool = true"#;

        let data_spec = DataSpec::from_str(toml).unwrap();
        let mut points_writer_builder = PointsWriterBuilder::new_vec();

        let now = now_ns();

        generate::<ZeroRng>(
            &data_spec,
            &mut points_writer_builder,
            Some(now - 1_000_000_000),
            Some(now),
            now,
            false,
            2,
        )
        .await?;

        let line_protocol = points_writer_builder.written_data("basic");

        let expected_line_protocol = format!(
            r#"cpu,data_spec=demo_schema up=f {}
cpu,data_spec=demo_schema up=f {}
"#,
            now - 1_000_000_000,
            now
        );
        assert_eq!(line_protocol, expected_line_protocol);

        Ok(())
    }
}
