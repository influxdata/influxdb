use std::{fs::File, path::PathBuf, str::FromStr, sync::Arc};

use anyhow::{anyhow, bail, Context};
use chrono::{DateTime, Local};
use clap::Parser;
use influxdb3_client::Client;
use secrecy::{ExposeSecret, Secret};
use url::Url;

use crate::{
    report::{QueryReporter, SystemStatsReporter, WriteReporter},
    specification::{DataSpec, QuerierSpec},
};

#[derive(Debug, Parser)]
pub(crate) struct InfluxDb3Config {
    /// The host URL of the running InfluxDB 3.0 server
    #[clap(
        short = 'h',
        long = "host",
        env = "INFLUXDB3_HOST_URL",
        default_value = "http://127.0.0.1:8181"
    )]
    pub(crate) host_url: Url,

    /// The database name to generate load against
    #[clap(
        short = 'd',
        long = "dbname",
        env = "INFLUXDB3_DATABASE_NAME",
        default_value = "load_test"
    )]
    pub(crate) database_name: String,

    /// The token for authentication with the InfluxDB 3.0 server
    #[clap(long = "token", env = "INFLUXDB3_AUTH_TOKEN")]
    pub(crate) auth_token: Option<Secret<String>>,

    /// The name of the builtin spec to run. Use this instead of spec_path if you want to run
    /// one of the builtin specs as is.
    #[clap(long = "builtin-spec", env = "INFLUXDB3_LOAD_BUILTIN_SPEC")]
    pub(crate) builtin_spec: Option<String>,

    /// The name of the builtin spec to print to stdout. This is useful for seeing the structure
    /// of the builtin as a starting point for creating your own.
    #[clap(long = "print-spec", default_value_t = false)]
    pub(crate) print_spec: bool,

    /// The directory to save results to.
    ///
    /// If not specified, this will default to `results` in the current directory.
    ///
    /// Files saved here will be organized in a directory structure as follows:
    /// ```text
    /// results/<s>/<c>/<write|query|system>_<time>.csv`
    /// ```
    /// where,
    /// - `<s>`: the name of the load gen spec, e.g., `one_mil`
    /// - `<c>`: the provided `configuration_name`, or will default to the revision SHA of the
    ///   `influxdb3` binary
    /// - `<write|query|system>`: results for the `write` load, `query` load, or `system` stats of the
    ///   `influxdb3` binary, respectively.
    /// - `<time>`: a timestamp of when the test started in `YYYY-MM-DD-HH-MM` format.
    #[clap(
        short = 'r',
        long = "results-dir",
        env = "INFLUXDB3_LOAD_RESULTS_DIR",
        default_value = "results"
    )]
    pub(crate) results_dir: PathBuf,

    /// Provide a custom `configuration_name` for the generated results directory.
    ///
    /// If left blank, this will default to the revision SHA of the target `influxdb3` binary
    /// under test.
    #[clap(long = "config-name")]
    pub(crate) configuration_name: Option<String>,

    /// Generate a system stats file in the specified `results_dir`
    #[clap(long = "system-stats", default_value_t = false)]
    pub(crate) system_stats: bool,

    /// Provide an end time to stop the load generation.
    ///
    /// This can be a human readable offset, e.g., `10m` (10 minutes), `1h` (1 hour), etc., or an
    /// exact date-time in RFC3339 form, e.g., `2023-10-30T19:10:00-04:00`.
    #[clap(long = "end")]
    pub(crate) end_time: Option<FutureOffsetTime>,
}

/// A time in the future
///
/// Wraps a [`DateTime`], providing a custom [`FromStr`] implementation that parses human
/// time input and converts it to a date-time in the future.
#[derive(Debug, Clone, Copy)]
pub struct FutureOffsetTime(DateTime<Local>);

impl FromStr for FutureOffsetTime {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(t) = humantime::parse_rfc3339_weak(s) {
            Ok(Self(DateTime::<Local>::from(t)))
        } else {
            humantime::parse_duration(s)
                .map(|d| Local::now() + d)
                .map(Self)
                .with_context(|| format!("could not parse future offset time value: {s}"))
        }
    }
}

impl From<FutureOffsetTime> for DateTime<Local> {
    fn from(t: FutureOffsetTime) -> Self {
        t.0
    }
}

/// Can run the load generation tool exclusively in either `query` or `write` mode, or
/// run both at the same time
#[derive(Debug, Clone, Copy)]
pub(crate) enum LoadType {
    Write,
    Query,
    #[allow(dead_code)]
    Full,
}

/// A configuration for driving a `write` or `query` load generation run
#[derive(Debug, Default)]
pub(crate) struct LoadConfig {
    /// The target database name on the `influxdb3` server
    pub(crate) database_name: String,
    /// The end time of the load generation run
    pub(crate) end_time: Option<DateTime<Local>>,
    /// The directory that will store generated results files
    results_dir: PathBuf,
    /// If `true`, the configuration will initialize only to print out
    /// the spec as JSON, it will not create any files
    print_mode: bool,
    write_spec: Option<DataSpec>,
    write_results_file_path: Option<String>,
    write_results_file: Option<File>,
    query_spec: Option<QuerierSpec>,
    query_results_file_path: Option<String>,
    query_results_file: Option<File>,
    system_stats_file_path: Option<String>,
    system_stats_file: Option<File>,
}

impl LoadConfig {
    /// Create a new [`LoadConfig`]
    fn new(
        database_name: String,
        results_dir: PathBuf,
        end_time: Option<impl Into<DateTime<Local>>>,
        print_mode: bool,
    ) -> Self {
        let end_time: Option<DateTime<Local>> = end_time.map(Into::into);
        if let Some(t) = end_time {
            println!("running load generation until: {t}");
        } else {
            println!("running load generation indefinitely");
        }
        Self {
            database_name,
            results_dir,
            print_mode,
            end_time,
            ..Default::default()
        }
    }

    /// Setup the results directory for the load generation run.
    ///
    /// This will be used to store all results files generated.
    fn setup_dir(&mut self, spec_name: &str, config_name: &str) -> Result<(), anyhow::Error> {
        if self.print_mode {
            return Ok(());
        }
        self.results_dir = self.results_dir.join(format!("{spec_name}/{config_name}"));
        std::fs::create_dir_all(&self.results_dir).with_context(|| {
            format!(
                "failed to initialize the results directory at '{dir:#?}'",
                dir = self.results_dir.as_os_str()
            )
        })
    }

    /// Setup the `write` results file along with its path and the [`DataSpec`]
    fn setup_write(&mut self, time_str: &str, spec: DataSpec) -> Result<(), anyhow::Error> {
        if self.print_mode {
            println!("Write Spec:\n{}", spec.to_json_string_pretty()?);
            return Ok(());
        }
        self.write_spec = Some(spec);
        let file_path = self.results_dir.join(format!("write_{time_str}.csv"));
        self.write_results_file_path = Some(path_buf_to_string(file_path.clone())?);
        self.write_results_file =
            Some(File::create_new(file_path).context("write results file already exists")?);
        Ok(())
    }

    /// Setup the `query` results file along with its path and the [`QuerierSpec`]
    fn setup_query(&mut self, time_str: &str, spec: QuerierSpec) -> Result<(), anyhow::Error> {
        if self.print_mode {
            println!("Query Spec:\n{}", spec.to_json_string_pretty()?);
            return Ok(());
        }
        self.query_spec = Some(spec);
        let file_path = self.results_dir.join(format!("query_{time_str}.csv"));
        self.query_results_file_path = Some(path_buf_to_string(file_path.clone())?);
        self.query_results_file =
            Some(File::create_new(file_path).context("query results file already exists")?);
        Ok(())
    }

    /// Setup the `system` stats file along with its path
    fn setup_system(&mut self, time_str: &str) -> Result<(), anyhow::Error> {
        let file_path = self.results_dir.join(format!("system_{time_str}.csv"));
        self.system_stats_file_path = Some(path_buf_to_string(file_path.clone())?);
        self.system_stats_file =
            Some(File::create_new(file_path).context("system stats file already exists")?);
        Ok(())
    }

    pub(crate) fn query_spec(&mut self) -> Result<QuerierSpec, anyhow::Error> {
        self.query_spec
            .take()
            .context("there is no loaded query spec")
    }

    pub(crate) fn write_spec(&mut self) -> Result<DataSpec, anyhow::Error> {
        self.write_spec
            .take()
            .context("there is no loaded write spec")
    }

    /// Get the [`QueryReporter`] along with the path of the file it is generating as a `String`
    pub(crate) fn query_reporter(&mut self) -> Result<(String, Arc<QueryReporter>), anyhow::Error> {
        let file = self
            .query_results_file
            .take()
            .context("no generated query results file")?;
        let path = self
            .query_results_file_path
            .take()
            .context("no generated query results file path")?;

        // set up a results reporter and spawn a thread to flush results
        println!("generating query results in: {path}");
        let query_reporter = Arc::new(QueryReporter::new(file));
        let reporter = Arc::clone(&query_reporter);
        tokio::task::spawn_blocking(move || {
            reporter.flush_reports();
        });
        Ok((path, query_reporter))
    }

    /// Get the [`QueryReporter`] along with the path of the file it is generating as a `String`
    pub(crate) fn write_reporter(&mut self) -> Result<(String, Arc<WriteReporter>), anyhow::Error> {
        let file = self
            .write_results_file
            .take()
            .context("no generated write results file")?;
        let path = self
            .write_results_file_path
            .take()
            .context("no generated write results file path")?;

        // set up a results reporter and spawn a thread to flush results
        println!("generating write results in: {path}");
        let write_reporter = Arc::new(WriteReporter::new(file)?);
        let reporter = Arc::clone(&write_reporter);
        tokio::task::spawn_blocking(move || {
            reporter.flush_reports();
        });
        Ok((path, write_reporter))
    }

    /// Get a [`SystemStatsReporter`] along with the path of the file it is generating as a `String`
    pub(crate) fn system_reporter(
        &mut self,
    ) -> Result<Option<(String, Arc<SystemStatsReporter>)>, anyhow::Error> {
        if let (Some(stats_file), Some(stats_file_path)) = (
            self.system_stats_file.take(),
            self.system_stats_file_path.take(),
        ) {
            println!("generating system stats in: {stats_file_path}");
            let stats_reporter = Arc::new(SystemStatsReporter::new(stats_file)?);
            let s = Arc::clone(&stats_reporter);
            tokio::task::spawn_blocking(move || {
                s.report_stats();
            });
            Ok(Some((stats_file_path, stats_reporter)))
        } else {
            Ok(None)
        }
    }
}

fn path_buf_to_string(path: PathBuf) -> Result<String, anyhow::Error> {
    path.into_os_string()
        .into_string()
        .map_err(|os_str| anyhow!("write file path could not be converted to a string: {os_str:?}"))
}

impl InfluxDb3Config {
    pub(crate) async fn initialize_query(
        self,
        querier_spec_path: Option<PathBuf>,
    ) -> Result<(Client, LoadConfig), anyhow::Error> {
        self.initialize(LoadType::Query, querier_spec_path, None)
            .await
    }

    pub(crate) async fn initialize_write(
        self,
        writer_spec_path: Option<PathBuf>,
    ) -> Result<(Client, LoadConfig), anyhow::Error> {
        self.initialize(LoadType::Write, None, writer_spec_path)
            .await
    }

    pub(crate) async fn initialize_full(
        self,
        querier_spec_path: Option<PathBuf>,
        writer_spec_path: Option<PathBuf>,
    ) -> Result<(Client, LoadConfig), anyhow::Error> {
        self.initialize(LoadType::Full, querier_spec_path, writer_spec_path)
            .await
    }

    async fn initialize(
        self,
        load_type: LoadType,
        querier_spec_path: Option<PathBuf>,
        writer_spec_path: Option<PathBuf>,
    ) -> Result<(Client, LoadConfig), anyhow::Error> {
        let Self {
            host_url,
            database_name,
            auth_token,
            builtin_spec,
            print_spec,
            results_dir,
            configuration_name,
            system_stats,
            end_time,
        } = self;

        match (
            builtin_spec.as_ref(),
            load_type,
            querier_spec_path.as_ref(),
            writer_spec_path.as_ref(),
        ) {
            (None, LoadType::Write | LoadType::Full, _, None)
            | (None, LoadType::Query | LoadType::Full, None, _) => {
                if matches!(load_type, LoadType::Write) {
                    // TODO - print help for query as well
                    crate::commands::write::print_help();
                }
                bail!("You did not provide a spec path or specify a built-in spec");
            }
            _ => (),
        }

        let built_in_specs = crate::specs::built_in_specs();

        // sepcify a time string for generated results file names:
        let time_str = format!("{}", Local::now().format("%Y-%m-%dT%H-%M-%S"));

        // initialize the influxdb3 client:
        let client =
            create_client(host_url, auth_token).context("unable to create influxdb3 client")?;

        // use the user-specified configuration name, or pull it from the running server:
        let config_name = if let Some(n) = configuration_name {
            n
        } else {
            client
                .ping()
                .await
                .context("influxdb3 server did not respond to ping request")?
                .revision()
                .to_owned()
        };

        // initialize the load config:
        let mut config = LoadConfig::new(database_name, results_dir, end_time, print_spec);

        // if builtin spec is set, use that instead of the spec path
        if let Some(b) = builtin_spec {
            let builtin = built_in_specs
                .into_iter()
                .find(|spec| spec.name == *b)
                .with_context(|| {
                    let names = crate::specs::built_in_spec_names().join(", ");
                    format!(
                        "built-in spec with name '{b}' not found, available built-in specs are: {names}"
                    )
                })?;
            println!("using built-in spec: {}", builtin.name);
            let spec_name = builtin.name.as_str();
            config.setup_dir(spec_name, &config_name)?;
            match load_type {
                LoadType::Write => {
                    config.setup_write(&time_str, builtin.write_spec)?;
                }
                LoadType::Query => {
                    config.setup_query(&time_str, builtin.query_spec)?;
                }
                LoadType::Full => {
                    config.setup_write(&time_str, builtin.write_spec)?;
                    config.setup_query(&time_str, builtin.query_spec)?;
                }
            }
        } else {
            match load_type {
                LoadType::Write => {
                    let spec = DataSpec::from_path(writer_spec_path.unwrap())?;
                    let spec_name = spec.name.to_owned();
                    config.setup_dir(&spec_name, &config_name)?;
                    config.setup_write(&time_str, spec)?;
                }
                LoadType::Query => {
                    let spec = QuerierSpec::from_path(querier_spec_path.unwrap())?;
                    let spec_name = spec.name.to_owned();
                    config.setup_dir(&spec_name, &config_name)?;
                    config.setup_query(&time_str, spec)?;
                }
                LoadType::Full => {
                    bail!("can only run custom specs with the `query` or `write` load directly")
                }
            }
        };
        // if print spec is set, print the spec and exit
        if print_spec {
            bail!("exiting after printing spec");
        }

        // Setup the system stats file if specified
        if system_stats {
            config.setup_system(&time_str)?;
        }

        Ok((client, config))
    }
}

pub(crate) fn create_client(
    host_url: Url,
    auth_token: Option<Secret<String>>,
) -> Result<Client, influxdb3_client::Error> {
    let mut client = Client::new(host_url)?;
    if let Some(t) = auth_token {
        client = client.with_auth_token(t.expose_secret());
    }
    Ok(client)
}
