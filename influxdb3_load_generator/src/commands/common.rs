use std::{fs::File, path::PathBuf};

use anyhow::{anyhow, bail, Context};
use chrono::Local;
use clap::Parser;
use influxdb3_client::Client;
use secrecy::{ExposeSecret, Secret};
use url::Url;

use crate::specification::{DataSpec, QuerierSpec};

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

    /// The path to the spec file to use for this run. Or specify a name of a builtin spec to use.
    /// If not specified, the generator will output a list of builtin specs along with help and
    /// an example for writing your own.
    #[clap(short = 's', long = "spec", env = "INFLUXDB3_LOAD_DATA_SPEC_PATH")]
    pub(crate) spec_path: Option<String>,

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
    /// ```ignore
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
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum LoadType {
    Write,
    Query,
    #[allow(dead_code)]
    Full,
}

#[derive(Debug, Default)]
pub(crate) struct LoadConfig {
    pub(crate) database_name: String,
    results_dir: PathBuf,
    print_mode: bool,
    pub(crate) write_spec: Option<DataSpec>,
    pub(crate) write_results_file_path: Option<String>,
    pub(crate) write_results_file: Option<File>,
    pub(crate) query_spec: Option<QuerierSpec>,
    pub(crate) query_results_file_path: Option<String>,
    pub(crate) query_results_file: Option<File>,
    pub(crate) system_stats_file_path: Option<String>,
    pub(crate) system_stats_file: Option<File>,
}

impl LoadConfig {
    fn new(database_name: String, results_dir: PathBuf, print_mode: bool) -> Self {
        Self {
            database_name,
            results_dir,
            print_mode,
            ..Default::default()
        }
    }

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

    fn setup_system(&mut self, time_str: &str) -> Result<(), anyhow::Error> {
        let file_path = self.results_dir.join(format!("system_{time_str}.csv"));
        self.system_stats_file_path = Some(path_buf_to_string(file_path.clone())?);
        self.system_stats_file =
            Some(File::create_new(file_path).context("system stats file already exists")?);
        Ok(())
    }
}

fn path_buf_to_string(path: PathBuf) -> Result<String, anyhow::Error> {
    path.into_os_string()
        .into_string()
        .map_err(|os_str| anyhow!("write file path could not be converted to a string: {os_str:?}"))
}

impl InfluxDb3Config {
    pub(crate) async fn initialize(
        self,
        load_type: LoadType,
    ) -> Result<(Client, LoadConfig), anyhow::Error> {
        let Self {
            host_url,
            database_name,
            auth_token,
            spec_path,
            builtin_spec,
            print_spec,
            results_dir,
            configuration_name,
            system_stats,
        } = self;

        if spec_path.is_none() && builtin_spec.is_none() {
            if matches!(load_type, LoadType::Write) {
                // TODO - print help for query as well
                crate::commands::write::print_help();
            }
            bail!("You did not provide a spec path or specify a built-in spec");
        }

        let built_in_specs = crate::specs::built_in_specs();

        // sepcify a time string for generated results file names:
        let time_str = format!("{}", Local::now().format("%Y-%m-%d-%H-%M"));

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
        let mut config = LoadConfig::new(database_name, results_dir, print_spec);

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
                    let spec = DataSpec::from_path(&spec_path.unwrap())?;
                    let spec_name = spec.name.to_owned();
                    config.setup_dir(&spec_name, &config_name)?;
                    config.setup_write(&time_str, spec)?;
                }
                LoadType::Query => {
                    let spec = QuerierSpec::from_path(&spec_path.unwrap())?;
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
