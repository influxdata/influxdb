use std::path::PathBuf;

use clap::Parser;
use influxdb3_client::Client;
use secrecy::{ExposeSecret, Secret};
use url::Url;

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
    #[clap(long = "print-spec")]
    pub(crate) print_spec: Option<String>,

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
    #[clap(short = 'r', long = "results-dir", env = "INFLUXDB3_LOAD_RESULTS_DIR")]
    pub(crate) results_dir: Option<PathBuf>,

    /// Provide a custom `configuration_name` for the generated results directory.
    ///
    /// If left blank, this will default to the revision SHA of the target `influxdb3` binary
    /// under test.
    #[clap(long = "config-name")]
    pub(crate) configuration_name: Option<String>,
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

pub(crate) async fn init_results_dir(
    results_dir: Option<PathBuf>,
    spec_name: &str,
    configuration_name: Option<String>,
) {
    todo!();
}
