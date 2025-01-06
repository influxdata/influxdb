use crate::commands::common::{InfluxDb3Config, SeparatedKeyValue, SeparatedList};
use influxdb3_client::plugin_development::WalPluginTestRequest;
use secrecy::ExposeSecret;
use std::collections::HashMap;
use std::error::Error;

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    #[clap(flatten)]
    wal_plugin_test: WalPluginTest,
}

#[derive(Debug, clap::Parser)]
pub struct WalPluginTest {
    /// The name of the plugin, which should match its file name on the server `<plugin-dir>/<name>.py`
    #[clap(short = 'n', long = "name")]
    pub name: String,
    /// If given, pass this line protocol as input
    #[clap(long = "lp")]
    pub input_lp: Option<String>,
    /// If given, pass this file of LP as input from on the server `<plugin-dir>/<name>_test/<input-file>`
    #[clap(long = "file")]
    pub input_file: Option<String>,
    /// If given pass this map of string key/value pairs as input arguments
    #[clap(long = "input-arguments")]
    pub input_arguments: Option<SeparatedList<SeparatedKeyValue<String, String>>>,
}

impl From<WalPluginTest> for WalPluginTestRequest {
    fn from(val: WalPluginTest) -> Self {
        let input_arguments = val.input_arguments.map(|a| {
            a.into_iter()
                .map(|SeparatedKeyValue((k, v))| (k, v))
                .collect::<HashMap<String, String>>()
        });

        Self {
            name: val.name,
            input_lp: val.input_lp,
            input_file: val.input_file,
            input_arguments,
        }
    }
}

pub(super) async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    let InfluxDb3Config {
        host_url,
        auth_token,
        ..
    } = config.influxdb3_config;

    let wal_plugin_test_request: WalPluginTestRequest = config.wal_plugin_test.into();

    let mut client = influxdb3_client::Client::new(host_url)?;
    if let Some(t) = auth_token {
        client = client.with_auth_token(t.expose_secret());
    }
    let response = client.wal_plugin_test(wal_plugin_test_request).await?;

    let res = serde_json::to_string_pretty(&response)
        .expect("serialize wal plugin test response as JSON");

    // pretty print the response
    println!("{}", res);

    Ok(())
}
