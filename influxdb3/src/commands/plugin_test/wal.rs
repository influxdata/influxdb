use crate::commands::common::InfluxDb3Config;
use influxdb3_client::plugin_test::WalPluginTestRequest;
use secrecy::ExposeSecret;
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
    /// The name of the plugin, which should match its file name on the server <plugin-dir>/<name>.py
    #[clap(short = 'n', long = "name")]
    pub name: String,
    /// If given, pass this line protocol as input
    #[clap(long = "lp")]
    pub input_lp: Option<String>,
    /// If given, pass this file of LP as input from on the server <plugin-dir>/<name>_test/<input-file>
    #[clap(long = "file")]
    pub input_file: Option<String>,
    /// If given, save the output to this file on the server in <plugin-dir>/<name>_test/<save-output-to-file>
    #[clap(long = "save-output-to-file")]
    pub save_output_to_file: Option<String>,
    /// If given, validate the output against this file on the server in <plugin-dir>/<name>_test/<validate-output-file>
    #[clap(long = "validate-output-file")]
    pub validate_output_file: Option<String>,
}

impl Into<WalPluginTestRequest> for WalPluginTest {
    fn into(self) -> WalPluginTestRequest {
        WalPluginTestRequest {
            name: self.name,
            input_lp: self.input_lp,
            input_file: self.input_file,
            save_output_to_file: self.save_output_to_file,
            validate_output_file: self.validate_output_file,
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
    let resonse = client.wal_plugin_test(wal_plugin_test_request).await?;

    // pretty print the response
    println!(
        "RESPONSE:\n{}",
        serde_json::to_string_pretty(&resonse).expect("serialize wal plugin test response as JSON")
    );

    Ok(())
}
