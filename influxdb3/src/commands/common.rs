use std::str::FromStr;

use clap::Parser;
use secrecy::Secret;
use url::Url;

#[derive(Debug, Parser)]
pub struct InfluxDb3Config {
    /// The host URL of the running InfluxDB 3.0 server
    #[clap(
        short = 'h',
        long = "host",
        env = "INFLUXDB3_HOST_URL",
        default_value = "http://127.0.0.1:8181"
    )]
    pub host_url: Url,

    /// The database name to run the query against
    #[clap(short = 'd', long = "dbname", env = "INFLUXDB3_DATABASE_NAME")]
    pub database_name: String,

    /// The token for authentication with the InfluxDB 3.0 server
    #[clap(long = "token", env = "INFLUXDB3_AUTH_TOKEN")]
    pub auth_token: Option<Secret<String>>,
}

/// A clap argument privided as a list of items separated by `SEPARATOR`, which by default is a ','
#[derive(Debug, Clone)]
pub struct SeparatedList<T, const SEPARATOR: char = ','>(pub Vec<T>);

impl<T, const SEPARATOR: char> FromStr for SeparatedList<T, SEPARATOR>
where
    T: FromStr<Err: Into<anyhow::Error>>,
{
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(
            s.split(SEPARATOR)
                .map(|s| s.parse::<T>().map_err(Into::into))
                .collect::<Result<Vec<T>, Self::Err>>()?,
        ))
    }
}

impl<T, const SEPARATOR: char> IntoIterator for SeparatedList<T, SEPARATOR> {
    type Item = T;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
