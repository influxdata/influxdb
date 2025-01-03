use crate::commands::common::{parse_key_val, DataType, InfluxDb3Config, SeparatedList};
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use base64::Engine as _;
use rand::rngs::OsRng;
use rand::RngCore;
use secrecy::ExposeSecret;
use secrecy::Secret;
use sha2::Digest;
use sha2::Sha512;
use std::error::Error;
use std::num::NonZeroUsize;
use std::str;
use url::Url;

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum SubCommand {
    /// Create a new database
    Database(DatabaseConfig),
    /// Create a new table in a database
    Table(TableConfig),
    /// Create a new auth token
    Token,
    /// Create a new last cache
    LastCache(LastCacheConfig),
    /// Create a new metacache
    MetaCache(MetaCacheConfig),
}

#[derive(Debug, clap::Args)]
pub struct DatabaseConfig {
    /// The host URL of the running InfluxDB 3 Core server
    #[clap(
        short = 'H',
        long = "host",
        env = "INFLUXDB3_HOST_URL",
        default_value = "http://127.0.0.1:8181"
    )]
    pub host_url: Url,

    /// The token for authentication with the InfluxDB 3 Core server
    #[clap(long = "token", env = "INFLUXDB3_AUTH_TOKEN")]
    pub auth_token: Option<Secret<String>>,

    /// The database name to run the query against
    #[clap(env = "INFLUXDB3_DATABASE_NAME", required = true)]
    pub database_name: String,
}

#[derive(Debug, clap::Args)]
pub struct TableConfig {
    #[clap(long = "tags", required = true, num_args=0..)]
    tags: Vec<String>,

    #[clap(short = 'f', long = "fields", value_parser = parse_key_val::<String, DataType>, required = true, num_args=0..)]
    fields: Vec<(String, DataType)>,

    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    #[clap(required = true)]
    table_name: String,
}

#[derive(Debug, clap::Args)]
pub struct LastCacheConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// The table name for which the cache is being created
    #[clap(short = 't', long = "table")]
    table: String,

    /// Give a name for the cache.
    #[clap(long = "cache-name")]
    cache_name: Option<String>,

    /// Which columns in the table to use as keys in the cache
    #[clap(long = "key-columns")]
    key_columns: Option<SeparatedList<String>>,

    /// Which columns in the table to store as values in the cache
    #[clap(long = "value-columns")]
    value_columns: Option<SeparatedList<String>>,

    /// The number of entries per unique key column combination the cache will store
    #[clap(long = "count")]
    count: Option<usize>,

    /// The time-to-live (TTL) for entries in a cache in seconds
    #[clap(long = "ttl")]
    ttl: Option<u64>,
}

#[derive(Debug, clap::Args)]
pub struct MetaCacheConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// The table name for which the cache is being created
    #[clap(short = 't', long = "table")]
    table: String,

    /// Give the name of the cache.
    ///
    /// This will be automatically generated if not provided
    #[clap(long = "cache-name")]
    cache_name: Option<String>,

    /// Which columns in the table to cache distinct values for, as a comma-separated list of the
    /// column names.
    ///
    /// The cache is a hieararchical structure, with a level for each column specified; the order
    /// specified here will determine the order of the levels from top-to-bottom of the cache
    /// hierarchy.
    #[clap(long = "columns")]
    columns: SeparatedList<String>,

    /// The maximum number of distinct value combinations to hold in the cache
    #[clap(long = "max-cardinality")]
    max_cardinality: Option<NonZeroUsize>,

    /// The maximum age of an entry in the cache entered as a human-readable duration, e.g., "30d", "24h"
    #[clap(long = "max-age")]
    max_age: Option<humantime::Duration>,
}

pub async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    match config.cmd {
        SubCommand::Database(DatabaseConfig {
            host_url,
            database_name,
            auth_token,
        }) => {
            let mut client = influxdb3_client::Client::new(host_url)?;

            if let Some(t) = auth_token {
                client = client.with_auth_token(t.expose_secret());
            }

            client.api_v3_configure_db_create(&database_name).await?;

            println!("Database {:?} created successfully", &database_name);
        }
        SubCommand::Table(TableConfig {
            influxdb3_config:
                InfluxDb3Config {
                    host_url,
                    database_name,
                    auth_token,
                },
            table_name,
            tags,
            fields,
        }) => {
            let mut client = influxdb3_client::Client::new(host_url)?;
            if let Some(t) = auth_token {
                client = client.with_auth_token(t.expose_secret());
            }
            client
                .api_v3_configure_table_create(&database_name, &table_name, tags, fields)
                .await?;

            println!(
                "Table {:?}.{:?} created successfully",
                &database_name, &table_name
            );
        }
        SubCommand::Token => {
            let token = {
                let mut token = String::from("apiv3_");
                let mut key = [0u8; 64];
                OsRng.fill_bytes(&mut key);
                token.push_str(&B64.encode(key));
                token
            };
            println!(
                "\
                Token: {token}\n\
                Hashed Token: {hashed}\n\n\
                Start the server with `influxdb3 serve --bearer-token {hashed}`\n\n\
                HTTP requests require the following header: \"Authorization: Bearer {token}\"\n\
                This will grant you access to every HTTP endpoint or deny it otherwise
            ",
                hashed = hex::encode(&Sha512::digest(&token)[..])
            );
        }
        SubCommand::LastCache(LastCacheConfig {
            influxdb3_config:
                InfluxDb3Config {
                    host_url,
                    database_name,
                    auth_token,
                },
            table,
            cache_name,
            key_columns,
            value_columns,
            count,
            ttl,
        }) => {
            let mut client = influxdb3_client::Client::new(host_url)?;
            if let Some(t) = auth_token {
                client = client.with_auth_token(t.expose_secret());
            }
            let mut b = client.api_v3_configure_last_cache_create(database_name, table);

            // Add optional parameters:
            if let Some(name) = cache_name {
                b = b.name(name);
            }
            if let Some(keys) = key_columns {
                b = b.key_columns(keys);
            }
            if let Some(vals) = value_columns {
                b = b.value_columns(vals);
            }
            if let Some(count) = count {
                b = b.count(count);
            }
            if let Some(ttl) = ttl {
                b = b.ttl(ttl);
            }

            // Make the request:
            match b.send().await? {
                Some(def) => println!(
                    "new cache created: {}",
                    serde_json::to_string_pretty(&def)
                        .expect("serialize last cache definition as JSON")
                ),
                None => println!("a cache already exists for the provided parameters"),
            }
        }
        SubCommand::MetaCache(MetaCacheConfig {
            influxdb3_config:
                InfluxDb3Config {
                    host_url,
                    database_name,
                    auth_token,
                },
            table,
            cache_name,
            columns,
            max_cardinality,
            max_age,
        }) => {
            let mut client = influxdb3_client::Client::new(host_url)?;
            if let Some(t) = auth_token {
                client = client.with_auth_token(t.expose_secret());
            }
            let mut b = client.api_v3_configure_meta_cache_create(database_name, table, columns);

            // Add the optional stuff:
            if let Some(name) = cache_name {
                b = b.name(name);
            }
            if let Some(max_cardinality) = max_cardinality {
                b = b.max_cardinality(max_cardinality);
            }
            if let Some(max_age) = max_age {
                b = b.max_age(max_age.into());
            }

            match b.send().await? {
                Some(def) => println!(
                    "new cache created: {}",
                    serde_json::to_string_pretty(&def)
                        .expect("serialize meta cache definition as JSON")
                ),
                None => println!("a cache already exists for the provided parameters"),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    #[test]
    fn parse_args() {
        let args = super::Config::parse_from([
            "create",
            "last-cache",
            "--database",
            "bar",
            "--table",
            "foo",
            "--cache-name",
            "bar",
            "--key-columns",
            "tag1,tag2,tag3",
            "--value-columns",
            "field1,field2,field3",
            "--ttl",
            "3600",
            "--count",
            "5",
        ]);
        let super::SubCommand::LastCache(super::LastCacheConfig {
            table,
            cache_name,
            key_columns,
            value_columns,
            count,
            ttl,
            influxdb3_config: crate::commands::common::InfluxDb3Config { database_name, .. },
        }) = args.cmd
        else {
            panic!("Did not parse args correctly: {args:#?}")
        };
        assert_eq!("bar", database_name);
        assert_eq!("foo", table);
        assert!(cache_name.is_some_and(|n| n == "bar"));
        assert!(key_columns.is_some_and(|keys| keys.0 == ["tag1", "tag2", "tag3"]));
        assert!(value_columns.is_some_and(|vals| vals.0 == ["field1", "field2", "field3"]));
        assert!(count.is_some_and(|c| c == 5));
        assert!(ttl.is_some_and(|t| t == 3600));
    }
}
