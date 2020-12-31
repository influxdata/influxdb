//! This crate contains the IOx "configuration management system" such
//! as it is.
//!
//! IOx follows [The 12 Factor
//! Application Guidance](https://12factor.net/config) in respect to
//! configuration, and thus expects to read its configuration from the
//! environment.
//!
//! To facilitate local development and testing, configuration values
//! can also be stored in a "config" file, which defaults to
//! `$HOME/.influxdb_iox/config`
//!
//! Configuration values are name=value pairs in the style of
//! [dotenv](https://crates.io/crates/dotenv), meant to closely mirror
//! environment variables.
//!
//! If there is a value specified both in the environment *and* in the
//! config file, the value in the environment will take precidence
//! over the value in the config file, and a warning will be displayed.
//!
//! Note that even though IOx reads all configuration values from the
//! environment, *internally* IOx code should use this structure
//! rather than reading on the environment (which are process-wide
//! global variables) directly. Not only does this consolidate the
//! configuration in a single location, it avoids all the bad side
//! effects of global variables.
use std::{
    collections::HashMap,
    fmt,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use snafu::{ResultExt, Snafu};
mod item;

use item::ConfigItem;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error loading env config file '{:?}': {}", config_path, source))]
    LoadingConfigFile {
        config_path: PathBuf,
        source: dotenv::Error,
    },

    #[snafu(display("Error parsing env config file '{:?}': {}", config_path, source))]
    ParsingConfigFile {
        config_path: PathBuf,
        source: dotenv::Error,
    },

    #[snafu(display(
        "Error setting config '{}' to value '{}': {}",
        config_name,
        config_value,
        message
    ))]
    ValidationError {
        config_name: String,
        config_value: String,
        message: String,
    },

    #[snafu(display(
        "Error setting config '{}'. Expected value like '{}'. Got value '{}': : {}",
        config_name,
        example,
        config_value,
        message
    ))]
    ValidationErrorWithExample {
        config_name: String,
        example: String,
        config_value: String,
        message: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The InfluxDB application configuration. This struct provides typed
/// access to all of IOx's configuration values.
#[derive(Debug)]
pub struct Config {
    //--- Logging ---
    /// basic logging level
    pub rust_log: Option<String>,

    /// Open Telemetry Jaeger hostname
    pub otel_jaeger_host: Option<String>,

    /// Database Writer ID (TODO make this mandatory)
    pub writer_id: Option<u32>,

    /// port to listen for HTTP API
    pub http_bind_address: SocketAddr,

    /// port to listen for gRPC API
    pub grpc_bind_address: SocketAddr,

    /// Directory to store local database files
    pub database_directory: PathBuf,

    // --- GCP fields ---
    /// GCP object store bucekt
    pub gcp_bucket: Option<String>,
}

impl Config {
    /// Create the configuration object from a set of
    /// name=value pairs
    ///
    /// ADD NEW CONFIG VALUES HERE
    fn new_from_map(name_values: HashMap<String, String>) -> Result<Self> {
        use item::*;
        Ok(Config {
            rust_log: Self::parse_config(&name_values, &RustLog {})?,
            otel_jaeger_host: Self::parse_config(&name_values, &OTJaegerAgentHost {})?,
            writer_id: Self::parse_config(&name_values, &WriterID {})?,
            http_bind_address: Self::parse_config(&name_values, &HttpBindAddr {})?,
            grpc_bind_address: Self::parse_config(&name_values, &GrpcBindAddr {})?,
            database_directory: Self::parse_config(&name_values, &DBDir {})?,
            gcp_bucket: Self::parse_config(&name_values, &GCPBucket {})?,
        })
    }

    /// Displays this config, item by item.
    ///
    /// ADD NEW CONFIG VALUES HERE
    fn display_items(&self, f: &mut fmt::Formatter<'_>, verbose: bool) -> fmt::Result {
        use item::*;
        RustLog {}.display(f, &self.rust_log, verbose)?;
        OTJaegerAgentHost {}.display(f, &self.otel_jaeger_host, verbose)?;
        WriterID {}.display(f, &self.writer_id, verbose)?;
        HttpBindAddr {}.display(f, &self.http_bind_address, verbose)?;
        GrpcBindAddr {}.display(f, &self.grpc_bind_address, verbose)?;
        DBDir {}.display(f, &self.database_directory, verbose)?;
        GCPBucket {}.display(f, &self.gcp_bucket, verbose)?;
        Ok(())
    }

    /// returns the location of the default config file: ~/.influxdb_iox/config
    pub fn default_config_file() -> PathBuf {
        dirs::home_dir()
            .map(|a| a.join(".influxdb_iox").join("config"))
            .expect("Can not find home directory")
    }

    /// Creates a new Config object by reading from specified file, in
    /// dotenv format, and from the the provided values.
    ///
    /// Any values in `env_values` override the values in the file
    ///
    /// Returns an error if there is a problem reading the specified
    /// file or any validation fails
    fn try_from_path_then_map(
        config_path: &Path,
        env_values: HashMap<String, String>,
    ) -> Result<Self> {
        // load initial values from file
        //
        // Note, from_filename_iter method got "undeprecated" but that change is not yet
        // released: https://github.com/dotenv-rs/dotenv/pull/54
        #[allow(deprecated)]
        let parsed_values: Vec<(String, String)> = dotenv::from_filename_iter(config_path)
            .context(LoadingConfigFile { config_path })?
            .map(|item| item.context(ParsingConfigFile { config_path }))
            .collect::<Result<_>>()?;

        let mut name_values: HashMap<String, String> = parsed_values.into_iter().collect();

        // Apply values in `env_values` as an override to anything
        // found in config file, warning if so
        for (name, env_val) in env_values.iter() {
            let file_value = name_values.get(name);
            if let Some(file_value) = file_value {
                if file_value != env_val {
                    eprintln!(
                        "WARNING value for configuration item {} in file,  '{}'  hidden by value in environment '{}'",
                        name, file_value, env_val
                    );
                }
            }
        }

        // Now, mash in the values
        for (name, env_val) in env_values.into_iter() {
            name_values.insert(name, env_val);
        }

        Self::new_from_map(name_values)
    }

    /// creates a new Config object by reading from specified file, in
    /// dotenv format, and from the the provided values.
    ///
    /// Any values in `name_values` override the values in the file
    ///
    /// Returns an error if there is a problem reading the specified
    /// file or any validation fails
    pub fn try_from_path(config_path: &Path) -> Result<Self> {
        let name_values: HashMap<String, String> = std::env::vars().collect();
        Self::try_from_path_then_map(config_path, name_values)
    }

    /// creates a new Config object by reading from the environment variables
    /// only.
    ///
    /// Returns an error if any validation fails
    pub fn new_from_env() -> Result<Self> {
        // get all name/value pairs into a map and feed the config values one by one
        let name_values: HashMap<String, String> = std::env::vars().collect();
        Self::new_from_map(name_values)
    }

    /// Parse a single configuration item described with item and
    /// returns the value parsed
    fn parse_config<T>(
        name_values: &HashMap<String, String>,
        item: &impl ConfigItem<T>,
    ) -> Result<T> {
        let config_name = item.name();
        let config_value = name_values
            .get(config_name)
            .map(|s| s.to_owned())
            // If no config value was specified in the map, use the default from the item
            .or_else(|| item.default());

        item.parse(config_value.as_ref().map(|s| s.as_ref()))
            .map_err(|message| {
                let config_value = config_value.unwrap_or_else(|| "".into());

                let example = item.example().or_else(|| item.default());
                if let Some(example) = example {
                    Error::ValidationErrorWithExample {
                        config_name: config_name.into(),
                        config_value,
                        example,
                        message,
                    }
                } else {
                    Error::ValidationError {
                        config_name: config_name.into(),
                        config_value,
                        message,
                    }
                }
            })
    }

    /// return something which can be formatted using "{}" that gives
    /// detailed information about each config value
    pub fn verbose_display(&self) -> impl fmt::Display + '_ {
        struct Wrapper<'a>(&'a Config);

        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.display_items(f, true)
            }
        }
        Wrapper(self)
    }
}

impl fmt::Display for Config {
    /// Default display is the minimal configuration
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.display_items(f, false)
    }
}

#[cfg(test)]
mod test {
    use test_helpers::{assert_contains, make_temp_file};

    use super::*;

    // End to end positive test case for all valid config values to validate they
    // are hooked up
    #[test]
    fn test_all_values() {
        let name_values: HashMap<String, String> = vec![
            ("INFLUXDB_IOX_BIND_ADDR".into(), "127.0.0.1:1010".into()),
            (
                "INFLUXDB_IOX_GRPC_BIND_ADDR".into(),
                "127.0.0.2:2020".into(),
            ),
            ("INFLUXDB_IOX_DB_DIR".into(), "/foo/bar".into()),
            ("INFLUXDB_IOX_ID".into(), "42".into()),
            ("INFLUXDB_IOX_GCP_BUCKET".into(), "my_bucket".into()),
            ("RUST_LOG".into(), "rust_log_level".into()),
            (
                "OTEL_EXPORTER_JAEGER_AGENT_HOST".into(),
                "example.com".into(),
            ),
        ]
        .into_iter()
        .collect();
        let config = Config::new_from_map(name_values).unwrap();
        assert_eq!(config.rust_log, Some("rust_log_level".into()));
        assert_eq!(config.otel_jaeger_host, Some("example.com".into()));
        assert_eq!(config.writer_id, Some(42));
        assert_eq!(config.http_bind_address.to_string(), "127.0.0.1:1010");
        assert_eq!(config.grpc_bind_address.to_string(), "127.0.0.2:2020");
        assert_eq!(config.gcp_bucket, Some("my_bucket".into()));
    }

    #[test]
    fn test_display() {
        let config = Config::new_from_map(HashMap::new()).expect("IOx can work with just defaults");
        let config_display = normalize(&format!("{}", config));
        // test that the basic output is connected
        assert_contains!(&config_display, "INFLUXDB_IOX_DB_DIR=$HOME/.influxdb_iox");
        assert_contains!(&config_display, "INFLUXDB_IOX_BIND_ADDR=127.0.0.1:8080");
    }

    #[test]
    fn test_verbose_display() {
        let config = Config::new_from_map(HashMap::new()).expect("IOx can work with just defaults");
        let config_display = normalize(&format!("{}", config.verbose_display()));
        // test that the basic output is working and connected
        assert_contains!(
            &config_display,
            r#"INFLUXDB_IOX_BIND_ADDR: HTTP bind address
  current value: 127.0.0.1:8080
  default value: 127.0.0.1:8080

The address on which IOx will serve HTTP API requests
"#
        );
    }

    #[test]
    fn test_default_config_file() {
        assert_eq!(
            &normalize(&Config::default_config_file().to_string_lossy()),
            "$HOME/.influxdb_iox/config"
        );
    }

    #[test]
    fn test_default() {
        // Since the actual implementation uses env variables
        // directly, the tests use a different source
        let config = Config::new_from_map(HashMap::new()).expect("IOx can work with just defaults");
        // Spot some values to make sure they look good
        assert_eq!(
            &normalize(&config.database_directory.to_string_lossy()),
            "$HOME/.influxdb_iox"
        );
        assert_eq!(&config.http_bind_address.to_string(), "127.0.0.1:8080");
        // config items without default shouldn't be set
        assert_eq!(config.otel_jaeger_host, None);
    }

    #[test]
    fn test_default_override() {
        let name_values: HashMap<String, String> = vec![
            ("INFLUXDB_IOX_BIND_ADDR".into(), "127.0.0.1:1010".into()),
            (
                "INFLUXDB_IOX_GRPC_BIND_ADDR".into(),
                "127.0.0.2:2020".into(),
            ),
        ]
        .into_iter()
        .collect();
        let config = Config::new_from_map(name_values).unwrap();
        assert_eq!(&config.http_bind_address.to_string(), "127.0.0.1:1010");
        assert_eq!(&config.grpc_bind_address.to_string(), "127.0.0.2:2020");
    }

    #[test]
    fn test_vars_from_file() {
        let config_file = make_temp_file(
            "INFLUXDB_IOX_BIND_ADDR=127.0.0.3:3030\n\
             INFLUXDB_IOX_GRPC_BIND_ADDR=127.0.0.4:4040",
        );

        let config = Config::try_from_path_then_map(config_file.path(), HashMap::new()).unwrap();
        assert_eq!(&config.http_bind_address.to_string(), "127.0.0.3:3030");
        assert_eq!(&config.grpc_bind_address.to_string(), "127.0.0.4:4040");
    }

    #[test]
    fn test_vars_from_env_take_precidence() {
        // Given variables specified in both the config file and the
        // environment, the ones in the environment take precidence
        let config_file = make_temp_file(
            "INFLUXDB_IOX_BIND_ADDR=127.0.0.3:3030\n\
             INFLUXDB_IOX_GRPC_BIND_ADDR=127.0.0.4:4040",
        );

        let name_values: HashMap<String, String> = vec![
            ("INFLUXDB_IOX_BIND_ADDR".into(), "127.0.0.1:1010".into()),
            (
                "INFLUXDB_IOX_GRPC_BIND_ADDR".into(),
                "127.0.0.2:2020".into(),
            ),
        ]
        .into_iter()
        .collect();

        let config = Config::try_from_path_then_map(config_file.path(), name_values).unwrap();

        assert_eq!(&config.http_bind_address.to_string(), "127.0.0.1:1010");
        assert_eq!(&config.grpc_bind_address.to_string(), "127.0.0.2:2020");
    }

    #[test]
    fn test_vars_from_non_existent_file() {
        let config_file = make_temp_file(
            "INFLUXDB_IOX_BIND_ADDR=127.0.0.3:3030\n\
             INFLUXDB_IOX_GRPC_BIND_ADDR=127.0.0.4:4040",
        );
        let dangling_path: PathBuf = config_file.path().into();
        std::mem::drop(config_file); // force the temp file to be removed

        let result = Config::try_from_path_then_map(&dangling_path, HashMap::new());
        let error_message = format!("{}", result.unwrap_err());
        assert_contains!(&error_message, "Error loading env config file");
        assert_contains!(&error_message, dangling_path.to_string_lossy());
        assert_contains!(&error_message, "path not found");
    }

    /// test for using variable substitution in config file (.env style)
    /// it is really a test for .env but I use this test to document
    /// the expected behavior of iox config files.
    #[test]
    fn test_var_substitution_in_file() {
        let config_file = make_temp_file(
            "THE_HOSTNAME=127.0.0.1\n\
             INFLUXDB_IOX_BIND_ADDR=${THE_HOSTNAME}:3030\n\
             INFLUXDB_IOX_GRPC_BIND_ADDR=${THE_HOSTNAME}:4040",
        );

        let config = Config::try_from_path_then_map(config_file.path(), HashMap::new()).unwrap();
        assert_eq!(&config.http_bind_address.to_string(), "127.0.0.1:3030");
        assert_eq!(&config.grpc_bind_address.to_string(), "127.0.0.1:4040");
    }

    /// test for using variable substitution in config file with
    /// existing environment.  Again, this is really a test for .env
    /// but I use this test to document the expected behavior of iox
    /// config files.
    #[test]
    fn test_var_substitution_in_file_from_env() {
        std::env::set_var("MY_AWESOME_HOST", "192.100.100.42");
        let config_file = make_temp_file("INFLUXDB_IOX_BIND_ADDR=${MY_AWESOME_HOST}:3030\n");

        let config = Config::try_from_path_then_map(config_file.path(), HashMap::new()).unwrap();
        assert_eq!(&config.http_bind_address.to_string(), "192.100.100.42:3030");
        std::env::remove_var("MY_AWESOME_HOST");
    }

    /// test for using comments in config file (.env style)
    /// it is really a test for .env but I use this test to document
    /// the expected behavior of iox config files.
    #[test]
    fn test_comments_in_config_file() {
        let config_file = make_temp_file(
            "#INFLUXDB_IOX_BIND_ADDR=127.0.0.3:3030\n\
             INFLUXDB_IOX_GRPC_BIND_ADDR=127.0.0.4:4040",
        );

        let config = Config::try_from_path_then_map(config_file.path(), HashMap::new()).unwrap();
        // Should have the default value as the config file's value is commented out
        assert_eq!(&config.http_bind_address.to_string(), "127.0.0.1:8080");
        // Should have the value in the config file
        assert_eq!(&config.grpc_bind_address.to_string(), "127.0.0.4:4040");
    }

    #[test]
    fn test_invalid_config_data() {
        let config_file = make_temp_file(
            "HOSTNAME=127.0.0.1\n\
             INFLUXDB_IOX_BIND_ADDR=${HO",
        );

        let error_message = Config::try_from_path_then_map(config_file.path(), HashMap::new())
            .unwrap_err()
            .to_string();
        assert_contains!(&error_message, "Error parsing env config file");
        assert_contains!(&error_message, config_file.path().to_string_lossy());
        assert_contains!(&error_message, "'${HO', error at line index: 3");
    }

    #[test]
    fn test_parse_config_value() {
        let empty_values = HashMap::<String, String>::new();
        let name_values: HashMap<String, String> = vec![
            ("TEST_CONFIG".into(), "THE_VALUE".into()),
            ("SOMETHING ELSE".into(), "QUE PASA?".into()),
        ]
        .into_iter()
        .collect();

        let item_without_default = ConfigItemWithoutDefault {};
        let error_message: String = Config::parse_config(&empty_values, &item_without_default)
            .unwrap_err()
            .to_string();
        assert_eq!(error_message, "Error setting config 'TEST_CONFIG' to value '': no value specified for ConfigItemWithoutDefault");
        assert_eq!(
            Config::parse_config(&name_values, &item_without_default).unwrap(),
            "THE_VALUE"
        );

        let item_with_default = ConfigItemWithDefault {};
        assert_eq!(
            Config::parse_config(&empty_values, &item_with_default).unwrap(),
            "THE_DEFAULT"
        );
        assert_eq!(
            Config::parse_config(&name_values, &item_without_default).unwrap(),
            "THE_VALUE"
        );

        let item_without_default = ConfigItemWithoutDefaultButWithExample {};
        let error_message: String = Config::parse_config(&empty_values, &item_without_default)
            .unwrap_err()
            .to_string();
        assert_eq!(error_message, "Error setting config 'TEST_CONFIG'. Expected value like 'THE_EXAMPLE'. Got value '': : no value specified for ConfigItemWithoutDefaultButWithExample");
        assert_eq!(
            Config::parse_config(&name_values, &item_without_default).unwrap(),
            "THE_VALUE"
        );
    }

    /// normalizes things in a config file that change in different
    /// environments (e.g. a home directory)
    fn normalize(v: &str) -> String {
        let home_dir: String = dirs::home_dir().unwrap().to_string_lossy().into();
        v.replace(&home_dir, "$HOME")
    }

    struct ConfigItemWithDefault {}

    impl ConfigItem<String> for ConfigItemWithDefault {
        fn name(&self) -> &'static str {
            "TEST_CONFIG"
        }
        fn short_description(&self) -> String {
            "A test config".into()
        }
        fn default(&self) -> Option<String> {
            Some("THE_DEFAULT".into())
        }
        fn parse(&self, val: Option<&str>) -> std::result::Result<String, String> {
            val.map(|s| s.to_string())
                .ok_or_else(|| String::from("no value specified for ConfigItemWithDefault"))
        }
        fn unparse(&self, val: &String) -> String {
            val.to_string()
        }
    }

    struct ConfigItemWithoutDefault {}

    impl ConfigItem<String> for ConfigItemWithoutDefault {
        fn name(&self) -> &'static str {
            "TEST_CONFIG"
        }
        fn short_description(&self) -> String {
            "A test config".into()
        }
        fn parse(&self, val: Option<&str>) -> std::result::Result<String, String> {
            val.map(|s| s.to_string())
                .ok_or_else(|| String::from("no value specified for ConfigItemWithoutDefault"))
        }
        fn unparse(&self, val: &String) -> String {
            val.to_string()
        }
    }

    struct ConfigItemWithoutDefaultButWithExample {}

    impl ConfigItem<String> for ConfigItemWithoutDefaultButWithExample {
        fn name(&self) -> &'static str {
            "TEST_CONFIG"
        }
        fn short_description(&self) -> String {
            "A test config".into()
        }
        fn example(&self) -> Option<String> {
            Some("THE_EXAMPLE".into())
        }
        fn parse(&self, val: Option<&str>) -> std::result::Result<String, String> {
            val.map(|s| s.to_string()).ok_or_else(|| {
                String::from("no value specified for ConfigItemWithoutDefaultButWithExample")
            })
        }
        fn unparse(&self, val: &String) -> String {
            val.to_string()
        }
    }
}
