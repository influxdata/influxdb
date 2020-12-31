//! Contains individual config item definitions

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

/// Represents a single typed configuration item, specified as a
/// name=value pair.
///
/// This metadata is present so that IOx can programatically create
/// readable config files and provide useful help and debugging
/// information
///
/// The type parameter `T` is the type of the value of the
/// configuration item
pub(crate) trait ConfigItem<T> {
    /// Return the name of the config value (environment variable name)
    fn name(&self) -> &'static str;

    /// A one sentence description
    fn short_description(&self) -> String;

    /// Default value, if any
    fn default(&self) -> Option<String> {
        None
    }

    /// An optional example value
    fn example(&self) -> Option<String> {
        // (if default is provided, there is often no need for an
        // additional example)
        self.default()
    }

    /// An optional longer form description,
    fn long_description(&self) -> Option<String> {
        None
    }

    /// Parses an instance of this `ConfigItem` item from an optional
    /// string representation, the value of `default()` is passed.
    ///
    /// If an empty value is not valid for this item, an error should
    /// be returned.
    ///
    /// If an empty value is valid, then the config item should return
    /// a value that encodes an empty value correctly.
    fn parse(&self, val: Option<&str>) -> std::result::Result<T, String>;

    /// Convert a parsed value of this config item (that came from a
    /// call to `parse`) back into a String, for display purposes
    fn unparse(&self, val: &T) -> String;

    /// Display the value of an individual ConfigItem. If verbose is
    /// true, shows full help information, if false, shows minimal
    /// name=value env variable form
    fn display(&self, f: &mut std::fmt::Formatter<'_>, val: &T, verbose: bool) -> std::fmt::Result {
        let val = self.unparse(val);

        if verbose {
            writeln!(f, "-----------------")?;
            writeln!(f, "{}: {}", self.name(), self.short_description())?;
            writeln!(f, "  current value: {}", val)?;
            if let Some(default) = self.default() {
                writeln!(f, "  default value: {}", default)?;
            }
            if let Some(example) = self.example() {
                if self.default() != self.example() {
                    writeln!(f, "  example value: {}", example)?;
                }
            }
            if let Some(long_description) = self.long_description() {
                writeln!(f)?;
                writeln!(f, "{}", long_description)?;
            }
        } else if !val.is_empty() {
            write!(f, "{}={}", self.name(), val)?;

            // also add a note if it is different than the default value
            if let Some(default) = self.default() {
                if default != val {
                    write!(f, " # (default {})", default)?;
                }
            }
            writeln!(f)?;
        }
        Ok(())
    }
}

pub(crate) struct HttpBindAddr {}

impl ConfigItem<SocketAddr> for HttpBindAddr {
    fn name(&self) -> &'static str {
        "INFLUXDB_IOX_BIND_ADDR"
    }
    fn short_description(&self) -> String {
        "HTTP bind address".into()
    }
    fn default(&self) -> Option<String> {
        Some("127.0.0.1:8080".into())
    }
    fn long_description(&self) -> Option<String> {
        Some("The address on which IOx will serve HTTP API requests".into())
    }
    fn parse(&self, val: Option<&str>) -> std::result::Result<SocketAddr, String> {
        let addr: &str = val.ok_or_else(|| String::from("Empty value is not valid"))?;

        addr.parse()
            .map_err(|e| format!("Error parsing as SocketAddress address: {}", e))
    }
    fn unparse(&self, val: &SocketAddr) -> String {
        format!("{:?}", val)
    }
}

pub(crate) struct GrpcBindAddr {}

impl ConfigItem<SocketAddr> for GrpcBindAddr {
    fn name(&self) -> &'static str {
        "INFLUXDB_IOX_GRPC_BIND_ADDR"
    }
    fn short_description(&self) -> String {
        "gRPC bind address".into()
    }
    fn default(&self) -> Option<String> {
        Some("127.0.0.1:8082".into())
    }
    fn long_description(&self) -> Option<String> {
        Some("The address on which IOx will serve Storage gRPC API requests".into())
    }
    fn parse(&self, val: Option<&str>) -> std::result::Result<SocketAddr, String> {
        let addr: &str = val.ok_or_else(|| String::from("Empty value is not valid"))?;

        addr.parse()
            .map_err(|e| format!("Error parsing as SocketAddress address: {}", e))
    }
    fn unparse(&self, val: &SocketAddr) -> String {
        format!("{:?}", val)
    }
}

pub(crate) struct DBDir {}

impl ConfigItem<PathBuf> for DBDir {
    fn name(&self) -> &'static str {
        "INFLUXDB_IOX_DB_DIR"
    }
    fn short_description(&self) -> String {
        "Where to store files on disk:".into()
    }
    // default database path is $HOME/.influxdb_iox
    fn default(&self) -> Option<String> {
        dirs::home_dir()
            .map(|mut path| {
                path.push(".influxdb_iox");
                path
            })
            .and_then(|dir| dir.to_str().map(|s| s.to_string()))
    }
    fn long_description(&self) -> Option<String> {
        Some("The location InfluxDB IOx will use to store files locally".into())
    }
    fn parse(&self, val: Option<&str>) -> std::result::Result<PathBuf, String> {
        let location: &str = val.ok_or_else(|| String::from("database directory not specified"))?;

        Ok(Path::new(location).into())
    }
    fn unparse(&self, val: &PathBuf) -> String {
        // path came from a string, so it should be able to go back
        val.as_path().to_str().unwrap().to_string()
    }
}

pub(crate) struct WriterID {}

impl ConfigItem<Option<u32>> for WriterID {
    fn name(&self) -> &'static str {
        "INFLUXDB_IOX_ID"
    }
    fn short_description(&self) -> String {
        "The identifier for the server".into()
    }
    // There is no default datbase ID (on purpose)
    fn long_description(&self) -> Option<String> {
        Some(
            "The identifier for the server. Used for writing to object storage and as\
             an identifier that is added to replicated writes, WAL segments and Chunks. \
             Must be unique in a group of connected or semi-connected IOx servers. \
              Must be a number that can be represented by a 32-bit unsigned integer."
                .into(),
        )
    }
    fn parse(&self, val: Option<&str>) -> std::result::Result<Option<u32>, String> {
        val.map(|val| {
            val.parse::<u32>()
                .map_err(|e| format!("Error parsing {} as a u32:: {}", val, e))
        })
        .transpose()
    }
    fn unparse(&self, val: &Option<u32>) -> String {
        if let Some(val) = val.as_ref() {
            format!("{}", val)
        } else {
            "".into()
        }
    }
}

pub(crate) struct GCPBucket {}

impl ConfigItem<Option<String>> for GCPBucket {
    fn name(&self) -> &'static str {
        "INFLUXDB_IOX_GCP_BUCKET"
    }
    fn short_description(&self) -> String {
        "The bucket name, if using Google Cloud Storage as an object store".into()
    }
    fn example(&self) -> Option<String> {
        Some("bucket_name".into())
    }
    fn long_description(&self) -> Option<String> {
        Some(
            "If using Google Cloud Storage for the object store, this item, \
              as well as SERVICE_ACCOUNT must be set."
                .into(),
        )
    }
    fn parse(&self, val: Option<&str>) -> std::result::Result<Option<String>, String> {
        Ok(val.map(|s| s.to_string()))
    }
    fn unparse(&self, val: &Option<String>) -> String {
        if let Some(val) = val.as_ref() {
            val.to_string()
        } else {
            "".into()
        }
    }
}

/// This value simply passed into the environment and used by the
/// various loggign / tracing libraries. It has its own structure here
/// for documentation purposes and so it can be loaded from config file.
pub(crate) struct RustLog {}

impl ConfigItem<Option<String>> for RustLog {
    fn name(&self) -> &'static str {
        "RUST_LOG"
    }
    fn short_description(&self) -> String {
        "Rust logging level".into()
    }
    fn default(&self) -> Option<String> {
        Some("warn".into())
    }
    fn example(&self) -> Option<String> {
        Some("debug,hyper::proto::h1=info".into())
    }
    fn long_description(&self) -> Option<String> {
        Some(
            "This controls the IOx server logging level, as described in \
              https://crates.io/crates/env_logger. Levels for different modules can \
              be specified as well. For example `debug,hyper::proto::h1=info` \
              specifies debug logging for all modules except for the `hyper::proto::h1' module \
              which will only display info level logging."
                .into(),
        )
    }
    fn parse(&self, val: Option<&str>) -> std::result::Result<Option<String>, String> {
        Ok(val.map(|s| s.to_string()))
    }
    fn unparse(&self, val: &Option<String>) -> String {
        if let Some(val) = val.as_ref() {
            val.to_string()
        } else {
            "".into()
        }
    }
}

/// This value simply passed into the environment and used by open
/// telemetry create. It has its own structure here
/// for documentation purposes and so it can be loaded from config file.
pub(crate) struct OTJaegerAgentHost {}

impl ConfigItem<Option<String>> for OTJaegerAgentHost {
    fn name(&self) -> &'static str {
        "OTEL_EXPORTER_JAEGER_AGENT_HOST"
    }
    fn short_description(&self) -> String {
        "Open Telemetry Jaeger Host".into()
    }
    fn example(&self) -> Option<String> {
        Some("jaeger.influxdata.net".into())
    }
    fn long_description(&self) -> Option<String> {
        Some("If set, Jaeger traces are emitted to this host \
              using the OpenTelemetry tracer.\n\n\
              \
              NOTE: The OpenTelemetry agent CAN ONLY be \
              configured using environment variables. It CAN NOT be configured \
              using the IOx config file at this time. Some useful variables:\n \
              * OTEL_SERVICE_NAME: emitter service name (iox by default)\n \
              * OTEL_EXPORTER_JAEGER_AGENT_HOST: hostname/address of the collector\n \
              * OTEL_EXPORTER_JAEGER_AGENT_PORT: listening port of the collector.\n\n\
              \
              The entire list of variables can be found in \
              https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/sdk-environment-variables.md#jaeger-exporter".into())
    }
    fn parse(&self, val: Option<&str>) -> std::result::Result<Option<String>, String> {
        Ok(val.map(|s| s.to_string()))
    }
    fn unparse(&self, val: &Option<String>) -> String {
        if let Some(val) = val.as_ref() {
            val.to_string()
        } else {
            "".into()
        }
    }
}

#[cfg(test)]
mod test {
    use test_helpers::{assert_contains, assert_not_contains};

    use super::*;
    use std::fmt;

    #[test]
    fn test_display() {
        let item: TestConfigItem = Default::default();
        let val = String::from("current_value");

        assert_eq!(
            item.convert_to_string(&val),
            "TEST_CONFIG_ITEM=current_value\n"
        );

        let verbose_string = item.convert_to_verbose_string(&val);
        assert_contains!(&verbose_string, "TEST_CONFIG_ITEM: short_description");
        assert_contains!(&verbose_string, "current value: current_value");
        assert_not_contains!(&verbose_string, "default");
        assert_not_contains!(&verbose_string, "example");
    }

    #[test]
    fn test_display_verbose_with_default() {
        let item = TestConfigItem {
            default: Some("the_default_value".into()),
            ..Default::default()
        };

        let val = String::from("current_value");
        assert_eq!(
            item.convert_to_string(&val),
            "TEST_CONFIG_ITEM=current_value # (default the_default_value)\n"
        );

        let verbose_string = item.convert_to_verbose_string(&val);
        assert_contains!(&verbose_string, "TEST_CONFIG_ITEM: short_description");
        assert_contains!(&verbose_string, "current value: current_value");
        assert_contains!(&verbose_string, "default value: the_default_value");
        assert_not_contains!(&verbose_string, "example");
    }

    #[test]
    fn test_display_verbose_with_default_and_different_example() {
        let item = TestConfigItem {
            default: Some("the_default_value".into()),
            example: Some("the_example_value".into()),
            ..Default::default()
        };

        let val = String::from("current_value");
        assert_eq!(
            item.convert_to_string(&val),
            "TEST_CONFIG_ITEM=current_value # (default the_default_value)\n"
        );

        let verbose_string = item.convert_to_verbose_string(&val);
        assert_contains!(&verbose_string, "TEST_CONFIG_ITEM: short_description");
        assert_contains!(&verbose_string, "current value: current_value");
        assert_contains!(&verbose_string, "default value: the_default_value");
        assert_contains!(&verbose_string, "example value: the_example_value");
    }

    #[test]
    fn test_display_verbose_with_same_default_and_example() {
        let item = TestConfigItem {
            default: Some("the_value".into()),
            example: Some("the_value".into()),
            ..Default::default()
        };

        let val = String::from("current_value");
        assert_eq!(
            item.convert_to_string(&val),
            "TEST_CONFIG_ITEM=current_value # (default the_value)\n"
        );

        let verbose_string = item.convert_to_verbose_string(&val);
        assert_contains!(&verbose_string, "TEST_CONFIG_ITEM: short_description");
        assert_contains!(&verbose_string, "current value: current_value");
        assert_contains!(&verbose_string, "default value: the_value");
        assert_not_contains!(&verbose_string, "example");
    }

    #[test]
    fn test_display_verbose_with_long_description() {
        let item = TestConfigItem {
            long_description: Some("this is a long description".into()),
            ..Default::default()
        };

        let val = String::from("current_value");
        assert_eq!(
            item.convert_to_string(&val),
            "TEST_CONFIG_ITEM=current_value\n"
        );

        let verbose_string = item.convert_to_verbose_string(&val);
        assert_contains!(&verbose_string, "TEST_CONFIG_ITEM: short_description");
        assert_contains!(&verbose_string, "current value: current_value");
        assert_contains!(&verbose_string, "this is a long description\n");
        assert_not_contains!(&verbose_string, "example");
    }

    #[derive(Debug, Default)]
    struct TestConfigItem {
        pub default: Option<String>,
        pub example: Option<String>,
        pub long_description: Option<String>,
    }

    impl ConfigItem<String> for TestConfigItem {
        fn name(&self) -> &'static str {
            "TEST_CONFIG_ITEM"
        }

        fn short_description(&self) -> String {
            "short_description".into()
        }

        fn parse(&self, val: Option<&str>) -> Result<String, String> {
            Ok(val.map(|s| s.to_string()).unwrap_or_else(|| "".into()))
        }

        fn unparse(&self, val: &String) -> String {
            val.to_string()
        }

        fn default(&self) -> Option<String> {
            self.default.clone()
        }

        fn example(&self) -> Option<String> {
            self.example.clone()
        }

        fn long_description(&self) -> Option<String> {
            self.long_description.clone()
        }
    }

    impl TestConfigItem {
        /// convert the value to a string using the specified value
        fn convert_to_string(&self, val: &str) -> String {
            let val: String = val.to_string();
            struct Wrapper<'a>(&'a TestConfigItem, &'a String);

            impl<'a> fmt::Display for Wrapper<'a> {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    self.0.display(f, self.1, false)
                }
            }

            format!("{}", Wrapper(self, &val))
        }

        /// convert the value to a verbose string using the specified value
        fn convert_to_verbose_string(&self, val: &str) -> String {
            let val: String = val.to_string();
            struct Wrapper<'a>(&'a TestConfigItem, &'a String);

            impl<'a> fmt::Display for Wrapper<'a> {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    self.0.display(f, self.1, true)
                }
            }

            format!("{}", Wrapper(self, &val))
        }
    }
}
