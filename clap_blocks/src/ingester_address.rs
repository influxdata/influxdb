//! Shared configuration and tests for accepting ingester addresses as arguments.

use http::uri::{InvalidUri, InvalidUriParts, Uri};
use snafu::Snafu;
use std::{fmt::Display, str::FromStr};

/// An address to an ingester's gRPC API. Create by using `IngesterAddress::from_str`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngesterAddress {
    uri: Uri,
}

/// Why a specified ingester address might be invalid
#[allow(missing_docs)]
#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(context(false))]
    Invalid { source: InvalidUri },

    #[snafu(display("Port is required; no port found in `{value}`"))]
    MissingPort { value: String },

    #[snafu(context(false))]
    InvalidParts { source: InvalidUriParts },
}

impl FromStr for IngesterAddress {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let uri = Uri::from_str(s)?;

        if uri.port().is_none() {
            return MissingPortSnafu { value: s }.fail();
        }

        let uri = if uri.scheme().is_none() {
            Uri::from_str(&format!("http://{s}"))?
        } else {
            uri
        };

        Ok(Self { uri })
    }
}

impl Display for IngesterAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.uri)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{error::ErrorKind, Parser};
    use std::env;
    use test_helpers::{assert_contains, assert_error};

    /// Applications such as the router MUST have valid ingester addresses.
    #[derive(Debug, Clone, clap::Parser)]
    struct RouterConfig {
        #[clap(
            long = "ingester-addresses",
            env = "TEST_INFLUXDB_IOX_INGESTER_ADDRESSES",
            required = true,
            num_args=1..,
            value_delimiter = ','
        )]
        pub ingester_addresses: Vec<IngesterAddress>,
    }

    #[test]
    fn error_if_not_specified_when_required() {
        assert_error!(
            RouterConfig::try_parse_from(["my_binary"]),
            ref e if e.kind() == ErrorKind::MissingRequiredArgument
        );
    }

    /// Applications such as the querier might not have any ingester addresses, but if they have
    /// any, they should be valid.
    #[derive(Debug, Clone, clap::Parser)]
    struct QuerierConfig {
        #[clap(
            long = "ingester-addresses",
            env = "TEST_INFLUXDB_IOX_INGESTER_ADDRESSES",
            required = false,
            num_args=0..,
            value_delimiter = ','
        )]
        pub ingester_addresses: Vec<IngesterAddress>,
    }

    #[test]
    fn empty_if_not_specified_when_optional() {
        assert!(QuerierConfig::try_parse_from(["my_binary"])
            .unwrap()
            .ingester_addresses
            .is_empty());
    }

    fn both_types_valid(args: &[&'static str], expected: &[&'static str]) {
        let router = RouterConfig::try_parse_from(args).unwrap();
        let actual: Vec<_> = router
            .ingester_addresses
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(actual, expected);

        let querier = QuerierConfig::try_parse_from(args).unwrap();
        let actual: Vec<_> = querier
            .ingester_addresses
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(actual, expected);
    }

    fn both_types_error(args: &[&'static str], expected_error_message: &'static str) {
        assert_contains!(
            RouterConfig::try_parse_from(args).unwrap_err().to_string(),
            expected_error_message
        );
        assert_contains!(
            QuerierConfig::try_parse_from(args).unwrap_err().to_string(),
            expected_error_message
        );
    }

    #[test]
    fn accepts_one() {
        let args = [
            "my_binary",
            "--ingester-addresses",
            "http://example.com:1234",
        ];
        let expected = ["http://example.com:1234/"];

        both_types_valid(&args, &expected);
    }

    #[test]
    fn accepts_two() {
        let args = [
            "my_binary",
            "--ingester-addresses",
            "http://example.com:1234,http://example.com:5678",
        ];
        let expected = ["http://example.com:1234/", "http://example.com:5678/"];

        both_types_valid(&args, &expected);
    }

    #[test]
    fn rejects_any_invalid_uri() {
        let args = [
            "my_binary",
            "--ingester-addresses",
            "http://example.com:1234,", // note the trailing comma; empty URIs are invalid
        ];
        let expected = "error: invalid value '' for '--ingester-addresses";

        both_types_error(&args, expected);
    }

    #[test]
    fn rejects_uri_without_port() {
        let args = [
            "my_binary",
            "--ingester-addresses",
            "example.com,http://example.com:1234",
        ];
        let expected = "Port is required; no port found in `example.com`";

        both_types_error(&args, expected);
    }

    #[test]
    fn no_scheme_assumes_http() {
        let args = [
            "my_binary",
            "--ingester-addresses",
            "http://example.com:1234,somescheme://0.0.0.0:1000,127.0.0.1:8080",
        ];
        let expected = [
            "http://example.com:1234/",
            "somescheme://0.0.0.0:1000/",
            "http://127.0.0.1:8080/",
        ];

        both_types_valid(&args, &expected);
    }

    #[test]
    fn specifying_flag_multiple_times_works() {
        let args = [
            "my_binary",
            "--ingester-addresses",
            "http://example.com:1234",
            "--ingester-addresses",
            "somescheme://0.0.0.0:1000",
            "--ingester-addresses",
            "127.0.0.1:8080",
        ];
        let expected = [
            "http://example.com:1234/",
            "somescheme://0.0.0.0:1000/",
            "http://127.0.0.1:8080/",
        ];

        both_types_valid(&args, &expected);
    }

    #[test]
    fn specifying_flag_multiple_times_and_using_commas_works() {
        let args = [
            "my_binary",
            "--ingester-addresses",
            "http://example.com:1234",
            "--ingester-addresses",
            "somescheme://0.0.0.0:1000,127.0.0.1:8080",
        ];
        let expected = [
            "http://example.com:1234/",
            "somescheme://0.0.0.0:1000/",
            "http://127.0.0.1:8080/",
        ];

        both_types_valid(&args, &expected);
    }

    /// Use an environment variable name not shared with any other config to avoid conflicts when
    /// setting the var in tests.
    /// Applications such as the router MUST have valid ingester addresses.
    #[derive(Debug, Clone, clap::Parser)]
    struct EnvRouterConfig {
        #[clap(
            long = "ingester-addresses",
            env = "NO_CONFLICT_ROUTER_TEST_INFLUXDB_IOX_INGESTER_ADDRESSES",
            required = true,
            num_args=1..,
            value_delimiter = ','
        )]
        pub ingester_addresses: Vec<IngesterAddress>,
    }

    #[test]
    fn required_and_specified_via_environment_variable() {
        env::set_var(
            "NO_CONFLICT_ROUTER_TEST_INFLUXDB_IOX_INGESTER_ADDRESSES",
            "http://example.com:1234,somescheme://0.0.0.0:1000,127.0.0.1:8080",
        );
        let args = ["my_binary"];
        let expected = [
            "http://example.com:1234/",
            "somescheme://0.0.0.0:1000/",
            "http://127.0.0.1:8080/",
        ];

        let router = EnvRouterConfig::try_parse_from(args).unwrap();
        let actual: Vec<_> = router
            .ingester_addresses
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(actual, expected);
    }

    /// Use an environment variable name not shared with any other config to avoid conflicts when
    /// setting the var in tests.
    /// Applications such as the querier might not have any ingester addresses, but if they have
    /// any, they should be valid.
    #[derive(Debug, Clone, clap::Parser)]
    struct EnvQuerierConfig {
        #[clap(
            long = "ingester-addresses",
            env = "NO_CONFLICT_QUERIER_TEST_INFLUXDB_IOX_INGESTER_ADDRESSES",
            required = false,
            num_args=0..,
            value_delimiter = ','
        )]
        pub ingester_addresses: Vec<IngesterAddress>,
    }

    #[test]
    fn optional_and_specified_via_environment_variable() {
        env::set_var(
            "NO_CONFLICT_QUERIER_TEST_INFLUXDB_IOX_INGESTER_ADDRESSES",
            "http://example.com:1234,somescheme://0.0.0.0:1000,127.0.0.1:8080",
        );
        let args = ["my_binary"];
        let expected = [
            "http://example.com:1234/",
            "somescheme://0.0.0.0:1000/",
            "http://127.0.0.1:8080/",
        ];

        let querier = EnvQuerierConfig::try_parse_from(args).unwrap();
        let actual: Vec<_> = querier
            .ingester_addresses
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(actual, expected);
    }
}
