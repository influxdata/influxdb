use influxdb2_client::RequestError;
use observability_deps::tracing::debug;
use snafu::{OptionExt, ResultExt, Snafu};
use std::{fs::File, io::Read, path::PathBuf};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error reading file {:?}: {}", file_name, source))]
    ReadingFile {
        file_name: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Client error: {source}"))]
    ClientError { source: RequestError },

    #[snafu(display("Invalid namespace '{namespace}': {reason}"))]
    InvalidNamespace { namespace: String, reason: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Write data into the specified database
#[derive(Debug, clap::Parser)]
pub struct Config {
    /// The namespace into which to write
    #[clap(action)]
    namespace: String,

    /// File with data to load. Currently supported formats are .lp
    #[clap(action)]
    file_name: PathBuf,
}

pub async fn command(url: String, config: Config) -> Result<()> {
    let Config {
        namespace,
        file_name,
    } = config;
    let file_name = &file_name;

    let mut file = File::open(file_name).context(ReadingFileSnafu { file_name })?;

    let mut lp_data = String::new();
    file.read_to_string(&mut lp_data)
        .context(ReadingFileSnafu { file_name })?;

    let total_bytes = lp_data.len();

    // split a namespace name ("foo_bar") into org_bucket
    let (org_id, bucket_id) = split_namespace(&namespace)?;

    debug!(url, total_bytes, org_id, bucket_id, "Writing data");

    // IOx's v2 api doesn't validate auth tokens so pass an empty one
    let auth_token = "";
    let client = influxdb2_client::Client::new(url, auth_token);

    client
        .write_line_protocol(org_id, bucket_id, lp_data)
        .await
        .context(ClientSnafu)?;

    println!("{} Bytes OK", total_bytes);

    Ok(())
}

/// Splits up the strings into org_id and bucket_id
fn split_namespace(namespace: &str) -> Result<(&str, &str)> {
    let mut iter = namespace.split('_');
    let org_id = iter.next().context(InvalidNamespaceSnafu {
        namespace,
        reason: "empty",
    })?;

    if org_id.is_empty() {
        return InvalidNamespaceSnafu {
            namespace,
            reason: "No org_id found",
        }
        .fail();
    }

    let bucket_id = iter.next().context(InvalidNamespaceSnafu {
        namespace,
        reason: "Could not find '_'",
    })?;

    if bucket_id.is_empty() {
        return InvalidNamespaceSnafu {
            namespace,
            reason: "No bucket_id found",
        }
        .fail();
    }

    if iter.next().is_some() {
        return InvalidNamespaceSnafu {
            namespace,
            reason: "More than one '_'",
        }
        .fail();
    }

    Ok((org_id, bucket_id))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn split_good() {
        assert_eq!(split_namespace("foo_bar").unwrap(), ("foo", "bar"));
    }

    #[test]
    #[should_panic(expected = "No org_id found")]
    fn split_bad_empty() {
        split_namespace("").unwrap();
    }

    #[test]
    #[should_panic(expected = "No org_id found")]
    fn split_bad_only_underscore() {
        split_namespace("_").unwrap();
    }

    #[test]
    #[should_panic(expected = "No org_id found")]
    fn split_bad_empty_org_id() {
        split_namespace("_ff").unwrap();
    }

    #[test]
    #[should_panic(expected = "No bucket_id found")]
    fn split_bad_empty_bucket_id() {
        split_namespace("ff_").unwrap();
    }

    #[test]
    #[should_panic(expected = "More than one '_'")]
    fn split_too_many() {
        split_namespace("ff_bf_").unwrap();
    }

    #[test]
    #[should_panic(expected = "More than one '_'")]
    fn split_way_too_many() {
        split_namespace("ff_bf_dfd_3_f").unwrap();
    }
}
