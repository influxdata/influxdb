#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop
)]

//! Utility to seed a running delorean instance with data for development and testing purposes.
//!
//! Similar to `inch`.
//!
//! # Usage
//!
//! ```
//! cargo run --bin seed -- -o [ORG] -b [BUCKET] < line-protocol.txt
//! ```
//!
//! Or in conjunction with `generate`:
//!
//! ```
//! cargo run --bin generate | cargo run --bin seed -- -o [ORG] -b [BUCKET]
//! ```

use std::io::{self, Read};
use structopt::StructOpt;

const URL_BASE: &str = "http://localhost:8080/api/v2";

#[derive(StructOpt, Debug)]
#[structopt(name = "seed")]
struct CliOptions {
    #[structopt(short = "o", long)]
    org: String,

    #[structopt(short = "b", long)]
    bucket: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli_options = CliOptions::from_args();
    let org = cli_options.org;
    let bucket = cli_options.bucket;

    let client = reqwest::Client::new();

    // Create a bucket
    let url = format!("{}{}", URL_BASE, "/create_bucket");
    client
        .post(&url)
        .form(&[("bucket", &bucket), ("org", &org)])
        .send()
        .await?
        .error_for_status()?;

    // Read data from stdin
    let mut data = String::new();
    let stdin = io::stdin();
    let mut stdin_handle = stdin.lock();
    stdin_handle.read_to_string(&mut data).unwrap();

    // Write data to delorean
    write_data(&client, "/write", &org, &bucket, data).await?;

    Ok(())
}

async fn write_data(
    client: &reqwest::Client,
    path: &str,
    org_id: &str,
    bucket_id: &str,
    body: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{}{}", URL_BASE, path);
    client
        .post(&url)
        .query(&[("bucket", bucket_id), ("org", org_id)])
        .body(body)
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}
