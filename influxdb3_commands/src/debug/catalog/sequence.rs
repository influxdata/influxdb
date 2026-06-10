use std::error::Error;

use influxdb3_catalog::format::view::{self, HeaderView, RecordTypeCount, RecordView};
use serde::Serialize;

use super::render;
use super::{CatalogFormat, CatalogStore, CommonArgs};

#[derive(Debug, clap::Parser)]
pub struct Args {
    /// Log sequence number to read. Defaults to the latest log when omitted.
    pub sequence: Option<u64>,
    /// Print a record-type histogram without decoding bodies.
    #[clap(long = "headers-only")]
    pub headers_only: bool,
    /// Skip payload CRC verification to attempt to read a corrupt file.
    #[clap(long = "skip-crc")]
    pub skip_crc: bool,
    #[clap(flatten)]
    pub common: CommonArgs,
}

#[derive(Debug, Clone, Copy)]
pub struct Render {
    pub sequence: Option<u64>,
    pub headers_only: bool,
    pub skip_crc: bool,
}

#[derive(Debug, Serialize)]
struct LogView {
    header: HeaderView,
    records: Vec<RecordView>,
}

#[derive(Debug, Serialize)]
struct LogHeadersView {
    header: HeaderView,
    histogram: Vec<RecordTypeCount>,
}

pub async fn run(args: Args) -> Result<(), Box<dyn Error>> {
    let store = CatalogStore::new(args.common.store()?, args.common.prefix()?);
    let out = render_sequence(
        store,
        args.common.format,
        Render {
            sequence: args.sequence,
            headers_only: args.headers_only,
            skip_crc: args.skip_crc,
        },
    )
    .await?;
    println!("{out}");
    Ok(())
}

pub async fn render_sequence(
    store: CatalogStore,
    format: CatalogFormat,
    opts: Render,
) -> Result<String, Box<dyn Error>> {
    let sequence = match opts.sequence {
        Some(s) => s,
        None => store
            .latest_log_sequence()
            .await?
            .ok_or("no catalog log files present")?,
    };
    let file = store.read_log(sequence, opts.skip_crc).await?;
    let header = view::header_view(&file);

    if opts.headers_only {
        let histogram = view::histogram(&view::all_records(&file));
        let v = LogHeadersView { header, histogram };
        return match format {
            CatalogFormat::Pretty => {
                let mut out = render::structured(&v.header, CatalogFormat::Json)?;
                out.push_str("\n\nRecord types:\n");
                out.push_str(&render::table(
                    &["id", "name", "count"],
                    v.histogram
                        .iter()
                        .map(|h| {
                            vec![
                                h.id.to_string(),
                                h.name.unwrap_or("<unknown>").to_string(),
                                h.count.to_string(),
                            ]
                        })
                        .collect(),
                ));
                Ok(out)
            }
            CatalogFormat::Json => render::structured(&v, format),
        };
    }

    let records = view::record_views(&file);
    render::structured(&LogView { header, records }, format)
}
