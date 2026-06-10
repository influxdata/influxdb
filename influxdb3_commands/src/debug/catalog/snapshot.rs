use std::error::Error;

use influxdb3_catalog::format::view::{self, HeaderView, RecordTypeCount, RecordView};
use serde::Serialize;

use super::render;
use super::{CatalogFormat, CatalogStore, CommonArgs};

#[derive(Debug, clap::Parser)]
pub struct Args {
    /// Decode and print every record body.
    #[clap(long = "full")]
    pub full: bool,
    /// Skip payload CRC verification to attempt to read a corrupt file.
    #[clap(long = "skip-crc")]
    pub skip_crc: bool,
    #[clap(flatten)]
    pub common: CommonArgs,
}

/// Rendering options decoupled from clap for testing.
#[derive(Debug, Clone, Copy)]
pub struct Render {
    pub full: bool,
    pub skip_crc: bool,
}

#[derive(Debug, Serialize)]
struct Summary {
    header: HeaderView,
    histogram: Vec<RecordTypeCount>,
}

#[derive(Debug, Serialize)]
struct Full {
    header: HeaderView,
    records: Vec<RecordView>,
}

pub async fn run(args: Args) -> Result<(), Box<dyn Error>> {
    let store = CatalogStore::new(args.common.store()?, args.common.prefix()?);
    let out = render_snapshot(
        store,
        args.common.format,
        Render {
            full: args.full,
            skip_crc: args.skip_crc,
        },
    )
    .await?;
    println!("{out}");
    Ok(())
}

pub async fn render_snapshot(
    store: CatalogStore,
    format: CatalogFormat,
    opts: Render,
) -> Result<String, Box<dyn Error>> {
    let file = store.read_snapshot(opts.skip_crc).await?;
    let header = view::header_view(&file);

    if opts.full {
        let records = view::record_views(&file);
        return render::structured(&Full { header, records }, format);
    }

    let histogram = view::histogram(&view::all_records(&file));
    let summary = Summary { header, histogram };

    match format {
        CatalogFormat::Pretty => {
            let mut out = render::structured(&summary.header, CatalogFormat::Json)?;
            out.push_str("\n\nRecord types:\n");
            out.push_str(&render::table(
                &["id", "name", "count"],
                summary
                    .histogram
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
        CatalogFormat::Json => render::structured(&summary, format),
    }
}
