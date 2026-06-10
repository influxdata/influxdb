use std::error::Error;

use serde::Serialize;

use super::render;
use super::{CatalogFileKind, CatalogFormat, CatalogStore, CommonArgs};

#[derive(Debug, clap::Parser)]
pub struct Args {
    /// Also list log files written before the snapshot.
    #[clap(long = "all")]
    pub all: bool,
    #[clap(flatten)]
    pub common: CommonArgs,
}

#[derive(Debug, Serialize)]
struct FileRow {
    kind: String,
    sequence: Option<u64>,
    path: String,
    size_bytes: u64,
    last_modified: String,
}

#[derive(Debug, Serialize)]
struct Listing {
    files: Vec<FileRow>,
    log_count: usize,
    total_bytes: u64,
}

pub async fn run(args: Args) -> Result<(), Box<dyn Error>> {
    let store = CatalogStore::new(args.common.store()?, args.common.prefix()?);
    let out = render_listing(store, args.common.format, args.all).await?;
    println!("{out}");
    Ok(())
}

pub async fn render_listing(
    store: CatalogStore,
    format: CatalogFormat,
    include_pre_snapshot: bool,
) -> Result<String, Box<dyn Error>> {
    let entries = store.list(include_pre_snapshot).await?;
    let total_bytes: u64 = entries.iter().map(|e| e.size_bytes).sum();
    let log_count = entries
        .iter()
        .filter(|e| e.kind == CatalogFileKind::Log)
        .count();

    let rows: Vec<FileRow> = entries
        .iter()
        .map(|e| FileRow {
            kind: match e.kind {
                CatalogFileKind::Snapshot => "snapshot".to_string(),
                CatalogFileKind::Log => "log".to_string(),
            },
            sequence: e.sequence,
            path: e.path.to_string(),
            size_bytes: e.size_bytes,
            last_modified: e.last_modified.to_rfc3339(),
        })
        .collect();

    match format {
        CatalogFormat::Pretty => {
            let table = render::table(
                &["kind", "sequence", "size_bytes", "last_modified", "path"],
                rows.iter()
                    .map(|r| {
                        vec![
                            r.kind.clone(),
                            r.sequence.map(|s| s.to_string()).unwrap_or_default(),
                            r.size_bytes.to_string(),
                            r.last_modified.clone(),
                            r.path.clone(),
                        ]
                    })
                    .collect(),
            );
            Ok(format!(
                "{table}\n{log_count} log(s), total {total_bytes} bytes"
            ))
        }
        CatalogFormat::Json => render::structured(
            &Listing {
                files: rows,
                log_count,
                total_bytes,
            },
            format,
        ),
    }
}
