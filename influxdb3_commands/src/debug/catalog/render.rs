use std::error::Error;

use comfy_table::{Cell, Table};
use serde::Serialize;

use super::CatalogFormat;

/// Serialize a value as JSON (pretty).
pub fn structured<T: Serialize>(
    value: &T,
    format: CatalogFormat,
) -> Result<String, Box<dyn Error>> {
    match format {
        CatalogFormat::Json | CatalogFormat::Pretty => Ok(serde_json::to_string_pretty(value)?),
    }
}

/// Build a `comfy-table` from headers and string rows.
pub fn table(headers: &[&str], rows: Vec<Vec<String>>) -> String {
    let mut t = Table::new();
    t.set_header(headers.iter().map(Cell::new));
    for row in rows {
        t.add_row(row.iter().map(Cell::new));
    }
    t.to_string()
}
