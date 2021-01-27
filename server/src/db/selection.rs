use mutable_buffer::chunk::ChunkSelection;
use query::selection::Selection;
use read_buffer::ColumnSelection;
/// Convert a query::Selection into one specific for mutable buffer
pub fn to_mutable_buffer_selection(selection: Selection<'_>) -> ChunkSelection<'_> {
    match selection {
        Selection::All => ChunkSelection::All,
        Selection::Some(cols) => ChunkSelection::Some(cols),
    }
}

/// Convert a query::Selection into one specific for read buffer
pub fn to_read_buffer_selection(selection: Selection<'_>) -> ColumnSelection<'_> {
    match selection {
        Selection::All => ColumnSelection::All,
        Selection::Some(cols) => ColumnSelection::Some(cols),
    }
}
