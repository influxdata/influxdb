#[derive(Debug, Clone, Copy)]
/// A collection of columns to include in query results.
///
/// The `All` variant denotes that the caller wishes to include all table
/// columns in the results.
pub enum Selection<'a> {
    /// Return all columns (e.g. SELECT *)
    /// The columns are returned in an arbitrary order
    All,

    /// Return only the named columns
    Some(&'a [&'a str]),
}
