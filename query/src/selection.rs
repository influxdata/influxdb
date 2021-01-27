#[derive(Debug)]
/// This represents the request to select set of columns from a table
pub enum Selection<'a> {
    /// Return all columns (e.g. SELECT *)
    All,

    /// Return only the named columns
    Some(&'a [&'a str]),
}
