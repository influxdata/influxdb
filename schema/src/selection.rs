use std::{collections::HashMap, fmt::Display, sync::Arc};

/// A collection of columns to include in query results.
///
/// The `All` variant denotes that the caller wishes to include all table
/// columns in the results.
///
/// # Owned
/// There is also [`OwnedSelection`] which does NOT borrow the column names.
#[derive(Debug, Clone, Copy)]
pub enum Selection<'a> {
    /// Return all columns (e.g. SELECT *)
    /// The columns are returned in an arbitrary order
    All,

    /// Return only the named columns
    Some(&'a [&'a str]),
}

impl<'a> Display for Selection<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Selection::All => write!(f, "*")?,
            Selection::Some(cols) => {
                for (i, col) in cols.iter().enumerate() {
                    write!(f, "{}", col)?;
                    if i < cols.len() - 1 {
                        write!(f, ",")?;
                    }
                }
            }
        }
        Ok(())
    }
}

/// Compute projected schema.
pub fn select_schema(
    selection: Selection<'_>,
    schema: &arrow::datatypes::Schema,
) -> arrow::datatypes::SchemaRef {
    // Indices of columns in the schema needed to read
    let projection: Vec<usize> = match selection {
        Selection::Some(cols) => {
            let fields_lookup: HashMap<_, _> = schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .enumerate()
                .map(|(v, k)| (k, v))
                .collect();
            cols.iter()
                .filter_map(|c| fields_lookup.get(c).cloned())
                .collect()
        }
        Selection::All => (0..schema.fields().len()).collect(),
    };

    // Compute final (output) schema after selection
    Arc::new(arrow::datatypes::Schema::new_with_metadata(
        projection
            .iter()
            .map(|i| schema.field(*i).clone())
            .collect(),
        schema.metadata().clone(),
    ))
}

/// A way to "own" a [`Selection`].
///
/// # Example
/// ```
/// use schema::selection::{
///     HalfOwnedSelection,
///     OwnedSelection,
///     Selection,
/// };
///
/// let columns = vec!["foo", "bar"];
///
/// // Selection borrows columns strings and the containing slice
/// let selection: Selection<'_> = Selection::Some(&columns);
///
/// // OwnedSelection does NOT borrow at all
/// let owned: OwnedSelection = selection.into();
///
/// // To convert OwnedSelection back to Selection, we need an intermediate step.
/// let half_owned: HalfOwnedSelection<'_> = (&owned).into();
/// let selection: Selection<'_> = (&half_owned).into();
/// ```
#[derive(Debug, Clone)]
pub enum OwnedSelection {
    /// Return all columns (e.g. SELECT *)
    /// The columns are returned in an arbitrary order
    All,

    /// Return only the named columns
    Some(Vec<String>),
}

/// Helper to convert an [`OwnedSelection`] into a [`Selection`].
#[derive(Debug, Clone)]
pub enum HalfOwnedSelection<'a> {
    /// Return all columns (e.g. SELECT *)
    /// The columns are returned in an arbitrary order
    All,

    /// Return only the named columns
    Some(Vec<&'a str>),
}

impl From<Selection<'_>> for OwnedSelection {
    fn from(selection: Selection<'_>) -> Self {
        match selection {
            Selection::All => Self::All,
            Selection::Some(cols) => Self::Some(cols.iter().map(|s| (*s).to_owned()).collect()),
        }
    }
}

impl<'a> From<&'a OwnedSelection> for HalfOwnedSelection<'a> {
    fn from(selection: &'a OwnedSelection) -> Self {
        match selection {
            OwnedSelection::All => Self::All,
            OwnedSelection::Some(cols) => Self::Some(cols.iter().map(|s| s.as_str()).collect()),
        }
    }
}

impl<'a> From<&'a HalfOwnedSelection<'a>> for Selection<'a> {
    fn from(selection: &'a HalfOwnedSelection<'a>) -> Self {
        match selection {
            HalfOwnedSelection::All => Self::All,
            HalfOwnedSelection::Some(cols) => Self::Some(cols.as_slice()),
        }
    }
}

#[cfg(test)]
mod test_super {
    use super::*;

    #[test]
    fn test_selection_display() {
        let selections = vec![
            (Selection::All, "*"),
            (Selection::Some(&["env"]), "env"),
            (Selection::Some(&["env", "region"]), "env,region"),
        ];

        for (selection, exp) in selections {
            assert_eq!(format!("{}", selection).as_str(), exp);
        }
    }
}
