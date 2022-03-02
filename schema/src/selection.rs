use std::fmt::Display;

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
