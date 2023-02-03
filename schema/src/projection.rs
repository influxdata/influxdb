use std::{collections::HashMap, fmt::Display, sync::Arc};

/// A collection of columns to include in query results.
///
/// The `All` variant denotes that the caller wishes to include all table
/// columns in the results.
///
#[derive(Debug, Clone, Copy)]
pub enum Projection<'a> {
    /// Return all columns (e.g. SELECT *)
    /// The columns are returned in an arbitrary order
    All,

    /// Return only the named columns
    Some(&'a [&'a str]),
}

impl<'a> Display for Projection<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Projection::All => write!(f, "*")?,
            Projection::Some(cols) => {
                for (i, col) in cols.iter().enumerate() {
                    write!(f, "{col}")?;
                    if i < cols.len() - 1 {
                        write!(f, ",")?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl<'a> Projection<'a> {
    /// Compute projected schema.
    pub fn project_schema(
        &self,
        schema: &arrow::datatypes::SchemaRef,
    ) -> arrow::datatypes::SchemaRef {
        match self {
            Projection::Some(cols) => {
                let fields_lookup: HashMap<_, _> = schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .enumerate()
                    .map(|(v, k)| (k, v))
                    .collect();

                // Indices of columns in the schema needed to read
                let projection: Vec<usize> = cols
                    .iter()
                    .filter_map(|c| fields_lookup.get(c).cloned())
                    .collect();

                // try NOT to create yet another schema if this is basically a "select all"
                if (projection.len() == schema.fields().len())
                    && projection.iter().enumerate().all(|(a, b)| a == *b)
                {
                    return Arc::clone(schema);
                }

                // Compute final (output) schema after selection
                Arc::new(schema.project(&projection).expect("projection bug"))
            }
            Projection::All => Arc::clone(schema),
        }
    }
}

#[cfg(test)]
mod test_super {
    use super::*;

    #[test]
    fn test_selection_display() {
        let selections = vec![
            (Projection::All, "*"),
            (Projection::Some(&["env"]), "env"),
            (Projection::Some(&["env", "region"]), "env,region"),
        ];

        for (selection, exp) in selections {
            assert_eq!(format!("{selection}").as_str(), exp);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::builder::SchemaBuilder;

    use super::*;

    #[test]
    fn test_select_schema_all() {
        let schema = SchemaBuilder::new()
            .tag("t1")
            .tag("t2")
            .timestamp()
            .build()
            .unwrap()
            .as_arrow();
        let actual = Projection::All.project_schema(&schema);
        assert!(Arc::ptr_eq(&schema, &actual));
    }

    #[test]
    fn test_select_schema_some() {
        let schema = SchemaBuilder::new()
            .tag("t1")
            .tag("t2")
            .timestamp()
            .build()
            .unwrap()
            .as_arrow();

        // normal selection
        let actual = Projection::Some(&["time", "t2"]).project_schema(&schema);
        let expected = schema.project(&[2, 1]).unwrap();
        assert!(!Arc::ptr_eq(&schema, &actual));
        assert_eq!(actual.as_ref(), &expected);

        // select unknown columns
        let actual = Projection::Some(&["time", "t3", "t2"]).project_schema(&schema);
        let expected = schema.project(&[2, 1]).unwrap();
        assert!(!Arc::ptr_eq(&schema, &actual));
        assert_eq!(actual.as_ref(), &expected);

        // "hidden" all
        let actual = Projection::Some(&["t1", "t2", "time"]).project_schema(&schema);
        assert!(Arc::ptr_eq(&schema, &actual));
    }
}
