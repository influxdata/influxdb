use std::{collections::HashSet, sync::Arc};

use crate::Schema;

/// Helper that handles [Interning] for [`Schema`]s.
///
/// Note that this is rather expensive since the interner needs to compare the entire schema, so if you find another
/// key to to store your schema (e.g. a table ID), use a `HashMap<K, Arc<Schema>>` instead.
///
/// [Interning]: https://en.wikipedia.org/wiki/Interning_(computer_science)
#[derive(Debug, Default)]
pub struct SchemaInterner {
    schemas: HashSet<Arc<Schema>>,
}

impl SchemaInterner {
    /// Create new, empty interner.
    pub fn new() -> Self {
        Self::default()
    }

    /// Intern schema.
    pub fn intern(&mut self, schema: Schema) -> Arc<Schema> {
        if let Some(schema) = self.schemas.get(&schema) {
            Arc::clone(schema)
        } else {
            let schema = Arc::new(schema);
            self.schemas.insert(Arc::clone(&schema));
            schema
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::builder::SchemaBuilder;

    use super::*;

    #[test]
    fn test() {
        let mut interner = SchemaInterner::default();

        let schema_1a = SchemaBuilder::new().tag("t1").tag("t2").build().unwrap();
        let schema_1b = SchemaBuilder::new().tag("t1").tag("t2").build().unwrap();
        let schema_2 = SchemaBuilder::new().tag("t1").tag("t3").build().unwrap();

        let interned_1a = interner.intern(schema_1a.clone());
        assert_eq!(interned_1a.as_ref(), &schema_1a);

        let interned_1b = interner.intern(schema_1b);
        assert!(Arc::ptr_eq(&interned_1a, &interned_1b));

        let interned_2 = interner.intern(schema_2.clone());
        assert_eq!(interned_2.as_ref(), &schema_2);
    }
}
