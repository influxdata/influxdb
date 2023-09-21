use std::collections::HashSet;

use crate::Schema;

/// Helper that handles [Interning] for [`Schema`]s.
///
/// Note that this is rather expensive since the interner needs to compare the entire schema, so if you find another
/// key to to store your schema (e.g. a table ID), use a `HashMap<K, Arc<Schema>>` instead.
///
/// [Interning]: https://en.wikipedia.org/wiki/Interning_(computer_science)
#[derive(Debug, Default)]
pub struct SchemaInterner {
    schemas: HashSet<Schema>,
}

impl SchemaInterner {
    /// Create new, empty interner.
    pub fn new() -> Self {
        Self::default()
    }

    /// Intern schema.
    pub fn intern(&mut self, schema: Schema) -> Schema {
        if let Some(schema) = self.schemas.get(&schema) {
            schema.clone()
        } else {
            self.schemas.insert(schema.clone());
            schema
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::builder::SchemaBuilder;
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test() {
        let mut interner = SchemaInterner::default();

        let schema_1a = SchemaBuilder::new().tag("t1").tag("t2").build().unwrap();
        let schema_1b = SchemaBuilder::new().tag("t1").tag("t2").build().unwrap();
        let schema_2 = SchemaBuilder::new().tag("t1").tag("t3").build().unwrap();

        let interned_1a = interner.intern(schema_1a.clone());
        assert_eq!(interned_1a, schema_1a);

        let interned_1b = interner.intern(schema_1b);
        assert!(Arc::ptr_eq(interned_1a.inner(), interned_1b.inner()));

        let interned_2 = interner.intern(schema_2.clone());
        assert_eq!(interned_2, schema_2);
    }
}
