use std::sync::Arc;

pub trait CatalogResource: Clone {
    type Identifier;

    /// Static label identifying this kind of resource as a collection
    /// (e.g. "tables", "databases"). Used by [`Repository`] to produce
    /// self-identifying errors.
    ///
    /// [`Repository`]: crate::repository::Repository
    const CATEGORY: &'static str;

    fn id(&self) -> Self::Identifier;
    fn name(&self) -> Arc<str>;
}
