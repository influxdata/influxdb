use std::sync::Arc;

pub trait CatalogResource: Clone {
    type Identifier;

    fn id(&self) -> Self::Identifier;
    fn name(&self) -> Arc<str>;
}
