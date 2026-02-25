//! The Distinct Value Cache holds the distinct values for a column or set of columns on a table

mod cache;
pub use cache::{CacheError, CreateDistinctCacheArgs};
mod provider;
pub use provider::{DistinctCacheProvider, ProviderError, background_catalog_update};
mod table_function;
pub use table_function::DISTINCT_CACHE_UDTF_NAME;
pub use table_function::DistinctCacheFunction;

#[cfg(test)]
mod tests;
