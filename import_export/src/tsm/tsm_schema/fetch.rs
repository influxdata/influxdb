use std::sync::Arc;

use futures::prelude::*;
use object_store::{path::Path, DynObjectStore, GetResult};
use thiserror::Error;
use tokio::select;

use crate::tsm::AggregateTSMSchema;

// Possible errors from schema commands
#[derive(Debug, Error)]
pub enum FetchError {
    #[error("Error fetching schemas from object storage: {0}")]
    Fetching(#[from] object_store::Error),

    #[error("Error parsing schema from object storage: {0}")]
    Parsing(#[from] serde_json::Error),
}

pub async fn fetch_schema(
    object_store: Arc<DynObjectStore>,
    prefix: Option<&Path>,
    suffix: &str,
) -> Result<Vec<AggregateTSMSchema>, FetchError> {
    let mut schemas: Vec<AggregateTSMSchema> = vec![];
    let mut results = object_store
        .list(prefix)
        .await
        .map_err(FetchError::Fetching)?;
    // TODO: refactor to do these concurrently using `buffered`
    loop {
        select! {
            item = results.next() => {
                match item {
                    Some(item) => {
                        let item = item.map_err(FetchError::Fetching)?;
                        if !item.location.as_ref().ends_with(suffix) {
                            continue;
                        }
                        let read_stream = object_store.get(&item.location).await?;
                        if let GetResult::Stream(read_stream) = read_stream {
                            let chunks: Vec<_> = read_stream.try_collect().await?;
                            let mut buf = Vec::with_capacity(chunks.iter().map(|c| c.len()).sum::<usize>());
                            for c in chunks {
                                buf.extend(c);
                            }
                            let schema: AggregateTSMSchema = buf.try_into().map_err(FetchError::Parsing)?;
                            schemas.push(schema);
                        }
                    }
                    None => {
                        break;
                    }
                }
            }
        }
    }
    Ok(schemas)
}
