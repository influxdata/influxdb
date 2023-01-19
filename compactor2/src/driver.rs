use std::sync::Arc;

use futures::StreamExt;
use observability_deps::tracing::info;

use crate::{compact::compact_files, components::Components, config::Config};

pub async fn compact(config: &Config, components: &Arc<Components>) {
    let partition_ids = components.partitions_source.fetch().await;

    futures::stream::iter(partition_ids)
        .map(|partition_id| {
            let config = config.clone();
            let components = Arc::clone(components);

            async move {
                let files = components.partition_files_source.fetch(partition_id).await;
                let files = components.files_filter.apply(files);

                if !components.partition_filter.apply(&files) {
                    return;
                }

                match compact_files(&files, &config.catalog).await {
                    Ok(create) => {
                        let delete_ids = files.iter().map(|f| f.id).collect::<Vec<_>>();
                        components.commit.commit(&delete_ids, &create).await;
                    }
                    // TODO: remove this stop-gap
                    Err(crate::compact::Error::NotImplemented) => {
                        info!("not implemented");
                    }
                    #[allow(unreachable_patterns)] // TODO: remove this when stop-gap is removed
                    Err(e) => {
                        components
                            .partition_error_sink
                            .record(partition_id, &e.to_string())
                            .await;
                    }
                }
            }
        })
        .buffer_unordered(config.partition_concurrency.get())
        .collect::<()>()
        .await;
}
