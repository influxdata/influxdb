use std::{num::NonZeroUsize, sync::Arc};

use backoff::{Backoff, BackoffConfig};
use futures::StreamExt;
use object_store::DynObjectStore;
use parquet_file::ParquetFilePath;

pub async fn copy_files(
    files_in: &[ParquetFilePath],
    files_out: &[ParquetFilePath],
    from: Arc<DynObjectStore>,
    to: Arc<DynObjectStore>,
    backoff_config: &BackoffConfig,
    concurrency: NonZeroUsize,
) {
    futures::stream::iter(files_in.iter().cloned().zip(files_out.to_vec()))
        .map(|(f_in, f_out)| {
            let backoff_config = backoff_config.clone();
            let from = Arc::clone(&from);
            let to = Arc::clone(&to);
            let path_in = f_in.object_store_path();
            let path_out = f_out.object_store_path();

            async move {
                Backoff::new(&backoff_config)
                    .retry_all_errors("copy file", || async {
                        let bytes = from.get(&path_in).await?.bytes().await?;
                        to.put(&path_out, bytes).await?;
                        Ok::<_, object_store::Error>(())
                    })
                    .await
                    .expect("retry forever")
            }
        })
        .buffer_unordered(concurrency.get())
        .collect::<()>()
        .await;
}

pub async fn delete_files(
    files: &[ParquetFilePath],
    store: Arc<DynObjectStore>,
    backoff_config: &BackoffConfig,
    concurrency: NonZeroUsize,
) {
    // Note: `files.to_vec()` is required to avoid rustc freaking out about lifetimes
    futures::stream::iter(files.to_vec())
        .map(|f| {
            let backoff_config = backoff_config.clone();
            let store = Arc::clone(&store);
            let path = f.object_store_path();

            async move {
                Backoff::new(&backoff_config)
                    .retry_all_errors("delete file", || async { store.delete(&path).await })
                    .await
                    .expect("retry forever")
            }
        })
        .buffer_unordered(concurrency.get())
        .collect::<()>()
        .await;
}
