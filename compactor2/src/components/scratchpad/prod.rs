use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Display,
    num::NonZeroUsize,
    sync::Arc,
};

use async_trait::async_trait;
use backoff::BackoffConfig;
use object_store::DynObjectStore;
use observability_deps::tracing::warn;
use parquet_file::ParquetFilePath;
use uuid::Uuid;

use super::{
    util::{copy_files, delete_files},
    Scratchpad, ScratchpadGen,
};

#[derive(Debug)]
pub struct ProdScratchpadGen {
    concurrency: NonZeroUsize,
    backoff_config: BackoffConfig,
    store_input: Arc<DynObjectStore>,
    store_scratchpad: Arc<DynObjectStore>,
    store_output: Arc<DynObjectStore>,
}

impl ProdScratchpadGen {
    pub fn new(
        concurrency: NonZeroUsize,
        backoff_config: BackoffConfig,
        store_input: Arc<DynObjectStore>,
        store_scratchpad: Arc<DynObjectStore>,
        store_output: Arc<DynObjectStore>,
    ) -> Self {
        Self {
            concurrency,
            backoff_config,
            store_input,
            store_scratchpad,
            store_output,
        }
    }
}

impl Display for ProdScratchpadGen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "prod")
    }
}

impl ScratchpadGen for ProdScratchpadGen {
    fn pad(&self) -> Box<dyn Scratchpad> {
        Box::new(ProdScratchpad {
            concurrency: self.concurrency,
            backoff_config: self.backoff_config.clone(),
            store_input: Arc::clone(&self.store_input),
            store_scratchpad: Arc::clone(&self.store_scratchpad),
            store_output: Arc::clone(&self.store_output),
            mask: Uuid::new_v4(),
            files_unmasked: HashMap::default(),
        })
    }
}

#[derive(Debug)]
struct ProdScratchpad {
    concurrency: NonZeroUsize,
    backoff_config: BackoffConfig,
    store_input: Arc<DynObjectStore>,
    store_scratchpad: Arc<DynObjectStore>,
    store_output: Arc<DynObjectStore>,
    mask: Uuid,

    /// Set of known, unmasked file.
    ///
    /// If the file is part of this map, it is in the scratchpad. If the boolean key is set, it was already copied to
    /// the output store
    files_unmasked: HashMap<ParquetFilePath, bool>,
}

impl ProdScratchpad {
    fn apply_mask(&self, files: &[ParquetFilePath]) -> (Vec<ParquetFilePath>, Vec<Uuid>) {
        files
            .iter()
            .map(|f| {
                let uuid = Self::xor_uuids(f.objest_store_id(), self.mask);
                let f = (*f).with_object_store_id(uuid);
                (f, uuid)
            })
            .unzip()
    }

    fn xor_uuids(a: Uuid, b: Uuid) -> Uuid {
        Uuid::from_u128(a.as_u128() ^ b.as_u128())
    }

    fn check_known(
        &mut self,
        files_unmasked: &[ParquetFilePath],
        files_masked: &[ParquetFilePath],
        output: bool,
    ) -> (Vec<ParquetFilePath>, Vec<ParquetFilePath>) {
        files_unmasked
            .iter()
            .zip(files_masked)
            .filter(
                |(f_unmasked, _f_masked)| match self.files_unmasked.entry(**f_unmasked) {
                    Entry::Occupied(mut o) => {
                        let old_var = *o.get();
                        *o.get_mut() |= output;
                        output && !old_var
                    }
                    Entry::Vacant(v) => {
                        v.insert(output);
                        true
                    }
                },
            )
            .unzip()
    }
}

impl Drop for ProdScratchpad {
    fn drop(&mut self) {
        if !self.files_unmasked.is_empty() {
            warn!("scratchpad context not cleaned, may leak resources");

            // clean up eventually
            // Note: Use manual clean up code and do not create yet-another ProdScratchpad to avoid infinite recursions
            //       during drop.
            let files = self
                .files_unmasked
                .drain()
                .map(|(k, _in_out)| k)
                .collect::<Vec<_>>();
            let (files_masked, _uuids) = self.apply_mask(&files);
            let store_scratchpad = Arc::clone(&self.store_scratchpad);
            let concurrency = self.concurrency;
            let backoff_config = self.backoff_config.clone();
            tokio::spawn(async move {
                delete_files(
                    &files_masked,
                    Arc::clone(&store_scratchpad),
                    &backoff_config,
                    concurrency,
                )
                .await;
            });
        }
    }
}

#[async_trait]
impl Scratchpad for ProdScratchpad {
    async fn load_to_scratchpad(&mut self, files: &[ParquetFilePath]) -> Vec<Uuid> {
        let (files_to, uuids) = self.apply_mask(files);
        let (files_from, files_to) = self.check_known(files, &files_to, false);
        copy_files(
            &files_from,
            &files_to,
            Arc::clone(&self.store_input),
            Arc::clone(&self.store_scratchpad),
            &self.backoff_config,
            self.concurrency,
        )
        .await;
        uuids
    }

    async fn make_public(&mut self, files: &[ParquetFilePath]) -> Vec<Uuid> {
        let (files_to, uuids) = self.apply_mask(files);

        // only keep files that we did not know about, all others we've already synced it between the two stores
        let (files_to, files_from) = self.check_known(&files_to, files, true);

        copy_files(
            &files_from,
            &files_to,
            Arc::clone(&self.store_scratchpad),
            Arc::clone(&self.store_output),
            &self.backoff_config,
            self.concurrency,
        )
        .await;
        uuids
    }

    async fn clean_from_scratchpad(&mut self, files: &[ParquetFilePath]) {
        let files = files
            .iter()
            .filter(|f| self.files_unmasked.remove(f).is_some())
            .cloned()
            .collect::<Vec<_>>();
        let (files_masked, _uuids) = self.apply_mask(&files);
        delete_files(
            &files_masked,
            Arc::clone(&self.store_scratchpad),
            &self.backoff_config,
            self.concurrency,
        )
        .await;
    }

    async fn clean(&mut self) {
        let files: Vec<_> = self.files_unmasked.keys().cloned().collect();
        self.clean_from_scratchpad(&files).await;
        self.files_unmasked.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use test_helpers::{maybe_start_logging, tracing::TracingCapture};

    use crate::{
        components::scratchpad::test_util::{assert_content, file_path, stores},
        test_util::list_object_store,
    };

    use super::*;

    #[test]
    fn test_display() {
        let (store_input, store_scratchpad, store_output) = stores();
        let gen = ProdScratchpadGen::new(
            NonZeroUsize::new(1).unwrap(),
            BackoffConfig::default(),
            store_input,
            store_scratchpad,
            store_output,
        );
        assert_eq!(gen.to_string(), "prod");
    }

    #[tokio::test]
    async fn test_staging() {
        maybe_start_logging();

        let (store_input, store_scratchpad, store_output) = stores();
        let gen = ProdScratchpadGen::new(
            NonZeroUsize::new(1).unwrap(),
            BackoffConfig::default(),
            Arc::clone(&store_input),
            Arc::clone(&store_scratchpad),
            Arc::clone(&store_output),
        );
        let mut pad = gen.pad();

        let f1 = file_path(1);
        let f2 = file_path(2);
        let f3 = file_path(3);
        let f4 = file_path(4);
        let f5_masked = file_path(5);
        let f6_masked = file_path(6);
        let f7_masked = file_path(7);

        for f in [&f1, &f2, &f3, &f4] {
            store_input
                .put(&f.object_store_path(), vec![].into())
                .await
                .unwrap();
        }

        assert_content(&store_input, [&f1, &f2, &f3, &f4]).await;
        assert_content(&store_scratchpad, []).await;
        assert_content(&store_output, []).await;

        let uuids = pad.load_to_scratchpad(&[f1, f2]).await;
        assert_eq!(uuids.len(), 2);
        let f1_masked = f1.with_object_store_id(uuids[0]);
        let f2_masked = f2.with_object_store_id(uuids[1]);

        assert_content(&store_input, [&f1, &f2, &f3, &f4]).await;
        assert_content(&store_scratchpad, [&f1_masked, &f2_masked]).await;
        assert_content(&store_output, []).await;

        let uuids = pad.load_to_scratchpad(&[f2, f3]).await;
        assert_eq!(uuids.len(), 2);
        assert_eq!(f2_masked.objest_store_id(), uuids[0]);
        let f3_masked = f3.with_object_store_id(uuids[1]);

        assert_content(&store_input, [&f1, &f2, &f3, &f4]).await;
        assert_content(&store_scratchpad, [&f1_masked, &f2_masked, &f3_masked]).await;
        assert_content(&store_output, []).await;

        for f in [&f5_masked, &f6_masked, &f7_masked] {
            store_scratchpad
                .put(&f.object_store_path(), vec![].into())
                .await
                .unwrap();
        }

        assert_content(&store_input, [&f1, &f2, &f3, &f4]).await;
        assert_content(
            &store_scratchpad,
            [
                &f1_masked, &f2_masked, &f3_masked, &f5_masked, &f6_masked, &f7_masked,
            ],
        )
        .await;
        assert_content(&store_output, []).await;

        let uuids = pad.make_public(&[f5_masked, f6_masked]).await;
        assert_eq!(uuids.len(), 2);
        let f5 = f5_masked.with_object_store_id(uuids[0]);
        let f6 = f6_masked.with_object_store_id(uuids[1]);

        assert_content(&store_input, [&f1, &f2, &f3, &f4]).await;
        assert_content(
            &store_scratchpad,
            [
                &f1_masked, &f2_masked, &f3_masked, &f5_masked, &f6_masked, &f7_masked,
            ],
        )
        .await;
        assert_content(&store_output, [&f5, &f6]).await;

        let uuids = pad.make_public(&[f1_masked]).await;
        assert_eq!(uuids.len(), 1);
        assert_eq!(f1.objest_store_id(), uuids[0]);

        assert_content(&store_input, [&f1, &f2, &f3, &f4]).await;
        assert_content(
            &store_scratchpad,
            [
                &f1_masked, &f2_masked, &f3_masked, &f5_masked, &f6_masked, &f7_masked,
            ],
        )
        .await;
        assert_content(&store_output, [&f1, &f5, &f6]).await;

        pad.clean_from_scratchpad(&[f1, f5]).await;

        assert_content(&store_input, [&f1, &f2, &f3, &f4]).await;
        assert_content(
            &store_scratchpad,
            [&f2_masked, &f3_masked, &f6_masked, &f7_masked],
        )
        .await;
        assert_content(&store_output, [&f1, &f5, &f6]).await;

        pad.clean().await;

        assert_content(&store_input, [&f1, &f2, &f3, &f4]).await;
        assert_content(&store_scratchpad, [&f7_masked]).await; // pad didn't know about these files
        assert_content(&store_output, [&f1, &f5, &f6]).await;
    }

    #[tokio::test]
    async fn test_collision() {
        let (store_input, store_scratchpad, store_output) = stores();
        let gen = ProdScratchpadGen::new(
            NonZeroUsize::new(1).unwrap(),
            BackoffConfig::default(),
            Arc::clone(&store_input),
            Arc::clone(&store_scratchpad),
            Arc::clone(&store_output),
        );

        let mut pad1 = gen.pad();
        let mut pad2 = gen.pad();

        let f = file_path(1);

        store_input
            .put(&f.object_store_path(), Default::default())
            .await
            .unwrap();

        let uuids = pad1.load_to_scratchpad(&[f]).await;
        assert_eq!(uuids.len(), 1);
        let f_masked1 = f.with_object_store_id(uuids[0]);

        let uuids = pad2.load_to_scratchpad(&[f]).await;
        assert_eq!(uuids.len(), 1);
        let f_masked2 = f.with_object_store_id(uuids[0]);

        assert_content(&store_scratchpad, [&f_masked1, &f_masked2]).await;

        pad2.clean().await;

        assert_content(&store_scratchpad, [&f_masked1]).await;
    }

    #[tokio::test]
    async fn test_clean_on_drop() {
        let (store_input, store_scratchpad, store_output) = stores();
        let gen = ProdScratchpadGen::new(
            NonZeroUsize::new(1).unwrap(),
            BackoffConfig::default(),
            Arc::clone(&store_input),
            Arc::clone(&store_scratchpad),
            Arc::clone(&store_output),
        );
        let mut pad = gen.pad();

        let f = file_path(1);

        store_input
            .put(&f.object_store_path(), Default::default())
            .await
            .unwrap();

        pad.load_to_scratchpad(&[f]).await;

        let capture = TracingCapture::new();

        drop(pad);

        // warning emitted
        assert_eq!(
            capture.to_string(),
            "level = WARN; message = scratchpad context not cleaned, may leak resources; "
        );

        // eventually cleaned up
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if list_object_store(&store_scratchpad).await.is_empty() {
                    return;
                }

                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("no timeout");
    }

    #[tokio::test]
    #[should_panic(expected = "foo")]
    async fn test_clean_does_not_crash_on_panic() {
        let (store_input, store_scratchpad, store_output) = stores();
        let gen = ProdScratchpadGen::new(
            NonZeroUsize::new(1).unwrap(),
            BackoffConfig::default(),
            Arc::clone(&store_input),
            Arc::clone(&store_scratchpad),
            Arc::clone(&store_output),
        );
        let mut pad = gen.pad();

        let f = file_path(1);

        store_input
            .put(&f.object_store_path(), Default::default())
            .await
            .unwrap();

        pad.load_to_scratchpad(&[f]).await;

        panic!("foo");
    }
}
