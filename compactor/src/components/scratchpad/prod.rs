use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Display,
    num::NonZeroUsize,
    sync::{Arc, RwLock},
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
    shadow_mode: bool,
    backoff_config: BackoffConfig,
    store_input: Arc<DynObjectStore>,
    store_scratchpad: Arc<DynObjectStore>,
    store_output: Arc<DynObjectStore>,
}

impl ProdScratchpadGen {
    pub fn new(
        shadow_mode: bool,
        concurrency: NonZeroUsize,
        backoff_config: BackoffConfig,
        store_input: Arc<DynObjectStore>,
        store_scratchpad: Arc<DynObjectStore>,
        store_output: Arc<DynObjectStore>,
    ) -> Self {
        Self {
            shadow_mode,
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

/// ScratchpadGen is the factory pattern; it creates Scratchpads
impl ScratchpadGen for ProdScratchpadGen {
    fn pad(&self) -> Arc<dyn Scratchpad> {
        Arc::new(ProdScratchpad {
            shadow_mode: self.shadow_mode,
            concurrency: self.concurrency,
            backoff_config: self.backoff_config.clone(),
            store_input: Arc::clone(&self.store_input),
            store_scratchpad: Arc::clone(&self.store_scratchpad),
            store_output: Arc::clone(&self.store_output),
            mask: Uuid::new_v4(),
            files_unmasked: RwLock::new(HashMap::default()),
        })
    }
}

struct ProdScratchpad {
    shadow_mode: bool,
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
    files_unmasked: RwLock<HashMap<ParquetFilePath, bool>>,
}

impl std::fmt::Debug for ProdScratchpad {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ref_files_unmasked = self.files_unmasked.read().unwrap();
        f.debug_struct("ProdScratchpad")
            .field("concurrency", &self.concurrency)
            .field("backoff_config", &self.backoff_config)
            .field("store_input", &self.store_input)
            .field("store_scratchpad", &self.store_scratchpad)
            .field("store_output", &self.store_output)
            .field("mask", &self.mask)
            .field("files_unmasked", &ref_files_unmasked)
            .finish()
    }
}

impl ProdScratchpad {
    fn apply_mask(&self, files: &[ParquetFilePath]) -> (Vec<ParquetFilePath>, Vec<Uuid>) {
        files
            .iter()
            .map(|f| {
                let uuid = Self::xor_uuids(f.objest_store_id(), self.mask);
                let f = (f.clone()).with_object_store_id(uuid);
                (f, uuid)
            })
            .unzip()
    }

    fn xor_uuids(a: Uuid, b: Uuid) -> Uuid {
        Uuid::from_u128(a.as_u128() ^ b.as_u128())
    }

    fn check_known(
        &self,
        files_unmasked: &[ParquetFilePath],
        files_masked: &[ParquetFilePath],
        output: bool,
    ) -> (Vec<ParquetFilePath>, Vec<ParquetFilePath>) {
        let mut ref_files_unmasked = self.files_unmasked.write().unwrap();

        files_unmasked
            .iter()
            .zip(files_masked)
            .filter(|(f_unmasked, _f_masked)| {
                match ref_files_unmasked.entry((*f_unmasked).clone()) {
                    Entry::Occupied(mut o) => {
                        let old_var = *o.get();
                        *o.get_mut() |= output;
                        output && !old_var
                    }
                    Entry::Vacant(v) => {
                        v.insert(output);
                        true
                    }
                }
            })
            .map(|(un, masked)| (un.clone(), masked.clone()))
            .unzip()
    }
}

impl Drop for ProdScratchpad {
    fn drop(&mut self) {
        let mut ref_files_unmasked = self.files_unmasked.write().unwrap();

        if !ref_files_unmasked.is_empty() {
            warn!("scratchpad context not cleaned, may leak resources");

            // clean up eventually
            // Note: Use manual clean up code and do not create yet-another ProdScratchpad to avoid infinite recursions
            //       during drop.
            let files = ref_files_unmasked
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
    fn uuids(&self, files: &[ParquetFilePath]) -> Vec<Uuid> {
        let (_, uuids) = self.apply_mask(files);
        uuids
    }

    async fn load_to_scratchpad(&self, files: &[ParquetFilePath]) -> Vec<Uuid> {
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

    async fn make_public(&self, files: &[ParquetFilePath]) -> Vec<Uuid> {
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

    // clean_from_scratchpad selectively removes some files from the scratchpad.
    // This should be called after uploading files to objectstore.
    // Cleaning should be done regularly, so the scratchpad doesn't get too big.
    async fn clean_from_scratchpad(&self, files: &[ParquetFilePath]) {
        let files_masked: Vec<ParquetFilePath>;
        let _uuid: Vec<Uuid>;

        // scope the files_unmasked lock to protect manipulation of the scratchpad's state, but release it
        // before doing the async delete of files removed from the scratchpad.
        {
            let mut ref_files_unmasked = self.files_unmasked.write().unwrap();

            let files = files
                .iter()
                .filter(|f| ref_files_unmasked.remove(f).is_some())
                .cloned()
                .collect::<Vec<_>>();
            (files_masked, _uuid) = self.apply_mask(&files);
        }

        delete_files(
            &files_masked,
            Arc::clone(&self.store_scratchpad),
            &self.backoff_config,
            self.concurrency,
        )
        .await;
    }

    // clean_written_from_scratchpad is the same as clean_from_scratchpad, but it does not remove files
    // when in shadow mode, since in shadow mode the scratchpad is the only copy of files.
    async fn clean_written_from_scratchpad(&self, files: &[ParquetFilePath]) {
        if !self.shadow_mode {
            self.clean_from_scratchpad(files).await;
        }
    }

    async fn clean(&self) {
        // clean will remove all files in the scratchpad as of the time files_unmasked is locked.
        let files: Vec<_> = self
            .files_unmasked
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect();

        // self.files_unmasked is locked again in clean_from_scratchpad.  If another thread removes a file
        // between this relock, clean_from_scratchpad will skip it.
        self.clean_from_scratchpad(&files).await;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use test_helpers::{maybe_start_logging, tracing::TracingCapture};

    use crate::components::scratchpad::test_util::{assert_content, file_path, stores};
    use compactor_test_utils::list_object_store;

    use super::*;

    #[test]
    fn test_display() {
        let (store_input, store_scratchpad, store_output) = stores();
        let gen = ProdScratchpadGen::new(
            true,
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
            true,
            NonZeroUsize::new(1).unwrap(),
            BackoffConfig::default(),
            Arc::clone(&store_input),
            Arc::clone(&store_scratchpad),
            Arc::clone(&store_output),
        );
        let pad = gen.pad();

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

        let early_get_uuids = pad.uuids(&[f1.clone(), f2.clone()]);

        let uuids = pad.load_to_scratchpad(&[f1.clone(), f2.clone()]).await;
        assert_eq!(uuids.len(), 2);
        assert_eq!(early_get_uuids, uuids);
        let f1_masked = f1.clone().with_object_store_id(uuids[0]);
        let f2_masked = f2.clone().with_object_store_id(uuids[1]);

        assert_content(&store_input, [&f1, &f2, &f3, &f4]).await;
        assert_content(&store_scratchpad, [&f1_masked, &f2_masked]).await;
        assert_content(&store_output, []).await;

        let uuids = pad.load_to_scratchpad(&[f2.clone(), f3.clone()]).await;
        assert_eq!(uuids.len(), 2);
        assert_eq!(f2_masked.objest_store_id(), uuids[0]);
        let f3_masked = f3.clone().with_object_store_id(uuids[1]);

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

        let uuids = pad
            .make_public(&[f5_masked.clone(), f6_masked.clone()])
            .await;
        assert_eq!(uuids.len(), 2);
        let f5 = f5_masked.clone().with_object_store_id(uuids[0]);
        let f6 = f6_masked.clone().with_object_store_id(uuids[1]);

        assert_content(&store_input, [&f1, &f2, &f3, &f4]).await;
        assert_content(
            &store_scratchpad,
            [
                &f1_masked, &f2_masked, &f3_masked, &f5_masked, &f6_masked, &f7_masked,
            ],
        )
        .await;
        assert_content(&store_output, [&f5, &f6]).await;

        let uuids = pad.make_public(&[f1_masked.clone()]).await;
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

        // we're in shadow mode, so written (compaction output) files must be be removed.
        pad.clean_written_from_scratchpad(&[f1.clone(), f5.clone()])
            .await;

        // they're still there
        assert_content(
            &store_scratchpad,
            [
                &f1_masked, &f2_masked, &f3_masked, &f5_masked, &f6_masked, &f7_masked,
            ],
        )
        .await;

        pad.clean_from_scratchpad(&[f1.clone(), f5.clone()]).await;

        assert_content(
            &store_scratchpad,
            [&f2_masked, &f3_masked, &f6_masked, &f7_masked],
        )
        .await;

        // Reload a cleaned file back into the scratchpad, simulating a backlogged partition that
        // requires several compaction loops (where the output of one compaction is later the input
        // to a subsequent compaction).
        let uuids = pad.load_to_scratchpad(&[f1.clone()]).await;
        assert_eq!(uuids.len(), 1);
        assert_eq!(f1_masked.objest_store_id(), uuids[0]);

        assert_content(&store_input, [&f1, &f2, &f3, &f4]).await;
        assert_content(
            &store_scratchpad,
            [&f1_masked, &f2_masked, &f3_masked, &f6_masked, &f7_masked],
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
            false,
            NonZeroUsize::new(1).unwrap(),
            BackoffConfig::default(),
            Arc::clone(&store_input),
            Arc::clone(&store_scratchpad),
            Arc::clone(&store_output),
        );

        let pad1 = gen.pad();
        let pad2 = gen.pad();

        let f = file_path(1);

        store_input
            .put(&f.object_store_path(), Default::default())
            .await
            .unwrap();

        let uuids = pad1.load_to_scratchpad(&[f.clone()]).await;
        assert_eq!(uuids.len(), 1);
        let f_masked1 = f.clone().with_object_store_id(uuids[0]);

        let uuids = pad2.load_to_scratchpad(&[f.clone()]).await;
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
            false,
            NonZeroUsize::new(1).unwrap(),
            BackoffConfig::default(),
            Arc::clone(&store_input),
            Arc::clone(&store_scratchpad),
            Arc::clone(&store_output),
        );
        let pad = gen.pad();

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
            false,
            NonZeroUsize::new(1).unwrap(),
            BackoffConfig::default(),
            Arc::clone(&store_input),
            Arc::clone(&store_scratchpad),
            Arc::clone(&store_output),
        );
        let pad = gen.pad();

        let f = file_path(1);

        store_input
            .put(&f.object_store_path(), Default::default())
            .await
            .unwrap();

        pad.load_to_scratchpad(&[f]).await;

        panic!("foo");
    }
}
