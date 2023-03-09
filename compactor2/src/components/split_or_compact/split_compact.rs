use std::fmt::Display;

use data_types::{CompactionLevel, ParquetFile};

use crate::{file_classification::FilesToCompactOrSplit, partition_info::PartitionInfo};

use super::{
    files_to_compact::limit_files_to_compact, files_to_split::identify_files_to_split,
    SplitOrCompact,
};

#[derive(Debug)]
pub struct SplitCompact {
    max_compact_size: usize,
}

impl SplitCompact {
    pub fn new(max_compact_size: usize) -> Self {
        Self { max_compact_size }
    }
}

impl Display for SplitCompact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "split_or_compact({})", self.max_compact_size)
    }
}

impl SplitOrCompact for SplitCompact {
    /// Return (`[files_to_split_or_compact]`, `[files_to_keep]`) of given files
    ///
    /// Verify if the the give files are over the max_compact_size limit
    /// If so, find start-level files that can be split to reduce the number of overlapped files that must be compact in one run.
    /// If split is not needed, pick files to compact that under max_compact_size limit
    fn apply(
        &self,
        _partition_info: &PartitionInfo,
        files: Vec<ParquetFile>,
        target_level: CompactionLevel,
    ) -> (FilesToCompactOrSplit, Vec<ParquetFile>) {
        // Compact all in one run if total size is less than max_compact_size
        let total_size: i64 = files.iter().map(|f| f.file_size_bytes).sum();
        if total_size as usize <= self.max_compact_size {
            return (FilesToCompactOrSplit::FilesToCompact(files), vec![]);
        }

        // See if split is needed
        let (files_to_split, files_not_to_split) = identify_files_to_split(files, target_level);

        if !files_to_split.is_empty() {
            // These files must be split before further compaction
            (
                FilesToCompactOrSplit::FilesToSplit(files_to_split),
                files_not_to_split,
            )
        } else {
            // No split is needed, need to limit number of files to compact to stay under total size limit
            let (files_to_compact, files_to_keep) =
                limit_files_to_compact(self.max_compact_size, files_not_to_split, target_level);

            (
                FilesToCompactOrSplit::FilesToCompact(files_to_compact),
                files_to_keep,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use compactor2_test_utils::{
        create_overlapped_l0_l1_files_2, create_overlapped_l1_l2_files_2, format_files,
        format_files_split,
    };
    use data_types::CompactionLevel;

    use crate::{
        components::split_or_compact::{split_compact::SplitCompact, SplitOrCompact},
        test_utils::PartitionInfoBuilder,
    };

    const MAX_SIZE: usize = 100;

    #[test]
    fn test_empty() {
        let files = vec![];
        let p_info = Arc::new(PartitionInfoBuilder::new().build());
        let split_compact = SplitCompact::new(MAX_SIZE);
        let (files_to_compact_or_split, files_to_keep) =
            split_compact.apply(&p_info, files, CompactionLevel::Initial);

        assert!(files_to_compact_or_split.is_empty());
        assert!(files_to_keep.is_empty());
    }

    #[test]
    fn test_compact_too_large_to_compact() {
        let files = create_overlapped_l1_l2_files_2(MAX_SIZE as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L1, all files 100b                                                                                  "
        - "L1.13[600,700]                                                                      |----L1.13-----|"
        - "L1.12[400,500]                                      |----L1.12-----|                                "
        - "L1.11[250,350]              |----L1.11-----|                                                        "
        - "L2, all files 100b                                                                                  "
        - "L2.22[200,300]      |----L2.22-----|                                                                "
        "###
        );

        let p_info = Arc::new(PartitionInfoBuilder::new().build());
        let split_compact = SplitCompact::new(MAX_SIZE);
        let (files_to_compact_or_split, files_to_keep) =
            split_compact.apply(&p_info, files, CompactionLevel::Final);
        // nothing to compact or split
        // after https://github.com/influxdata/idpe/issues/17246, this list won't be empty
        assert!(files_to_compact_or_split.is_empty());
        assert_eq!(files_to_keep.len(), 4);
    }

    #[test]
    fn test_compact_files_no_limit() {
        let files = create_overlapped_l0_l1_files_2(MAX_SIZE as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 100b                                                                                  "
        - "L0.2[650,750]                                               |-----L0.2-----|                        "
        - "L0.1[450,620]               |----------L0.1-----------|                                             "
        - "L0.3[800,900]                                                                       |-----L0.3-----|"
        - "L1, all files 100b                                                                                  "
        - "L1.13[600,700]                                      |----L1.13-----|                                "
        - "L1.12[400,500]      |----L1.12-----|                                                                "
        "###
        );

        // size limit > total size --> compact all
        let p_info = Arc::new(PartitionInfoBuilder::new().build());
        let split_compact = SplitCompact::new(MAX_SIZE * 6 + 1);
        let (files_to_compact_or_split, files_to_keep) =
            split_compact.apply(&p_info, files, CompactionLevel::FileNonOverlapped);

        assert_eq!(files_to_compact_or_split.files_to_compact_len(), 5);
        assert!(files_to_keep.is_empty());

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact", &files_to_compact_or_split.files_to_compact() , "files to keep:", &files_to_keep),
            @r###"
        ---
        - files to compact
        - "L0, all files 100b                                                                                  "
        - "L0.2[650,750]                                               |-----L0.2-----|                        "
        - "L0.1[450,620]               |----------L0.1-----------|                                             "
        - "L0.3[800,900]                                                                       |-----L0.3-----|"
        - "L1, all files 100b                                                                                  "
        - "L1.13[600,700]                                      |----L1.13-----|                                "
        - "L1.12[400,500]      |----L1.12-----|                                                                "
        - "files to keep:"
        "###
        );
    }

    #[test]
    fn test_split_files() {
        let files = create_overlapped_l0_l1_files_2(MAX_SIZE as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 100b                                                                                  "
        - "L0.2[650,750]                                               |-----L0.2-----|                        "
        - "L0.1[450,620]               |----------L0.1-----------|                                             "
        - "L0.3[800,900]                                                                       |-----L0.3-----|"
        - "L1, all files 100b                                                                                  "
        - "L1.13[600,700]                                      |----L1.13-----|                                "
        - "L1.12[400,500]      |----L1.12-----|                                                                "
        "###
        );

        // hit size limit -> split start_level files that overlap with more than 1 target_level files
        let p_info = Arc::new(PartitionInfoBuilder::new().build());
        let split_compact = SplitCompact::new(MAX_SIZE);
        let (files_to_compact_or_split, files_to_keep) =
            split_compact.apply(&p_info, files, CompactionLevel::FileNonOverlapped);

        assert_eq!(files_to_compact_or_split.files_to_split_len(), 1);
        assert_eq!(files_to_keep.len(), 4);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact or split:", &files_to_compact_or_split.files_to_split(), "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact or split:"
        - "L0, all files 100b                                                                                  "
        - "L0.1[450,620]       |-------------------------------------L0.1-------------------------------------|"
        - "files to keep:"
        - "L0, all files 100b                                                                                  "
        - "L0.2[650,750]                                               |-----L0.2-----|                        "
        - "L0.3[800,900]                                                                       |-----L0.3-----|"
        - "L1, all files 100b                                                                                  "
        - "L1.12[400,500]      |----L1.12-----|                                                                "
        - "L1.13[600,700]                                      |----L1.13-----|                                "
        "###
        );
    }

    #[test]
    fn test_compact_files() {
        let files = create_overlapped_l1_l2_files_2(MAX_SIZE as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L1, all files 100b                                                                                  "
        - "L1.13[600,700]                                                                      |----L1.13-----|"
        - "L1.12[400,500]                                      |----L1.12-----|                                "
        - "L1.11[250,350]              |----L1.11-----|                                                        "
        - "L2, all files 100b                                                                                  "
        - "L2.22[200,300]      |----L2.22-----|                                                                "
        "###
        );

        // hit size limit and nthign to split --> limit number if files to compact
        let p_info = Arc::new(PartitionInfoBuilder::new().build());
        let split_compact = SplitCompact::new(MAX_SIZE * 3);
        let (files_to_compact_or_split, files_to_keep) =
            split_compact.apply(&p_info, files, CompactionLevel::Final);

        assert_eq!(files_to_compact_or_split.files_to_compact_len(), 3);
        assert_eq!(files_to_keep.len(), 1);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact or split:", &files_to_compact_or_split.files_to_compact() , "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact or split:"
        - "L1, all files 100b                                                                                  "
        - "L1.11[250,350]                   |---------L1.11----------|                                         "
        - "L1.12[400,500]                                                           |---------L1.12----------| "
        - "L2, all files 100b                                                                                  "
        - "L2.22[200,300]      |---------L2.22----------|                                                      "
        - "files to keep:"
        - "L1, all files 100b                                                                                  "
        - "L1.13[600,700]      |------------------------------------L1.13-------------------------------------|"
        "###
        );
    }
}
