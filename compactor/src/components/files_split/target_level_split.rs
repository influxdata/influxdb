use std::fmt::Display;

use data_types::{CompactionLevel, ParquetFile};

use super::FilesSplit;

/// Split given files into 2 groups of files: `[<= target_level]` and `[> target_level]`
#[derive(Debug)]
pub struct TargetLevelSplit {}

impl TargetLevelSplit {
    pub fn new() -> Self {
        Self {}
    }
}

impl Display for TargetLevelSplit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Target level split for TargetLevel version")
    }
}

impl FilesSplit for TargetLevelSplit {
    fn apply(
        &self,
        files: Vec<ParquetFile>,
        target_level: CompactionLevel,
    ) -> (Vec<ParquetFile>, Vec<ParquetFile>) {
        files
            .into_iter()
            .partition(|f| f.compaction_level <= target_level)
    }
}

#[cfg(test)]
mod tests {

    use compactor_test_utils::{
        create_l0_files, create_l1_files, create_l2_files, create_overlapped_files, format_files,
        format_files_split,
    };

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            TargetLevelSplit::new().to_string(),
            "Target level split for TargetLevel version"
        );
    }

    #[test]
    fn test_apply_empty_files() {
        let files = vec![];
        let split = TargetLevelSplit::new();

        let (lower, higher) = split.apply(files, CompactionLevel::FileNonOverlapped);
        assert_eq!(lower.len(), 0);
        assert_eq!(higher.len(), 0);
    }

    #[test]
    fn test_apply_partial_empty_files_l0() {
        let files = create_l0_files(1);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 1b                                                                                                   "
        - "L0.2[650,750] 0ns                                                |-------L0.2-------|                              "
        - "L0.1[450,620] 0ns        |--------------L0.1--------------|                                                        "
        - "L0.3[800,900] 0ns                                                                              |-------L0.3-------|"
        "###
        );

        let split = TargetLevelSplit::new();
        let (lower, higher) = split.apply(files.clone(), CompactionLevel::Initial);
        assert_eq!(lower.len(), 3);
        assert_eq!(higher.len(), 0);

        let (lower, higher) = split.apply(files.clone(), CompactionLevel::FileNonOverlapped);
        assert_eq!(lower.len(), 3);
        assert_eq!(higher.len(), 0);

        let (lower, higher) = split.apply(files, CompactionLevel::Final);
        assert_eq!(lower.len(), 3);
        assert_eq!(higher.len(), 0);
    }

    #[test]
    fn test_apply_partial_empty_files_l1() {
        let files = create_l1_files(1);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L1, all files 1b                                                                                                   "
        - "L1.13[600,700] 0ns                                                                             |------L1.13-------|"
        - "L1.12[400,500] 0ns                                     |------L1.12-------|                                        "
        - "L1.11[250,350] 0ns       |------L1.11-------|                                                                      "
        "###
        );

        let split = TargetLevelSplit::new();
        let (lower, higher) = split.apply(files.clone(), CompactionLevel::Initial);
        assert_eq!(lower.len(), 0);
        assert_eq!(higher.len(), 3);

        let (lower, higher) = split.apply(files.clone(), CompactionLevel::FileNonOverlapped);
        assert_eq!(lower.len(), 3);
        assert_eq!(higher.len(), 0);
        //
        let (lower, higher) = split.apply(files, CompactionLevel::Final);
        assert_eq!(lower.len(), 3);
        assert_eq!(higher.len(), 0);
    }

    #[test]
    fn test_apply_partial_empty_files_l2() {
        let files = create_l2_files();
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L2, all files 1b                                                                                                   "
        - "L2.21[0,100] 0ns         |-----------L2.21------------|                                                            "
        - "L2.22[200,300] 0ns                                                                   |-----------L2.22------------|"
        "###
        );

        let split = TargetLevelSplit::new();
        let (lower, higher) = split.apply(files.clone(), CompactionLevel::Initial);
        assert_eq!(lower.len(), 0);
        assert_eq!(higher.len(), 2);

        let (lower, higher) = split.apply(files.clone(), CompactionLevel::FileNonOverlapped);
        assert_eq!(lower.len(), 0);
        assert_eq!(higher.len(), 2);

        let (lower, higher) = split.apply(files, CompactionLevel::Final);
        assert_eq!(lower.len(), 2);
        assert_eq!(higher.len(), 0);
    }

    #[test]
    fn test_apply_target_level_0() {
        // Test target level Initial
        let files = create_overlapped_files();
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0                                                                                                                 "
        - "L0.2[650,750] 0ns 1b                                                                      |--L0.2--|               "
        - "L0.1[450,620] 0ns 1b                                                  |-----L0.1------|                            "
        - "L0.3[800,900] 0ns 100b                                                                                   |--L0.3--|"
        - "L1                                                                                                                 "
        - "L1.13[600,700] 0ns 100b                                                              |-L1.13--|                    "
        - "L1.12[400,500] 0ns 1b                                            |-L1.12--|                                        "
        - "L1.11[250,350] 0ns 1b                             |-L1.11--|                                                       "
        - "L2                                                                                                                 "
        - "L2.21[0,100] 0ns 1b      |-L2.21--|                                                                                "
        - "L2.22[200,300] 0ns 1b                        |-L2.22--|                                                            "
        "###
        );

        let split = TargetLevelSplit::new();
        let (lower, higher) = split.apply(files, CompactionLevel::Initial);

        insta::assert_yaml_snapshot!(
            format_files_split("lower", &lower, "higher", &higher),
            @r###"
        ---
        - lower
        - "L0                                                                                                                 "
        - "L0.2[650,750] 0ns 1b                                             |-------L0.2-------|                              "
        - "L0.1[450,620] 0ns 1b     |--------------L0.1--------------|                                                        "
        - "L0.3[800,900] 0ns 100b                                                                         |-------L0.3-------|"
        - higher
        - "L1                                                                                                                 "
        - "L1.13[600,700] 0ns 100b                                                                               |--L1.13---| "
        - "L1.12[400,500] 0ns 1b                                                       |--L1.12---|                           "
        - "L1.11[250,350] 0ns 1b                                    |--L1.11---|                                              "
        - "L2                                                                                                                 "
        - "L2.21[0,100] 0ns 1b      |--L2.21---|                                                                              "
        - "L2.22[200,300] 0ns 1b                             |--L2.22---|                                                     "
        "###
        );

        // verify number of files
        assert_eq!(lower.len(), 3);
        assert_eq!(higher.len(), 5);
        // verify compaction level of files
        assert!(lower
            .iter()
            .all(|f| f.compaction_level == CompactionLevel::Initial));
        assert!(higher
            .iter()
            .all(|f| f.compaction_level == CompactionLevel::FileNonOverlapped
                || f.compaction_level == CompactionLevel::Final));
    }

    #[test]
    fn test_apply_target_level_l1() {
        // Test target level is FileNonOverlapped
        let files = create_overlapped_files();
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0                                                                                                                 "
        - "L0.2[650,750] 0ns 1b                                                                      |--L0.2--|               "
        - "L0.1[450,620] 0ns 1b                                                  |-----L0.1------|                            "
        - "L0.3[800,900] 0ns 100b                                                                                   |--L0.3--|"
        - "L1                                                                                                                 "
        - "L1.13[600,700] 0ns 100b                                                              |-L1.13--|                    "
        - "L1.12[400,500] 0ns 1b                                            |-L1.12--|                                        "
        - "L1.11[250,350] 0ns 1b                             |-L1.11--|                                                       "
        - "L2                                                                                                                 "
        - "L2.21[0,100] 0ns 1b      |-L2.21--|                                                                                "
        - "L2.22[200,300] 0ns 1b                        |-L2.22--|                                                            "
        "###
        );

        let split = TargetLevelSplit::new();
        let (lower, higher) = split.apply(files, CompactionLevel::FileNonOverlapped);

        insta::assert_yaml_snapshot!(
            format_files_split("lower", &lower, "higher", &higher),
            @r###"
        ---
        - lower
        - "L0                                                                                                                 "
        - "L0.2[650,750] 0ns 1b                                                            |---L0.2----|                      "
        - "L0.1[450,620] 0ns 1b                                |--------L0.1---------|                                        "
        - "L0.3[800,900] 0ns 100b                                                                               |---L0.3----| "
        - "L1                                                                                                                 "
        - "L1.13[600,700] 0ns 100b                                                  |---L1.13---|                             "
        - "L1.12[400,500] 0ns 1b                        |---L1.12---|                                                         "
        - "L1.11[250,350] 0ns 1b    |---L1.11---|                                                                             "
        - higher
        - "L2, all files 1b                                                                                                   "
        - "L2.21[0,100] 0ns         |-----------L2.21------------|                                                            "
        - "L2.22[200,300] 0ns                                                                   |-----------L2.22------------|"
        "###
        );

        // verify number of files
        assert_eq!(lower.len(), 6);
        assert_eq!(higher.len(), 2);
        // verify compaction level of files
        assert!(lower
            .iter()
            .all(|f| f.compaction_level == CompactionLevel::Initial
                || f.compaction_level == CompactionLevel::FileNonOverlapped));
        assert!(higher
            .iter()
            .all(|f| f.compaction_level == CompactionLevel::Final));
    }

    #[test]
    fn test_apply_taget_level_l2() {
        // Test target level is Final
        let files = create_overlapped_files();
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0                                                                                                                 "
        - "L0.2[650,750] 0ns 1b                                                                      |--L0.2--|               "
        - "L0.1[450,620] 0ns 1b                                                  |-----L0.1------|                            "
        - "L0.3[800,900] 0ns 100b                                                                                   |--L0.3--|"
        - "L1                                                                                                                 "
        - "L1.13[600,700] 0ns 100b                                                              |-L1.13--|                    "
        - "L1.12[400,500] 0ns 1b                                            |-L1.12--|                                        "
        - "L1.11[250,350] 0ns 1b                             |-L1.11--|                                                       "
        - "L2                                                                                                                 "
        - "L2.21[0,100] 0ns 1b      |-L2.21--|                                                                                "
        - "L2.22[200,300] 0ns 1b                        |-L2.22--|                                                            "
        "###
        );

        let split = TargetLevelSplit::new();
        let (lower, higher) = split.apply(files, CompactionLevel::Final);

        // verify number of files (nothing in higher)
        assert_eq!(lower.len(), 8);
        assert_eq!(higher.len(), 0);
        // verify compaction level of files
        assert!(lower
            .iter()
            .all(|f| f.compaction_level == CompactionLevel::Initial
                || f.compaction_level == CompactionLevel::FileNonOverlapped
                || f.compaction_level == CompactionLevel::Final));
    }
}
