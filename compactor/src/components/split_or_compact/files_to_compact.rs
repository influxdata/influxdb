use data_types::{CompactionLevel, ParquetFile, Timestamp};

use crate::components::{
    divide_initial::multiple_branches::order_files,
    files_split::{target_level_split::TargetLevelSplit, FilesSplit},
};
use crate::file_classification::FileToSplit;

/// Return a struct that holds 2 sets of files:
///  1. Either files_to_compact or files_to_split
///      - files_to_compact is prioritized first as long as there is a minimum possible compacting
///        set of files that is under max_compact_size limit.
///      - files_to_split is returned when  the minimum possible compacting set of files
///        is over max_compact_size limit. files_to_split is that minimum set of files.
///  2. files_to_keep for next round of compaction
///
/// The input of this function has a constraint that every single file in start-level must overlap
/// with at most one file in target level
///
/// To deduplicate data correctly, we need to select start-level files in their max_l0_created_at order
/// if they are level-0 or in their min_time order otherwise, and they must be compacted with overlapped
/// files in target level. See example below for the
/// correlation between created order and overlapped time ranges of files
///
/// Example of start level as L0 and target level as L1:
///
///  Input Files: three L0 and threee L1 files. The ID after the dot is the order the files are created
///     |---L0.1---|  |---L0.3---|  |---L0.2---|  Note that L0.2 is created BEFORE L0.3 but has LATER time range
///   |---L1.1---|  |---L1.2---|  |---L1.3---|
///
///  Output: 4 possible choices:
///   1. Smallest compacting set, L0.1 + L1.1, is too large to compact:
///     - files_to_split: L0.1 + L1.1
///     - files_to_keep: L0.2 + L0.3 + L1.2 + L1.3
///   2. Smallest compacting set: L0.1 + L1.1
///     - files_to_compact: L0.1 + L1.1
///     - files_to_keep: L0.2 + L0.3 + L1.2 + L1.3
///   2. Medium size compacting set: L0.1 + L1.1 + L0.2 + L1.2 + L1.3
///      Note that L1.2 overlaps with the time range of L0.1 + L0.2 and must be included here
///     - files_to_compact: L0.1 + L1.1 + L0.2 + L1.2 + L1.3
///     - files_to_keep: L0.3
///   3. Largest compacting set: All input files
///     - files_to_compact: All input files
///     - files_to_keep: None
///
/// Example of start level as L1 and target level as L2.
/// Note the difference of the output compared with the previous example
///
///  Input Files: three L1 and threee L2 files. The ID after the dot is the order the files are created
///     |---L1.1---|  |---L1.3---|  |---L1.2---|  Note that L1.2 is created BEFORE L1.3 but has LATER time range
///   |---L2.1---|  |---L2.2---|  |---L2.3---|
///
///  Output: 4 possible choices:
///  1. Smallest compacting set, L1.1 + L2.1, is too large to compact:
///    - files_to_split: L1.1 + L2.1
///    - files_to_keep: L1.2 + L1.3 + L2.2 + L2.3
///  2. Smallest compacting set: L1.1 + L2.1
///    - files_to_compact: L1.1 + L2.1
///    - files_to_keep: L1.2 + L1.3 + L2.2 + L2.3
///  3. Medium size compacting set: L1.1 + L2.1 + L1.3 + L2.2
///    Note L1.3 has smaller time range and must be compacted before L1.2
///    - files_to_compact: L1.1 + L2.1 + L1.3 + L2.2
///    - files_to_keep: L1.2 + L2.3
///  4. Largest compacting set: All input files
///    - files_to_compact: All input files
///    - files_to_keep: None
///
/// Example of start level as L1 and target level as L2.
/// Note that even though each start level file overlaps at most one target level file, there is a target
/// level file that overlaps two start level files. In this scenario, it is not allowed to compact L1.3
/// without also compacting L1.2.
///
///  Input Files: three L1 and threee L2 files. The ID after the dot is the order the files are created
///     |---L1.1---|  |---L1.3---|  |---L1.2---|  Note that L1.2 is created BEFORE L1.3 but has LATER time range
///   |---L2.1---|  |---L2.2----------------|
///
///  Output: 4 possible choices:
///  1. Smallest compacting set, L1.1 + L2.1, is too large to compact:
///    - files_to_split: L1.1 + L2.1
///    - files_to_keep: L1.2 + L1.3 + L2.2
///  2. Smallest compacting set: L1.1 + L2.1
///    - files_to_compact: L1.1 + L2.1
///    - files_to_keep: L1.2 + L1.3 + L2.2
///  3. Largest compacting set: All input files
///    - files_to_compact: All input files
///    - files_to_keep: None
/// Illegal option that breaks invariant:
///  1. Medium size compacting set: L1.1 + L2.1 + L1.3 + L2.2
///    Note even though each L1 only overlaps a single L2, L2.2 overlaps two L1s.
///    If L1.3 is compacted with L2.2, but without L1.2, the resulting L2 (which
///    still overlaps L1.2) now has a newer max_l0_created_at than L1.2, which
///    violates an invariant.  Thus, for L1->L2 compactions, we must include all
///    overlaps of the target level files.
///
pub fn limit_files_to_compact(
    max_compact_files: usize,
    max_compact_size: usize,
    files: Vec<ParquetFile>,
    target_level: CompactionLevel,
) -> KeepAndCompactSplit {
    // panic if not all files are either in target level or start level
    let start_level = target_level.prev();
    assert!(files
        .iter()
        .all(|f| f.compaction_level == target_level || f.compaction_level == start_level));

    // Get start-level and target-level files
    let len = files.len();
    let split = TargetLevelSplit::new();
    let (start_level_files, mut target_level_files) = split.apply(files, start_level);

    // panic if there is any file in start level that overlaps with more than one file in target level
    assert!(start_level_files.iter().all(|s| target_level_files
        .iter()
        .filter(|t| t.overlaps(s))
        .count()
        <= 1));

    // Order start-level files to group the files to compact them correctly
    let start_level_files = order_files(start_level_files, start_level);
    let mut start_level_files_iter = start_level_files.clone().into_iter();

    // Go over start-level files and find overlapped files in target level
    let mut start_level_files_to_compact = Vec::with_capacity(len);
    let mut target_level_files_to_compact = Vec::with_capacity(len);
    let mut files_to_further_split = Vec::with_capacity(len);
    let mut files_to_keep = Vec::with_capacity(len);
    let mut specific_splits: Vec<FileToSplit> = Vec::with_capacity(len);
    let mut total_size = 0;

    for file in start_level_files_iter.by_ref() {
        // A start-level file, if compacted, must be compacted with all of its overlapped target-level files.
        // Once target-level files are selected, we must recheck for additional overlapping start level files,
        // as they must be included in the compacting set.
        // Thus compute the size needed before deciding to compact this file and its overlaps or not

        if start_level_files_to_compact
            .iter()
            .any(|x: &ParquetFile| x.id == file.id)
        {
            // `file` was added in prior iteration (see extra_overlapped_start_files below)
            continue;
        }

        // Time range of start_level_files_to_compact plus this file
        let (min_time, max_time) = time_range(&file, &start_level_files_to_compact);

        // Get all target-level files that overlaps with the time range and not yet in target_level_files_to_compact
        let overlapped_files: Vec<&ParquetFile> = target_level_files
            .iter()
            .filter(|f| f.overlaps_time_range(min_time, max_time))
            .filter(|f| !target_level_files_to_compact.iter().any(|x| x == *f))
            .collect();

        // Now check if those additional target-level files overlap additional start level files not already under consideration.
        // See `Illegal option that breaks invariant` in the comment above.  Requiring the inclusion of these extra overlapped start
        // files is how to prevent invariant violation.
        let mut extra_overlapped_start_files: Vec<&ParquetFile> = Vec::with_capacity(len);
        if !overlapped_files.is_empty() && target_level == CompactionLevel::Final {
            let (min_time, max_time) = list_time_range(&overlapped_files);

            extra_overlapped_start_files = start_level_files
                .as_slice()
                .iter()
                .filter(|f| f.overlaps_time_range(min_time, max_time))
                .filter(|f| !start_level_files_to_compact.iter().any(|x| x.id == f.id))
                .filter(|f| f.id != file.id)
                .collect();
        }

        // Size of the file and its overlapped files
        let size = file.file_size_bytes
            + overlapped_files
                .iter()
                .map(|f| f.file_size_bytes)
                .sum::<i64>()
            + extra_overlapped_start_files
                .iter()
                .map(|f| f.file_size_bytes)
                .sum::<i64>();

        // If total size is under limit, add this file and its overlapped files to files_to_compact
        if total_size + size <= max_compact_size as i64
            && start_level_files_to_compact.len() < max_compact_files
        {
            start_level_files_to_compact.push(file);

            for f in extra_overlapped_start_files {
                start_level_files_to_compact.push(f.clone());
            }

            target_level_files_to_compact
                .extend(overlapped_files.into_iter().cloned().collect::<Vec<_>>());
            total_size += size;
        } else {
            // Over limit, stop here
            if start_level_files_to_compact.is_empty() {
                if !extra_overlapped_start_files.is_empty() {
                    // The target level files that overlap file, also overlap additional start level files, which
                    // caused (or worsened) the over size condition.  So we'll split the overlapping file(s) at
                    // the timestamps at which they overlap additional start level files.
                    let all_split_times = &extra_overlapped_start_files
                        .iter()
                        .map(|f| f.min_time)
                        .collect::<Vec<_>>();

                    for f in overlapped_files.clone() {
                        let split_times = all_split_times
                            .iter()
                            .filter(|t| f.overlaps_time_range(**t, **t))
                            .map(|t| t.get())
                            .collect::<Vec<_>>();

                        specific_splits.push(FileToSplit {
                            file: f.clone(),
                            split_times,
                        });
                    }
                }
                // If the above found some specific splits, we'll go with that.  Otherwise, make a general split decision.
                if specific_splits.is_empty() {
                    // nothing to compact & no specific split opportunities identified.
                    // return this minimum compacting set for further spliting
                    files_to_further_split.push(file);
                    // since there is only one start_level file,
                    // the number of overlapped target_level must be <= 1
                    assert!(overlapped_files.len() <= 1);
                    files_to_further_split
                        .extend(overlapped_files.into_iter().cloned().collect::<Vec<_>>());
                } else {
                    files_to_keep.push(file);
                }
            } else {
                // would be nice to get extra_overlapped_start_files split...
                files_to_keep.push(file);
            }

            // TODO(maybe): See matching comment in stuck.rs/stuck_l1
            // Its possible we split a few L1s so they don't overlap with too many L2s, then decided to compact just
            // a few of them above.  Now this break will cause us to leave the rest of the L1s we split to wait for
            // the next round that may never come.  The few we're compacting now may be enough to make the L1s fall
            // below the threshold for compacting another round.
            // We could set something here that's passed all the way up to the loop in `try_compact_partition` that
            // skips the partition filter and forces it to do one more round because this round was stopped prematurely.
            // That flag would mean: 'I'm in the middle of something, so ignore the continue critiera and let me finish'
            break;
        }
    }

    // Remove all files in target_level_files_to_compact
    // and files_to_further_split from target_level_files
    target_level_files.retain(|f| !target_level_files_to_compact.iter().any(|x| x == f));
    target_level_files.retain(|f| !files_to_further_split.iter().any(|x| x == f));
    target_level_files.retain(|f| !specific_splits.iter().any(|x| x.file.id == f.id));

    // All files left in start_level_files  and target_level_files are kept for next round
    target_level_files.extend(start_level_files_iter);
    files_to_keep.extend(target_level_files);

    // All files in start_level_files_to_compact and target_level_files_to_compact will be compacted
    let files_to_compact = start_level_files_to_compact
        .into_iter()
        .chain(target_level_files_to_compact.into_iter())
        .collect::<Vec<_>>();

    // Sanity check
    // All files are returned
    assert_eq!(
        files_to_compact.len()
            + files_to_further_split.len()
            + specific_splits.len()
            + files_to_keep.len(),
        len
    );

    assert!(
        files_to_compact.is_empty() as u8
            + files_to_further_split.is_empty() as u8
            + specific_splits.is_empty() as u8
            >= 2
    );

    let files_to_compact_or_further_split = if !files_to_compact.is_empty() {
        CompactOrFurtherSplit::Compact(files_to_compact)
    } else if !files_to_further_split.is_empty() {
        CompactOrFurtherSplit::FurtherSplit(files_to_further_split)
    } else {
        CompactOrFurtherSplit::SpecificSplit(specific_splits)
    };

    KeepAndCompactSplit {
        files_to_compact_or_further_split,
        files_to_keep,
    }
}

/// Return time range of the given file and the list of given files
fn time_range(file: &ParquetFile, files: &[ParquetFile]) -> (Timestamp, Timestamp) {
    let mut min_time = file.min_time;
    let mut max_time = file.max_time;
    files.iter().for_each(|f| {
        min_time = min_time.min(f.min_time);
        max_time = max_time.max(f.max_time);
    });

    (min_time, max_time)
}

/// Return time range of the list of given files
fn list_time_range(files: &[&ParquetFile]) -> (Timestamp, Timestamp) {
    let mut min_time = files[0].min_time;
    let mut max_time = files[0].max_time;
    files.iter().for_each(|f| {
        min_time = min_time.min(f.min_time);
        max_time = max_time.max(f.max_time);
    });

    (min_time, max_time)
}
/// Hold two sets of file:
///   1. files that are either small enough to compact or too large and need to further split and
///   2. files to keep for next compaction round
pub struct KeepAndCompactSplit {
    // Files are either small compact or tto large and need further split
    files_to_compact_or_further_split: CompactOrFurtherSplit,
    // Files to keep for next compaction round
    files_to_keep: Vec<ParquetFile>,
}

impl KeepAndCompactSplit {
    pub fn files_to_compact(&self) -> Vec<ParquetFile> {
        match &self.files_to_compact_or_further_split {
            CompactOrFurtherSplit::Compact(files) => files.clone(),
            CompactOrFurtherSplit::FurtherSplit(_) => vec![],
            CompactOrFurtherSplit::SpecificSplit(_) => vec![],
        }
    }

    pub fn files_to_further_split(&self) -> Vec<ParquetFile> {
        match &self.files_to_compact_or_further_split {
            CompactOrFurtherSplit::Compact(_) => vec![],
            CompactOrFurtherSplit::FurtherSplit(files) => files.clone(),
            CompactOrFurtherSplit::SpecificSplit(_) => vec![],
        }
    }

    pub fn specific_splits(&self) -> Vec<FileToSplit> {
        match &self.files_to_compact_or_further_split {
            CompactOrFurtherSplit::Compact(_) => vec![],
            CompactOrFurtherSplit::FurtherSplit(_) => vec![],
            CompactOrFurtherSplit::SpecificSplit(splits) => splits.clone(),
        }
    }

    pub fn files_to_keep(&self) -> Vec<ParquetFile> {
        self.files_to_keep.clone()
    }
}

/// Files to either compact or to further split
pub enum CompactOrFurtherSplit {
    // These overlapped files are small enough to be compacted
    Compact(Vec<ParquetFile>),

    // These overlapped files are the minimum set to compact but still too large to do so
    FurtherSplit(Vec<ParquetFile>),

    // These files are not necessarily too big, but their time range is a preventing compaction.
    // They need split at a specific time.
    SpecificSplit(Vec<FileToSplit>),
}

#[cfg(test)]
mod tests {
    use compactor_test_utils::{
        create_l1_files, create_overlapped_files, create_overlapped_l0_l1_files_2,
        create_overlapped_l0_l1_files_3, create_overlapped_start_target_files, format_files,
        format_files_split,
    };
    use data_types::CompactionLevel;

    use crate::components::split_or_compact::files_to_compact::limit_files_to_compact;

    const MAX_SIZE: usize = 100;
    const MAX_COUNT: usize = 20;

    #[test]
    fn test_compact_empty() {
        let files = vec![];
        let keep_and_split_or_compact =
            limit_files_to_compact(MAX_COUNT, MAX_SIZE, files, CompactionLevel::Initial);

        let files_to_compact = keep_and_split_or_compact.files_to_compact();
        let files_to_further_split = keep_and_split_or_compact.files_to_further_split();
        let files_to_keep = keep_and_split_or_compact.files_to_keep();

        assert!(files_to_compact.is_empty());
        assert!(files_to_further_split.is_empty());
        assert!(files_to_keep.is_empty());
    }

    #[test]
    #[should_panic]
    fn test_compact_wrong_target_level() {
        // all L1 files
        let files = create_l1_files(1);

        // Target is L0 while all files are in L1 --> panic
        let _keep_and_split_or_compact =
            limit_files_to_compact(MAX_COUNT, MAX_SIZE, files, CompactionLevel::Initial);
    }

    #[test]
    #[should_panic]
    fn test_compact_files_three_level_files() {
        // Three level files
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

        // panic because it only handle at most 2 levels next to each other
        let _keep_and_split_or_compact = limit_files_to_compact(
            MAX_COUNT,
            MAX_SIZE,
            files,
            CompactionLevel::FileNonOverlapped,
        );
    }

    #[test]
    // at least one start-level file overlaps with more than 1 target-level file
    #[should_panic]
    fn test_compact_files_start_level_overlap_many_target_levels() {
        let files = create_overlapped_l0_l1_files_2(MAX_SIZE as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 100b                                                                                                 "
        - "L0.2[650,750] 180s                                                    |------L0.2------|                           "
        - "L0.1[450,620] 120s                |------------L0.1------------|                                                   "
        - "L0.3[800,900] 300s                                                                               |------L0.3------|"
        - "L1, all files 100b                                                                                                 "
        - "L1.13[600,700] 60s                                           |-----L1.13------|                                    "
        - "L1.12[400,500] 60s       |-----L1.12------|                                                                        "
        "###
        );

        // size limit > total size --> files to compact = all L0s and overalapped L1s
        let _keep_and_split_or_compact = limit_files_to_compact(
            MAX_COUNT,
            MAX_SIZE * 5 + 1,
            files,
            CompactionLevel::FileNonOverlapped,
        );
    }

    #[test]
    fn test_compact_files_no_limit() {
        let files = create_overlapped_l0_l1_files_3(MAX_SIZE as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 100b                                                                                                 "
        - "L0.2[650,750] 180s                                                    |------L0.2------|                           "
        - "L0.1[450,550] 120s                |------L0.1------|                                                               "
        - "L0.3[800,900] 300s                                                                               |------L0.3------|"
        - "L1, all files 100b                                                                                                 "
        - "L1.13[600,700] 60s                                           |-----L1.13------|                                    "
        - "L1.12[400,500] 60s       |-----L1.12------|                                                                        "
        "###
        );

        // size limit > total size --> files to compact = all L0s and overalapped L1s
        let keep_and_split_or_compact = limit_files_to_compact(
            MAX_COUNT,
            MAX_SIZE * 5 + 1,
            files,
            CompactionLevel::FileNonOverlapped,
        );

        let files_to_compact = keep_and_split_or_compact.files_to_compact();
        let files_to_further_split = keep_and_split_or_compact.files_to_further_split();
        let files_to_keep = keep_and_split_or_compact.files_to_keep();

        assert_eq!(files_to_compact.len(), 5);
        assert_eq!(files_to_further_split.len(), 0);
        assert_eq!(files_to_keep.len(), 0);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact:", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact:"
        - "L0, all files 100b                                                                                                 "
        - "L0.1[450,550] 120s                |------L0.1------|                                                               "
        - "L0.2[650,750] 180s                                                    |------L0.2------|                           "
        - "L0.3[800,900] 300s                                                                               |------L0.3------|"
        - "L1, all files 100b                                                                                                 "
        - "L1.12[400,500] 60s       |-----L1.12------|                                                                        "
        - "L1.13[600,700] 60s                                           |-----L1.13------|                                    "
        - "files to keep:"
        "###
        );
    }

    #[test]
    fn test_compact_files_limit_too_small() {
        let files = create_overlapped_l0_l1_files_3(MAX_SIZE as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 100b                                                                                                 "
        - "L0.2[650,750] 180s                                                    |------L0.2------|                           "
        - "L0.1[450,550] 120s                |------L0.1------|                                                               "
        - "L0.3[800,900] 300s                                                                               |------L0.3------|"
        - "L1, all files 100b                                                                                                 "
        - "L1.13[600,700] 60s                                           |-----L1.13------|                                    "
        - "L1.12[400,500] 60s       |-----L1.12------|                                                                        "
        "###
        );

        // size limit too small to compact anything
        let keep_and_split_or_compact = limit_files_to_compact(
            MAX_COUNT,
            MAX_SIZE,
            files,
            CompactionLevel::FileNonOverlapped,
        );

        let files_to_compact = keep_and_split_or_compact.files_to_compact();
        let files_to_further_split = keep_and_split_or_compact.files_to_further_split();
        let files_to_keep = keep_and_split_or_compact.files_to_keep();

        assert_eq!(files_to_compact.len(), 0);
        assert_eq!(files_to_further_split.len(), 2);
        assert_eq!(files_to_keep.len(), 3);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to further split:", &files_to_further_split, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to further split:"
        - "L0, all files 100b                                                                                                 "
        - "L0.1[450,550] 120s                                     |---------------------------L0.1---------------------------|"
        - "L1, all files 100b                                                                                                 "
        - "L1.12[400,500] 60s       |--------------------------L1.12---------------------------|                              "
        - "files to keep:"
        - "L0, all files 100b                                                                                                 "
        - "L0.2[650,750] 180s                      |------------L0.2------------|                                             "
        - "L0.3[800,900] 300s                                                                   |------------L0.3------------|"
        - "L1, all files 100b                                                                                                 "
        - "L1.13[600,700] 60s       |-----------L1.13------------|                                                            "
        "###
        );
    }

    #[test]
    fn test_compact_files_limit() {
        let files = create_overlapped_l0_l1_files_3(MAX_SIZE as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 100b                                                                                                 "
        - "L0.2[650,750] 180s                                                    |------L0.2------|                           "
        - "L0.1[450,550] 120s                |------L0.1------|                                                               "
        - "L0.3[800,900] 300s                                                                               |------L0.3------|"
        - "L1, all files 100b                                                                                                 "
        - "L1.13[600,700] 60s                                           |-----L1.13------|                                    "
        - "L1.12[400,500] 60s       |-----L1.12------|                                                                        "
        "###
        );

        // size limit < total size --> only enough to compact L0.1 with L1.12
        let keep_and_split_or_compact = limit_files_to_compact(
            MAX_COUNT,
            MAX_SIZE * 3,
            files,
            CompactionLevel::FileNonOverlapped,
        );

        let files_to_compact = keep_and_split_or_compact.files_to_compact();
        let files_to_further_split = keep_and_split_or_compact.files_to_further_split();
        let files_to_keep = keep_and_split_or_compact.files_to_keep();

        assert_eq!(files_to_compact.len(), 2);
        assert_eq!(files_to_further_split.len(), 0);
        assert_eq!(files_to_keep.len(), 3);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact:", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact:"
        - "L0, all files 100b                                                                                                 "
        - "L0.1[450,550] 120s                                     |---------------------------L0.1---------------------------|"
        - "L1, all files 100b                                                                                                 "
        - "L1.12[400,500] 60s       |--------------------------L1.12---------------------------|                              "
        - "files to keep:"
        - "L0, all files 100b                                                                                                 "
        - "L0.2[650,750] 180s                      |------------L0.2------------|                                             "
        - "L0.3[800,900] 300s                                                                   |------------L0.3------------|"
        - "L1, all files 100b                                                                                                 "
        - "L1.13[600,700] 60s       |-----------L1.13------------|                                                            "
        "###
        );
    }

    #[test]
    fn test_compact_files_limit_2() {
        let files = create_overlapped_l0_l1_files_3(MAX_SIZE as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 100b                                                                                                 "
        - "L0.2[650,750] 180s                                                    |------L0.2------|                           "
        - "L0.1[450,550] 120s                |------L0.1------|                                                               "
        - "L0.3[800,900] 300s                                                                               |------L0.3------|"
        - "L1, all files 100b                                                                                                 "
        - "L1.13[600,700] 60s                                           |-----L1.13------|                                    "
        - "L1.12[400,500] 60s       |-----L1.12------|                                                                        "
        "###
        );

        // size limit < total size --> only enough to compact L0.1, L0.2 with L1.12 and L1.13
        let keep_and_split_or_compact = limit_files_to_compact(
            MAX_COUNT,
            MAX_SIZE * 4,
            files,
            CompactionLevel::FileNonOverlapped,
        );

        let files_to_compact = keep_and_split_or_compact.files_to_compact();
        let files_to_further_split = keep_and_split_or_compact.files_to_further_split();
        let files_to_keep = keep_and_split_or_compact.files_to_keep();

        assert_eq!(files_to_compact.len(), 4);
        assert_eq!(files_to_further_split.len(), 0);
        assert_eq!(files_to_keep.len(), 1);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact:", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact:"
        - "L0, all files 100b                                                                                                 "
        - "L0.1[450,550] 120s                   |---------L0.1----------|                                                     "
        - "L0.2[650,750] 180s                                                                       |---------L0.2----------| "
        - "L1, all files 100b                                                                                                 "
        - "L1.12[400,500] 60s       |---------L1.12---------|                                                                 "
        - "L1.13[600,700] 60s                                                          |---------L1.13---------|              "
        - "files to keep:"
        - "L0, all files 100b                                                                                                 "
        - "L0.3[800,900] 300s       |------------------------------------------L0.3------------------------------------------|"
        "###
        );
    }

    #[test]
    fn test_compact_files_limit_3() {
        let files = create_overlapped_start_target_files(MAX_SIZE as i64, CompactionLevel::Initial);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 100b                                                                                                 "
        - "L0.2[550,650] 180s                                                                                |-----L0.2-----| "
        - "L0.1[150,250] 120s               |-----L0.1-----|                                                                  "
        - "L0.3[350,450] 300s                                               |-----L0.3-----|                                  "
        - "L1, all files 100b                                                                                                 "
        - "L1.12[300,400] 60s                                       |----L1.12-----|                                          "
        - "L1.13[500,600] 60s                                                                        |----L1.13-----|         "
        - "L1.11[100,200] 60s       |----L1.11-----|                                                                          "
        "###
        );

        // There are only 4 choices:
        //  1. Smallest set is still too large to compact. Split the set: L0.1 with L1.11
        //  2. Smallest set to compact: L0.1 with L1.11
        //  3. Medium size set to compact: L0.1, L0.2 with L1.11, L1.12, L1.13
        //  4. All files to compact: L0.1, L0.2, L0.3 with L1.11, L1.12, L1.13

        // --------------------
        // size limit = MAX_SIZE  to force the first choice: splitting L0.1 with L1.11
        let keep_and_split_or_compact = limit_files_to_compact(
            MAX_COUNT,
            MAX_SIZE,
            files.clone(),
            CompactionLevel::FileNonOverlapped,
        );

        let files_to_compact = keep_and_split_or_compact.files_to_compact();
        let files_to_further_split = keep_and_split_or_compact.files_to_further_split();
        let files_to_keep = keep_and_split_or_compact.files_to_keep();

        assert_eq!(files_to_compact.len(), 0);
        assert_eq!(files_to_further_split.len(), 2);
        assert_eq!(files_to_keep.len(), 4);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to further split:", &files_to_further_split, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to further split:"
        - "L0, all files 100b                                                                                                 "
        - "L0.1[150,250] 120s                                     |---------------------------L0.1---------------------------|"
        - "L1, all files 100b                                                                                                 "
        - "L1.11[100,200] 60s       |--------------------------L1.11---------------------------|                              "
        - "files to keep:"
        - "L0, all files 100b                                                                                                 "
        - "L0.2[550,650] 180s                                                                       |---------L0.2----------| "
        - "L0.3[350,450] 300s                   |---------L0.3----------|                                                     "
        - "L1, all files 100b                                                                                                 "
        - "L1.12[300,400] 60s       |---------L1.12---------|                                                                 "
        - "L1.13[500,600] 60s                                                          |---------L1.13---------|              "
        "###
        );

        // --------------------
        // size limit = MAX_SIZE * 3 to force the second choice, L0.1 with L1.11
        let keep_and_split_or_compact = limit_files_to_compact(
            MAX_COUNT,
            MAX_SIZE * 3,
            files.clone(),
            CompactionLevel::FileNonOverlapped,
        );

        let files_to_compact = keep_and_split_or_compact.files_to_compact();
        let files_to_further_split = keep_and_split_or_compact.files_to_further_split();
        let files_to_keep = keep_and_split_or_compact.files_to_keep();

        assert_eq!(files_to_compact.len(), 2);
        assert_eq!(files_to_further_split.len(), 0);
        assert_eq!(files_to_keep.len(), 4);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact:", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact:"
        - "L0, all files 100b                                                                                                 "
        - "L0.1[150,250] 120s                                     |---------------------------L0.1---------------------------|"
        - "L1, all files 100b                                                                                                 "
        - "L1.11[100,200] 60s       |--------------------------L1.11---------------------------|                              "
        - "files to keep:"
        - "L0, all files 100b                                                                                                 "
        - "L0.2[550,650] 180s                                                                       |---------L0.2----------| "
        - "L0.3[350,450] 300s                   |---------L0.3----------|                                                     "
        - "L1, all files 100b                                                                                                 "
        - "L1.12[300,400] 60s       |---------L1.12---------|                                                                 "
        - "L1.13[500,600] 60s                                                          |---------L1.13---------|              "
        "###
        );

        // --------------------
        // size limit = MAX_SIZE * 4 to force the second choice, L0.1 with L1.11, because it still not enough to for second choice

        let keep_and_split_or_compact = limit_files_to_compact(
            MAX_COUNT,
            MAX_SIZE * 4,
            files.clone(),
            CompactionLevel::FileNonOverlapped,
        );

        let files_to_compact = keep_and_split_or_compact.files_to_compact();
        let files_to_further_split = keep_and_split_or_compact.files_to_further_split();
        let files_to_keep = keep_and_split_or_compact.files_to_keep();

        assert_eq!(files_to_compact.len(), 2);
        assert_eq!(files_to_further_split.len(), 0);
        assert_eq!(files_to_keep.len(), 4);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact:", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact:"
        - "L0, all files 100b                                                                                                 "
        - "L0.1[150,250] 120s                                     |---------------------------L0.1---------------------------|"
        - "L1, all files 100b                                                                                                 "
        - "L1.11[100,200] 60s       |--------------------------L1.11---------------------------|                              "
        - "files to keep:"
        - "L0, all files 100b                                                                                                 "
        - "L0.2[550,650] 180s                                                                       |---------L0.2----------| "
        - "L0.3[350,450] 300s                   |---------L0.3----------|                                                     "
        - "L1, all files 100b                                                                                                 "
        - "L1.12[300,400] 60s       |---------L1.12---------|                                                                 "
        - "L1.13[500,600] 60s                                                          |---------L1.13---------|              "
        "###
        );

        // --------------------
        // size limit = MAX_SIZE * 5 to force the third choice, L0.1, L0.2 with L1.11, L1.12, L1.13
        let keep_and_split_or_compact = limit_files_to_compact(
            MAX_COUNT,
            MAX_SIZE * 5,
            files.clone(),
            CompactionLevel::FileNonOverlapped,
        );

        let files_to_compact = keep_and_split_or_compact.files_to_compact();
        let files_to_further_split = keep_and_split_or_compact.files_to_further_split();
        let files_to_keep = keep_and_split_or_compact.files_to_keep();

        assert_eq!(files_to_compact.len(), 5);
        assert_eq!(files_to_further_split.len(), 0);
        assert_eq!(files_to_keep.len(), 1);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact:", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact:"
        - "L0, all files 100b                                                                                                 "
        - "L0.1[150,250] 120s               |-----L0.1-----|                                                                  "
        - "L0.2[550,650] 180s                                                                                |-----L0.2-----| "
        - "L1, all files 100b                                                                                                 "
        - "L1.11[100,200] 60s       |----L1.11-----|                                                                          "
        - "L1.12[300,400] 60s                                       |----L1.12-----|                                          "
        - "L1.13[500,600] 60s                                                                        |----L1.13-----|         "
        - "files to keep:"
        - "L0, all files 100b                                                                                                 "
        - "L0.3[350,450] 300s       |------------------------------------------L0.3------------------------------------------|"
        "###
        );

        // --------------------
        // size limit >= total size to force the forth choice compacting everything:  L0.1, L0.2, L0.3 with L1.11, L1.12, L1.13
        let keep_and_split_or_compact = limit_files_to_compact(
            MAX_COUNT,
            MAX_SIZE * 6,
            files,
            CompactionLevel::FileNonOverlapped,
        );

        let files_to_compact = keep_and_split_or_compact.files_to_compact();
        let files_to_further_split = keep_and_split_or_compact.files_to_further_split();
        let files_to_keep = keep_and_split_or_compact.files_to_keep();

        assert_eq!(files_to_compact.len(), 6);
        assert_eq!(files_to_further_split.len(), 0);
        assert_eq!(files_to_keep.len(), 0);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact:", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact:"
        - "L0, all files 100b                                                                                                 "
        - "L0.1[150,250] 120s               |-----L0.1-----|                                                                  "
        - "L0.2[550,650] 180s                                                                                |-----L0.2-----| "
        - "L0.3[350,450] 300s                                               |-----L0.3-----|                                  "
        - "L1, all files 100b                                                                                                 "
        - "L1.11[100,200] 60s       |----L1.11-----|                                                                          "
        - "L1.12[300,400] 60s                                       |----L1.12-----|                                          "
        - "L1.13[500,600] 60s                                                                        |----L1.13-----|         "
        - "files to keep:"
        "###
        );
    }

    #[test]
    fn test_compact_files_limit_start_level_1() {
        let files = create_overlapped_start_target_files(
            MAX_SIZE as i64,
            CompactionLevel::FileNonOverlapped,
        );
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L1, all files 100b                                                                                                 "
        - "L1.2[550,650] 180s                                                                                |-----L1.2-----| "
        - "L1.1[150,250] 120s               |-----L1.1-----|                                                                  "
        - "L1.3[350,450] 300s                                               |-----L1.3-----|                                  "
        - "L2, all files 100b                                                                                                 "
        - "L2.12[300,400] 60s                                       |----L2.12-----|                                          "
        - "L2.13[500,600] 60s                                                                        |----L2.13-----|         "
        - "L2.11[100,200] 60s       |----L2.11-----|                                                                          "
        "###
        );

        // There are only 4 choices:
        //  1. Smallest set is still too large to compact. Split the set: L1.1 with L2.11
        //  2. Smallest set to compact: L1.1 with L2.11
        //  3. Medium size set to compact: L1.1, L1.3 with L1.11, L1.12,
        //  4. All files to compact: L1.1, L1.2, L1.3 with L2.11, L2.12, L2.13

        // --------------------
        // size limit = MAX_SIZE to force the first choice: splitting L1.1 & L2.11
        let keep_and_split_or_compact =
            limit_files_to_compact(MAX_COUNT, MAX_SIZE, files.clone(), CompactionLevel::Final);

        let files_to_compact = keep_and_split_or_compact.files_to_compact();
        let files_to_further_split = keep_and_split_or_compact.files_to_further_split();
        let files_to_keep = keep_and_split_or_compact.files_to_keep();

        assert_eq!(files_to_compact.len(), 0);
        assert_eq!(files_to_further_split.len(), 2);
        assert_eq!(files_to_keep.len(), 4);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to further split:", &files_to_further_split , "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to further split:"
        - "L1, all files 100b                                                                                                 "
        - "L1.1[150,250] 120s                                     |---------------------------L1.1---------------------------|"
        - "L2, all files 100b                                                                                                 "
        - "L2.11[100,200] 60s       |--------------------------L2.11---------------------------|                              "
        - "files to keep:"
        - "L1, all files 100b                                                                                                 "
        - "L1.3[350,450] 300s                   |---------L1.3----------|                                                     "
        - "L1.2[550,650] 180s                                                                       |---------L1.2----------| "
        - "L2, all files 100b                                                                                                 "
        - "L2.12[300,400] 60s       |---------L2.12---------|                                                                 "
        - "L2.13[500,600] 60s                                                          |---------L2.13---------|              "
        "###
        );

        // --------------------
        // size limit = MAX_SIZE * 3 to force the second choice,: compact L1.1 with L2.11
        let keep_and_split_or_compact = limit_files_to_compact(
            MAX_COUNT,
            MAX_SIZE * 3,
            files.clone(),
            CompactionLevel::Final,
        );

        let files_to_compact = keep_and_split_or_compact.files_to_compact();
        let files_to_further_split = keep_and_split_or_compact.files_to_further_split();
        let files_to_keep = keep_and_split_or_compact.files_to_keep();

        assert_eq!(files_to_compact.len(), 2);
        assert_eq!(files_to_further_split.len(), 0);
        assert_eq!(files_to_keep.len(), 4);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact:", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact:"
        - "L1, all files 100b                                                                                                 "
        - "L1.1[150,250] 120s                                     |---------------------------L1.1---------------------------|"
        - "L2, all files 100b                                                                                                 "
        - "L2.11[100,200] 60s       |--------------------------L2.11---------------------------|                              "
        - "files to keep:"
        - "L1, all files 100b                                                                                                 "
        - "L1.3[350,450] 300s                   |---------L1.3----------|                                                     "
        - "L1.2[550,650] 180s                                                                       |---------L1.2----------| "
        - "L2, all files 100b                                                                                                 "
        - "L2.12[300,400] 60s       |---------L2.12---------|                                                                 "
        - "L2.13[500,600] 60s                                                          |---------L2.13---------|              "
        "###
        );

        // --------------------
        // size limit = MAX_SIZE * 3 to force the second choice, compact L1.1 with L1.12, because it still not enough to for third choice
        let keep_and_split_or_compact = limit_files_to_compact(
            MAX_COUNT,
            MAX_SIZE * 3,
            files.clone(),
            CompactionLevel::Final,
        );

        let files_to_compact = keep_and_split_or_compact.files_to_compact();
        let files_to_further_split = keep_and_split_or_compact.files_to_further_split();
        let files_to_keep = keep_and_split_or_compact.files_to_keep();

        assert_eq!(files_to_compact.len(), 2);
        assert_eq!(files_to_further_split.len(), 0);
        assert_eq!(files_to_keep.len(), 4);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact:", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact:"
        - "L1, all files 100b                                                                                                 "
        - "L1.1[150,250] 120s                                     |---------------------------L1.1---------------------------|"
        - "L2, all files 100b                                                                                                 "
        - "L2.11[100,200] 60s       |--------------------------L2.11---------------------------|                              "
        - "files to keep:"
        - "L1, all files 100b                                                                                                 "
        - "L1.3[350,450] 300s                   |---------L1.3----------|                                                     "
        - "L1.2[550,650] 180s                                                                       |---------L1.2----------| "
        - "L2, all files 100b                                                                                                 "
        - "L2.12[300,400] 60s       |---------L2.12---------|                                                                 "
        - "L2.13[500,600] 60s                                                          |---------L2.13---------|              "
        "###
        );

        // --------------------
        // size limit = MAX_SIZE * 5 to force the third choice, L1.1, L1.2 with L2.11, L2.12, L2.13
        let keep_and_split_or_compact = limit_files_to_compact(
            MAX_COUNT,
            MAX_SIZE * 5,
            files.clone(),
            CompactionLevel::Final,
        );

        let files_to_compact = keep_and_split_or_compact.files_to_compact();
        let files_to_further_split = keep_and_split_or_compact.files_to_further_split();
        let files_to_keep = keep_and_split_or_compact.files_to_keep();

        assert_eq!(files_to_compact.len(), 4);
        assert_eq!(files_to_further_split.len(), 0);
        assert_eq!(files_to_keep.len(), 2);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact:", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact:"
        - "L1, all files 100b                                                                                                 "
        - "L1.1[150,250] 120s                   |---------L1.1----------|                                                     "
        - "L1.3[350,450] 300s                                                                       |---------L1.3----------| "
        - "L2, all files 100b                                                                                                 "
        - "L2.11[100,200] 60s       |---------L2.11---------|                                                                 "
        - "L2.12[300,400] 60s                                                          |---------L2.12---------|              "
        - "files to keep:"
        - "L1, all files 100b                                                                                                 "
        - "L1.2[550,650] 180s                                     |---------------------------L1.2---------------------------|"
        - "L2, all files 100b                                                                                                 "
        - "L2.13[500,600] 60s       |--------------------------L2.13---------------------------|                              "
        "###
        );

        // --------------------
        // size limit >= total size to force the forth choice compacting everything:  L1.1, L1.2, L1.3 with L2.11, L2.12, L2.13
        let keep_and_split_or_compact =
            limit_files_to_compact(MAX_COUNT, MAX_SIZE * 6, files, CompactionLevel::Final);

        let files_to_compact = keep_and_split_or_compact.files_to_compact();
        let files_to_further_split = keep_and_split_or_compact.files_to_further_split();
        let files_to_keep = keep_and_split_or_compact.files_to_keep();

        assert_eq!(files_to_compact.len(), 6);
        assert_eq!(files_to_further_split.len(), 0);
        assert_eq!(files_to_keep.len(), 0);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact:", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact:"
        - "L1, all files 100b                                                                                                 "
        - "L1.1[150,250] 120s               |-----L1.1-----|                                                                  "
        - "L1.3[350,450] 300s                                               |-----L1.3-----|                                  "
        - "L1.2[550,650] 180s                                                                                |-----L1.2-----| "
        - "L2, all files 100b                                                                                                 "
        - "L2.11[100,200] 60s       |----L2.11-----|                                                                          "
        - "L2.12[300,400] 60s                                       |----L2.12-----|                                          "
        - "L2.13[500,600] 60s                                                                        |----L2.13-----|         "
        - "files to keep:"
        "###
        );
    }
}
