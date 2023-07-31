use data_types::{CompactionLevel, ParquetFile, Timestamp};
use itertools::Itertools;
use observability_deps::tracing::debug;

use crate::{
    components::files_split::{target_level_split::TargetLevelSplit, FilesSplit},
    file_classification::{FileToSplit, SplitReason},
};

/// This function only takes action in scenarios with high backlog of overlapped L0s.
///
/// Return (`[files_to_split]`, `[files_not_to_split]`) of given files
/// such that `files_to_split` are files in potentially both levels, and overlap each other.
///
/// The returned `[files_to_split]` includes a set of pairs. A pair is composed of a file
///  and its corresponding split-times at which the file will be split into multiple files.
///
/// Example:
///  . Input: Many L0s heavily overlapping
//    - "L0.11[76,932] 1us 10mb         |-----------------------------------L0.11-----------------------------------|       "
//    - "L0.12[42,986] 1us 10mb      |---------------------------------------L0.12---------------------------------------|  "
//    - "L0.13[173,950] 1us 10mb                 |-------------------------------L0.13--------------------------------|     "
//    - "L0.14[50,629] 1us 10mb       |----------------------L0.14-----------------------|                                  "
//    - "L0.15[76,932] 1us 10mb         |-----------------------------------L0.15-----------------------------------|       "
//    - "L0.16[42,986] 1us 10mb      |---------------------------------------L0.16---------------------------------------|  "
//    - "L0.17[173,950] 1.01us 10mb               |-------------------------------L0.17--------------------------------|     "
//    - "L0.18[50,629] 1.01us 10mb    |----------------------L0.18-----------------------|                                  "
//  ... <pattern repeats>
//    - "L0.55[76,932] 1.04us 10mb      |-----------------------------------L0.55-----------------------------------|       "
//    - "L0.56[42,986] 1.05us 10mb   |---------------------------------------L0.56---------------------------------------|  "
//    - "L0.57[173,950] 1.05us 10mb               |-------------------------------L0.57--------------------------------|     "
//    - "L0.58[50,629] 1.05us 10mb    |----------------------L0.58-----------------------|                                  "
//    - "L0.59[76,932] 1.05us 10mb      |-----------------------------------L0.59-----------------------------------|       "
//    - "L0.60[42,986] 1.05us 10mb   |---------------------------------------L0.60---------------------------------------|  "
///
///  . Output:
///     split all L0 input files at: split(split_times=[248, 372, 496, 620, 744, 868]
///
/// Reason behind the split:
///  If this input was allowed to compact and split based on the algorithm focused on less
///  extensively overlapped files, it can produce overlapped L1s so large than they can
///  only be compacted 2-3 at a time, and then resplit at their prior boundaries.
///
///  Instead, this algorithm recognizes the large quantity of highly overlapped data and
///  performs a 'vertical split', instead of selecting bite sized set of files and splitting
///  just them, it selects all overlapping data and splits all of the files at the same
///  time(s).  A single split decision made by this function may split a quantity of files
///  many times larger than what we can compact in a single compaction.  But the split will
///  allow a future compaction to fit all the data for a relatively narrow time range.
///  The usefulness of this function's output relies on the chain identification in the
///  `ManySmallFiles` case of `divide` in multiple_branches.rs.
///
pub fn high_l0_overlap_split(
    max_compact_size: usize,
    mut files: Vec<ParquetFile>,
    target_level: CompactionLevel,
) -> (Vec<FileToSplit>, Vec<ParquetFile>, SplitReason) {
    let start_level = target_level.prev();
    let len = files.len();

    // This function focuses on many overlaps in a single level, which is only possible in L0.
    if start_level != CompactionLevel::Initial {
        return (vec![], files, SplitReason::HighL0OverlapSingleFile);
    }

    // We'll apply vertical splitting when the number of overlapped files is too large
    // for a single compaction.
    let total_cap: usize = files.iter().map(|f| f.file_size_bytes as usize).sum();

    if total_cap < max_compact_size {
        // Take no action, pass all files on to the next rule.
        return (vec![], files, SplitReason::HighL0OverlapSingleFile);
    }

    // panic if not all files are either in target level or start level
    assert!(files
        .iter()
        .all(|f| f.compaction_level == target_level || f.compaction_level == start_level));

    // Get files in start level that overlap with any file in target level
    let mut files_to_split = Vec::with_capacity(len);
    let mut overlaps: Vec<&ParquetFile> = Vec::with_capacity(len);
    let mut files_not_to_split = Vec::with_capacity(len);

    // We need to operate on the earliest ("left most") files first.
    files.sort_by_key(|f| f.max_l0_created_at);

    // This function currently applies a `vertical split` in two scenarios.
    // The second scenario is not sufficient on its own.  The first scenario may be adequate on its own (perhaps at
    // a lower threshold than 2xmax_compact_size).  We do get a little better efficiency by keeping both vertical
    // split methods, so scenario #2 can stay for now but may be removed later as further work is done on efficiency.
    // Vertical split scenario 1:
    if total_cap > 2 * max_compact_size {
        let chains = split_into_chains(files);

        for mut chain in chains {
            let chain_cap: usize = chain.iter().map(|f| f.file_size_bytes as usize).sum();

            if chain_cap <= max_compact_size {
                // Its a small chain, we can compact it, no need to split it.
                files_not_to_split.append(&mut chain);
            } else {
                // This chain is too big to compact on its own, so split it into smaller, more manageable chains.
                let min = chain.iter().map(|f| f.min_time.get()).min().unwrap();
                let max = chain.iter().map(|f| f.max_time.get()).max().unwrap();
                let split_times = select_split_times(chain_cap, max_compact_size, min, max);

                for file in &chain {
                    let this_file_splits: Vec<i64> = split_times
                        .iter()
                        .filter(|split| {
                            split >= &&file.min_time.get() && split < &&file.max_time.get()
                        })
                        .cloned()
                        .collect();

                    if !this_file_splits.is_empty() {
                        files_to_split.push(FileToSplit {
                            file: (*file).clone(),
                            split_times: this_file_splits,
                        });
                    } else {
                        // even though the chain this file is in needs split, this file falls between the splits.
                        files_not_to_split.push((*file).clone());
                    }
                }
            }
        }
        assert_eq!(files_to_split.len() + files_not_to_split.len(), len);

        return (
            files_to_split,
            files_not_to_split,
            SplitReason::HighL0OverlapTotalBacklog,
        );
    }

    // Vertical split scenario 2:
    // The files in `files` all overlap, but they might overlap in a chain (file1 overlaps file2, which overlaps file3...)
    // This algorithm is not concerned with the chain of overlaps that keeps pulling in more files.  Instead, for each file,
    // we'll look at what overlaps just it.  If that's over max_compact_size, we'll split them.
    for file in &files {
        if file.compaction_level != start_level {
            continue;
        }

        // Get the set of overlaping start_level files that includes file
        overlaps = files.iter().filter(|f| file.overlaps(f)).collect();

        let min = overlaps.iter().map(|f| f.min_time.get()).min().unwrap();
        let max = overlaps.iter().map(|f| f.max_time.get()).max().unwrap();

        let overlapped_cap: usize = overlaps.iter().map(|f| f.file_size_bytes as usize).sum();

        if overlapped_cap > max_compact_size {
            // Overlapping start_level files are too big to compact all once.  So we'll split all of them, as if
            // drawing a vertical line on the `input` in the above example.

            let split_times = select_split_times(overlapped_cap, max_compact_size, min, max);
            if split_times.is_empty() {
                overlaps = vec![];
                continue;
            }

            // for all the files in the overlapped set, if our chosen split times fall within the file, split it at the chosen time(s).
            for file in &overlaps {
                let this_file_splits: Vec<i64> = split_times
                    .iter()
                    .filter(|split| split >= &&file.min_time.get() && split < &&file.max_time.get())
                    .cloned()
                    .collect();

                if !this_file_splits.is_empty() {
                    files_to_split.push(FileToSplit {
                        file: (*file).clone(),
                        split_times: this_file_splits,
                    });
                } else {
                    files_not_to_split.push((*file).clone());
                }
            }

            break;
        }

        overlaps = vec![];
    }

    // Files in overlaps are already in files_to_split and files_not_to_split.  Everything else belongs in files_not_to_split.
    let start_leftovers: Vec<ParquetFile> = files
        .iter()
        .filter(|f| !overlaps.contains(f))
        .cloned()
        .collect();

    files_not_to_split.extend(start_leftovers);

    assert_eq!(files_to_split.len() + files_not_to_split.len(), len);

    (
        files_to_split,
        files_not_to_split,
        SplitReason::HighL0OverlapSingleFile,
    )
}

// selectSplitTimes returns an appropriate sets of split times to divide the given time range into,
// based on how much over the max_compact_size the capacity is.
// The assumption is that the caller has `cap` bytes spread across `min_time` to `max_time` and wants
// to split those bytes into chunks approximating `max_compact_size`.
// This function returns a vec of split times that assume the data is spread close to linearly across
// the time range, but padded to allow some deviation from an even distrubution of data.
// The cost of splitting into smaller pieces is minimal (potentially extra but smaller compactions),
// while the cost splitting into pieces that are too big is considerable (we may have split again).
pub fn select_split_times(
    cap: usize,
    max_compact_size: usize,
    min_time: i64,
    max_time: i64,
) -> Vec<i64> {
    if min_time == max_time {
        // can't split below 1 ns.
        return vec![];
    }

    // If the bytes are spread perfectly even across `min_time` to `max_time`, the ideal number of splits
    // would simply be cap / max_compact_size.
    let mut splits = cap / max_compact_size;

    // But the data won't be spread perfectly even across the time range, and its better err towards splitting
    // extra small rather than splitting extra large (which may still exceed max_compact_size).
    // So pad the split count beyond what a perfect distribution would require, by doubling it.
    splits *= 2;

    // Splitting the time range into `splits` pieces requires an increase between each split time of `delta`.
    let mut delta = (max_time - min_time) / (splits + 1) as i64;

    if delta == 0 {
        // The computed count leads to splitting at less than 1ns, which we cannot do.
        splits = (max_time - min_time) as usize; // The maximum number of splits possible for this time range.
        delta = 1; // The smallest time delta between splits possible for this time range.
    }

    let split_times: Vec<i64> = (1..splits + 1)
        .map(|i| min_time + (i as i64 * delta))
        .collect();

    split_times
}

// split_into_chains splits files into separate overlapping chains of files.
// A chain is a series of files that overlap.  Each file in the chain overlaps at least 1 neighbor, but all files
// in the chain may not overlap all other files in the chain.  A "chain" is identified by sorting by min_time.
// When the first file overlaps at least the next file.  As long as at least one prior file overlaps the next file,
// the chain continues.  When the next file (by min_time) does not overlap a prior file, the chain ends, and a new
// chain begins.
pub fn split_into_chains(mut files: Vec<ParquetFile>) -> Vec<Vec<ParquetFile>> {
    let mut left = files.len(); // how many files remain to consider
    let mut chains: Vec<Vec<ParquetFile>> = Vec::with_capacity(10);
    let mut chain: Vec<ParquetFile> = Vec::with_capacity(left);
    let mut max_time: Timestamp = Timestamp::new(0);

    files.sort_by_key(|f| f.min_time);

    for file in files.drain(..) {
        if chain.is_empty() {
            // first of new chain
            max_time = file.max_time;
            chain.push(file);
        } else if file.min_time <= max_time {
            // This overlaps the chain, add to it.
            if file.max_time > max_time {
                max_time = file.max_time;
            }
            chain.push(file);
        } else {
            // file does not overlap the chain, its the start of a new chain.
            max_time = file.max_time;
            chains.push(chain);
            chain = Vec::with_capacity(left);
            chain.push(file);
        }
        left -= 1;
    }
    chains.push(chain);
    chains
}

// merge_small_l0_chains takes a vector of overlapping "chains" (where a chain is vector of overlapping L0 files), and
// attempts to merge small chains together if doing so can keep them under the given max_compact_size.
// This function makes no assumption about the order of the chains - if they are created by `split_into_chains`, they're
// ordered by min_time, which is unsafe for merging L0 chains.
pub fn merge_small_l0_chains(
    mut chains: Vec<Vec<ParquetFile>>,
    max_compact_size: usize,
) -> Vec<Vec<ParquetFile>> {
    chains.sort_by_key(|a| get_max_l0_created_at(a.to_vec()));
    let mut merged_chains: Vec<Vec<ParquetFile>> = Vec::with_capacity(chains.len());
    let mut prior_chain_bytes: usize = 0;
    let mut prior_chain_idx: i32 = -1;
    for chain in &chains {
        let this_chain_bytes = chain.iter().map(|f| f.file_size_bytes as usize).sum();

        // matching max_lo_created_at times indicates that the files were deliberately split.  We shouldn't merge
        // chains with matching max_lo_created_at times, because that would encourage undoing the previous split,
        // which minimally increases write amplification, and may cause unproductive split/compact loops.
        let mut matches = 0;
        if prior_chain_bytes > 0 {
            for f in chain.iter() {
                for f2 in &merged_chains[prior_chain_idx as usize] {
                    if f.max_l0_created_at == f2.max_l0_created_at {
                        matches += 1;
                        break;
                    }
                }
            }
        }

        // Merge it if: there a prior chain to merge with, and merging wouldn't make it too big, or undo a previous split
        if prior_chain_bytes > 0
            && prior_chain_bytes + this_chain_bytes <= max_compact_size
            && matches == 0
        {
            // this chain can be added to the prior chain.
            merged_chains[prior_chain_idx as usize].append(&mut chain.clone());
            prior_chain_bytes += this_chain_bytes;
        } else {
            merged_chains.push(chain.to_vec());
            prior_chain_bytes = this_chain_bytes;
            prior_chain_idx += 1;
        }
    }

    merged_chains
}

// get_max_l0_created_at gets the highest max_l0_created_at from all files within a vec.
fn get_max_l0_created_at(files: Vec<ParquetFile>) -> Timestamp {
    files
        .into_iter()
        .map(|f| f.max_l0_created_at)
        .max()
        .unwrap()
}

/// Return (`[files_to_split]`, `[files_not_to_split]`) of given files
/// such that `files_to_split` are files  in start-level that overlaps with more than one file in target_level.
///
/// The returned `[files_to_split]` includes a set of pairs. A pair is composed of a file and its corresponding split-times
/// at which the file will be split into multiple files.
///
/// Unlike high_l0_overlap_split, this function focusses on scenarios where start level files are not highly overlapping.
/// Typically each file overlaps its neighbors, but each file does not overlap all or almost all L0.s
///
/// Example:
///  . Input:
///                          |---L0.1---|   |--L0.2--|
///            |--L1.1--| |--L1.2--| |--L1.3--|
///
///    L0.1 overlaps with 2 level-1 files (L1.2, L1.3) and should be split into 2 files, one overlaps with L1.2
///    and one oerlaps with L1.3
///
///  . Output:
///     . files_to_split = [L0.1]
///     . files_not_to_split = [L1.1, L1.2, L1.3, L0.2] which is the rest of the files
///
/// Reason behind the split:
/// Since a start-level file needs to compact with all of its overlapped target-level files to retain the invariant that
/// all files in target level are non-overlapped, splitting start-level files is to reduce the number of overlapped files
/// at the target level and avoid compacting too many files in the next compaction cycle.
/// To achieve this goal, a start-level file should be split to overlap with at most one target-level file. This enables the
/// minimum set of compacting files to 2 files: a start-level file and an overlapped target-level file.
///
pub fn identify_start_level_files_to_split(
    files: Vec<ParquetFile>,
    target_level: CompactionLevel,
) -> (Vec<FileToSplit>, Vec<ParquetFile>) {
    // panic if not all files are either in target level or start level
    let start_level = target_level.prev();
    assert!(files
        .iter()
        .all(|f| f.compaction_level == target_level || f.compaction_level == start_level));

    // Get start-level and target-level files
    let len = files.len();
    let split = TargetLevelSplit::new();
    let (mut start_level_files, mut target_level_files) = split.apply(files, start_level);

    // sort start_level files in their max_l0_created_at
    start_level_files.sort_by_key(|f| f.max_l0_created_at);
    // sort target level files in their min_time
    target_level_files.sort_by_key(|f| f.min_time);

    // Get files in start level that overlap with any file in target level
    let mut files_to_split = Vec::with_capacity(len);
    let mut files_not_to_split = Vec::with_capacity(len);
    for file in start_level_files {
        // Get target_level files that overlaps with this file
        let overlapped_target_level_files: Vec<&ParquetFile> = target_level_files
            .iter()
            .filter(|f| file.overlaps(f))
            .collect();

        // Neither split file that overlaps with only one file in target level
        // nor has a single timestamp (splitting this will lead to the same file and as a result will introduce infinite loop)
        // nor has time range = 1 (splitting this will cause panic because split_time will be min_tim/max_time which is disallowed)
        if overlapped_target_level_files.len() < 2 || file.min_time == file.max_time {
            files_not_to_split.push(file);
        } else {
            debug!(?file.min_time, ?file.max_time, ?file.compaction_level, "time range of file to split");
            overlapped_target_level_files
                .iter()
                .for_each(|f| debug!(?f.min_time, ?f.max_time, ?f.compaction_level, "time range of overlap file"));

            // this files will be split, add its max time
            let split_times: Vec<i64> = overlapped_target_level_files
                .iter()
                .filter(|f| f.max_time < file.max_time)
                .map(|f| f.max_time.get())
                .dedup()
                .collect();

            debug!(?split_times);

            files_to_split.push(FileToSplit { file, split_times });
        }
    }

    // keep the rest of the files for next round
    files_not_to_split.extend(target_level_files);

    assert_eq!(files_to_split.len() + files_not_to_split.len(), len);

    (files_to_split, files_not_to_split)
}

#[cfg(test)]
mod tests {
    use compactor_test_utils::{
        create_l1_files, create_overlapped_files, create_overlapped_l0_l1_files_2, format_files,
        format_files_split,
    };
    use data_types::CompactionLevel;

    #[test]
    fn test_select_split_times() {
        // First some normal cases:

        // splitting 150 bytes based on a max of 100, with a time range 0-100, gives 2 splits, into 3 pieces.
        // 1 split into 2 pieces would have also been ok.
        let mut split_times = super::select_split_times(150, 100, 0, 100);
        assert!(split_times == vec![33, 66]);

        // splitting 199 bytes based on a max of 100, with a time range 0-100, gives 2 splits, into 3 pieces.
        // 1 split into 2 pieces would be a bad choice - its any deviation from perfectly linear distribution
        // would cause the split range to still exceed the max.
        split_times = super::select_split_times(199, 100, 0, 100);
        assert!(split_times == vec![33, 66]);

        // splitting 200-299 bytes based on a max of 100, with a time range 0-100, gives 4 splits into 5 pieces.
        // A bit agressive for exactly 2x the max cap, but very reasonable for 1 byte under 3x.
        split_times = super::select_split_times(200, 100, 0, 100);
        assert!(split_times == vec![20, 40, 60, 80]);
        split_times = super::select_split_times(299, 100, 0, 100);
        assert!(split_times == vec![20, 40, 60, 80]);

        // splitting 300-399 bytes based on a max of 100, with a time range 0-100, gives 5 splits, 6 pieces.
        // A bit agressive for exactly 3x the max cap, but very reasonable for 1 byte under 4x.
        split_times = super::select_split_times(300, 100, 0, 100);
        assert!(split_times == vec![14, 28, 42, 56, 70, 84]);
        split_times = super::select_split_times(399, 100, 0, 100);
        assert!(split_times == vec![14, 28, 42, 56, 70, 84]);

        // splitting 400 bytes based on a max of 100, with a time range 0-100, gives 7 splits, 8 pieces.
        split_times = super::select_split_times(400, 100, 0, 100);
        assert!(split_times == vec![11, 22, 33, 44, 55, 66, 77, 88]);

        // Now some pathelogical cases:

        // splitting 400 bytes based on a max of 100, with a time range 0-3, gives 3 splits, into 4 pieces.
        // Some (maybe all) of these will still exceed the max, but this is the most splitting possible for
        // the time range.
        split_times = super::select_split_times(400, 100, 0, 3);
        assert!(split_times == vec![1, 2, 3]);
    }

    #[test]
    fn test_split_empty() {
        let files = vec![];
        let (files_to_split, files_not_to_split) =
            super::identify_start_level_files_to_split(files, CompactionLevel::Initial);
        assert!(files_to_split.is_empty());
        assert!(files_not_to_split.is_empty());
    }

    #[test]
    #[should_panic]
    fn test_split_files_wrong_target_level() {
        // all L1 files
        let files = create_l1_files(1);

        // Target is L0 while all files are in L1 --> panic
        let (_files_to_split, _files_not_to_split) =
            super::identify_start_level_files_to_split(files, CompactionLevel::Initial);
    }

    #[test]
    #[should_panic]
    fn test_split_files_three_level_files() {
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
        let (_files_to_split, _files_not_to_split) =
            super::identify_start_level_files_to_split(files, CompactionLevel::FileNonOverlapped);
    }

    #[test]
    fn test_split_files_no_split() {
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

        let (files_to_split, files_not_to_split) =
            super::identify_start_level_files_to_split(files, CompactionLevel::FileNonOverlapped);
        assert!(files_to_split.is_empty());
        assert_eq!(files_not_to_split.len(), 3);
    }

    #[test]
    fn test_split_files_split() {
        let files = create_overlapped_l0_l1_files_2(1);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 1b                                                                                                   "
        - "L0.2[650,750] 180s                                                    |------L0.2------|                           "
        - "L0.1[450,620] 120s                |------------L0.1------------|                                                   "
        - "L0.3[800,900] 300s                                                                               |------L0.3------|"
        - "L1, all files 1b                                                                                                   "
        - "L1.13[600,700] 60s                                           |-----L1.13------|                                    "
        - "L1.12[400,500] 60s       |-----L1.12------|                                                                        "
        "###
        );

        let (files_to_split, files_not_to_split) =
            super::identify_start_level_files_to_split(files, CompactionLevel::FileNonOverlapped);

        // L0.1 that overlaps with 2 level-1 files will be split
        assert_eq!(files_to_split.len(), 1);

        // L0.1 [450, 620] will be split at 500 (max of its overlapped L1.12)
        // The spit_times [500] means after we execute the split (in later steps), L0.1 will
        // be split into 2 files with time ranges: [450, 500] and [501, 620]. This means the first file will
        // overlap with L1.12 and the second file will overlap with L1.13
        assert_eq!(files_to_split[0].file.id.get(), 1);
        assert_eq!(files_to_split[0].split_times, vec![500]);

        // The rest is in not-split
        assert_eq!(files_not_to_split.len(), 4);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to split:", &files_to_split.iter().map(|f| f.file.clone()).collect::<Vec<_>>(), "files not to split:", &files_not_to_split),
            @r###"
        ---
        - "files to split:"
        - "L0, all files 1b                                                                                                   "
        - "L0.1[450,620] 120s       |------------------------------------------L0.1------------------------------------------|"
        - "files not to split:"
        - "L0, all files 1b                                                                                                   "
        - "L0.2[650,750] 180s                                                    |------L0.2------|                           "
        - "L0.3[800,900] 300s                                                                               |------L0.3------|"
        - "L1, all files 1b                                                                                                   "
        - "L1.12[400,500] 60s       |-----L1.12------|                                                                        "
        - "L1.13[600,700] 60s                                           |-----L1.13------|                                    "
        "###
        );
    }
}
