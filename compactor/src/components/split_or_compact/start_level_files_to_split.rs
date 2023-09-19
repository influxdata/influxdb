use data_types::{CompactionLevel, FileRange, ParquetFile, Timestamp, TransitionPartitionId};
use itertools::Itertools;
use observability_deps::tracing::debug;

use crate::{
    components::files_split::{target_level_split::TargetLevelSplit, FilesSplit},
    file_classification::FileToSplit,
};

// selectSplitTimes returns an appropriate sets of split times to divide the given time range into,
// based on how much over the max_compact_size the capacity is.
// The assumption is that the caller has `cap` bytes spread across `min_time` to `max_time` and wants
// to split those bytes into chunks approximating `max_compact_size`.  We don't know if the data is
// spread linearly, so we'll split into more pieces than would be necesary if the data is linear.
// The cost of splitting into smaller pieces is minimal (potentially extra but smaller compactions),
// while the cost splitting into pieces that are too big is considerable (we may have split again).
// A vec of split_hints can be provided, which is assumed to be the min/max file times of the target
// level files.  When hints are specified, this function will try to split at the hint times, if they're
// +/- 50% the computed split times.
pub fn select_split_times(
    cap: usize,
    max_compact_size: usize,
    min_time: i64,
    max_time: i64,
    split_hint: Vec<i64>,
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
    let mut default_delta = (max_time - min_time) / (splits + 1) as i64;
    let mut min_delta = default_delta / 2; // used to decide how far we'll deviate to align to split_hint
    let mut max_delta = default_delta * 3 / 2; // used to decide how far we'll deviate to align to split_hint

    if default_delta == 0 {
        // The computed count leads to splitting at less than 1ns, which we cannot do.
        splits = (max_time - min_time) as usize; // The maximum number of splits possible for this time range.
        default_delta = 1; // The smallest time delta between splits possible for this time range.
    }
    min_delta = min_delta.max(1);
    max_delta = max_delta.max(1);

    let mut split_time = min_time;
    let mut split_times = Vec::with_capacity(splits);
    let mut hint_idx = 0;

    // Allow max delta at the end so we don't split at the very end of the time range, resulting in tiny final time slice.
    while split_time + max_delta < max_time {
        // advance to the next possible hint
        while hint_idx < split_hint.len() && split_hint[hint_idx] < split_time + min_delta {
            hint_idx += 1;
        }

        // if there's multiple hints we could use for the next split time, chose the closest one to our default delta.
        let default_next = split_time + default_delta;
        while hint_idx + 1 < split_hint.len()
            && (split_hint[hint_idx] - default_next).abs()
                > (split_hint[hint_idx + 1] - default_next).abs()
        {
            hint_idx += 1;
        }

        split_time = if hint_idx < split_hint.len() && split_hint[hint_idx] < split_time + max_delta
        {
            // The next hint is close enough to the next split that we'll use it instead of the computed split.
            split_hint[hint_idx]
        } else {
            // There is no next hint, or its too far away, so go with the default.
            default_next
        };

        if split_time < max_time {
            split_times.push(split_time);
        }
    }

    split_times
}

// linear_dist_ranges detects non-linear distribution of the data, and if found tries to identify time ranges that
// have approximately linear distribution of data within them.  The intent is to prevent vertical splitting from
// making bad split time decisions because it assumes data is spread linearly across the time range.
// Fluctuations in data density (bytes per ns) that are smaller than the max_compact_size are ignored.
pub fn linear_dist_ranges(
    chain: &Vec<ParquetFile>, // not just any vec of files, these are overlapping L0 files.
    cap: usize,
    max_compact_size: usize,
    partition: TransitionPartitionId,
) -> Vec<FileRange> {
    let min_time = chain.iter().map(|f| f.min_time.get()).min().unwrap();
    let max_time = chain.iter().map(|f| f.max_time.get()).max().unwrap();

    // assume we're splitting to half the max size.
    let mut split_count = cap / (max_compact_size / 2);
    let mut region_caps: Vec<usize> = vec![0; split_count];
    let mut delta_time_interval: i64;

    if split_count < 2 {
        // there isn't much splitting necessary, so there isn't much opportunity for non-linear distribution
        return vec![FileRange {
            min: min_time,
            max: max_time,
            cap,
        }];
    }

    // Step 1: loop until we've found a distribution that's linear enough.
    loop {
        // Pretend we're splitting the files into split_count regions.
        // Assuming the data is evenly distributed within each file, we can estimate the bytes in each region.

        delta_time_interval = (max_time - min_time) / split_count as i64;
        if delta_time_interval < 1 {
            break;
        }

        // Given our split_count & time delta, compute each file's size contribution to each region.
        // Each file's contribtuion to the region capacity is added to region_caps.
        for f in chain {
            let f_min = f.min_time.get();
            let f_max = f.max_time.get();
            let f_cap = f.file_size_bytes;

            // because delta is a whole integer, we can lose a few nanoseconds of time.  So we have to check if the
            // computed regions are greater than the max region (the last region may get a few extra nanoseconds).
            let first_region_idx =
                ((f_min - min_time) / delta_time_interval).min(split_count as i64 - 1);
            let mut last_region_idx =
                ((f_max - min_time - 1) / delta_time_interval).min(split_count as i64 - 1);
            if first_region_idx > last_region_idx {
                // rounding error on single nanosecond region.
                last_region_idx = first_region_idx;
            }
            assert!(
                first_region_idx >= 0,
                "invalid first region, partition_id={partition}"
            );
            assert!(
                last_region_idx >= 0,
                "invalid last region, partition_id={partition}"
            );

            if first_region_idx == last_region_idx {
                // this file is entirely within one region
                region_caps[first_region_idx as usize] += f_cap as usize;
                continue;
            } else {
                // This file spans multiple regions.  Sort out how many bytes go each region `f` is in.
                // The first & last region have a partial share of the capacity, proportional to how many ns
                // of the region they cover, and the middle regions get an equal share.
                let first_region_start = min_time + first_region_idx * delta_time_interval;
                let last_region_start = min_time + last_region_idx * delta_time_interval;
                assert!(
                    first_region_start <= f_min,
                    "invalid first_region_start, partition_id={partition}"
                );
                assert!(
                    last_region_start <= f_max,
                    "invalid last_region_start, partition_id={partition}"
                );
                let total_time = f_max - f_min + 1;
                let time_in_first_region = first_region_start + delta_time_interval - f_min;
                let time_in_last_region = f_max - last_region_start + 1;

                assert!(
                    time_in_first_region > 0,
                    "invalid time_in_first_region, partition_id={partition}"
                );
                assert!(
                    time_in_last_region > 0,
                    "invalid time_in_last_region, partition_id={partition}"
                );
                let first_region_cap =
                    (f_cap as i128 * time_in_first_region as i128 / total_time as i128) as i64;
                let last_region_cap =
                    (f_cap as i128 * time_in_last_region as i128 / total_time as i128) as i64;
                assert!(
                    first_region_cap >= 0,
                    "invalid first_region_cap, partition_id={partition}"
                );
                assert!(
                    last_region_cap >= 0,
                    "invalid last_region_cap, partition_id={partition}"
                );

                region_caps[first_region_idx as usize] += first_region_cap as usize;
                region_caps[last_region_idx as usize] += last_region_cap as usize;

                if last_region_idx > first_region_idx + 1 {
                    // this file spans at least 3 regions.  The middle regions all get an equal share of the capacity.
                    let mid_region_cap = (f_cap - first_region_cap - last_region_cap)
                        / (last_region_idx - first_region_idx - 1);
                    for i in first_region_idx + 1..last_region_idx {
                        region_caps[i as usize] += mid_region_cap as usize;
                    }
                }
            }
        }

        // The above estimates how many bytes are in each of the hypothetical regions.
        // Now we need to decide if this is linear enough.
        let min_region_cap = region_caps.iter().min().unwrap();
        let max_region_cap = region_caps.iter().max().unwrap();
        if *min_region_cap * 3 / 2 > *max_region_cap
            || *max_region_cap < max_compact_size / 2
            || delta_time_interval == 1
        {
            // Either the distribution is sufficiently linear (min & max are close together), or the regions are small enough
            // we can deal with the non-linearity, or we're as small as we can go.
            break;
        }
        if split_count > 10000 {
            // this is getting expenisve to compute.  We can use these regions and split again if required.
            break;
        }

        // retry with smaller regions.  Eventually we'll get regions small enough we can isolate and deal with the non-linearity.
        split_count *= 2;
        region_caps = vec![0; split_count];
    }

    // Our hypothetical regions are either linearly distributed, or small enough that the capacity spikes are still under max compact size.
    // Now we can attempt to consoliate regions of similar data density (or consolidate dissimilar regions up to max compact size).
    let mut ranges = Vec::with_capacity(split_count);
    let mut consolidated_min_time: i64 = 0;
    let mut consolidated_max_time: i64 = 0;
    let mut consolidated_total_cap: usize = 0;
    let mut consolidated_region_cnt = 0;
    for (i, this_region_cap) in region_caps.iter().enumerate().take(split_count) {
        let this_min_time = min_time + i as i64 * delta_time_interval;
        let this_max_time = min_time + (i as i64 + 1) * delta_time_interval - 1;

        if consolidated_region_cnt > 0 {
            // Determin if region 'i' is appropriate to consolidate with the prior region(s).

            // To guide us on whether or not to consolidate this region with the prior regions, we'll compute
            // the data "density" (bytes per ns) for this region and the prior region(s).  If the density is
            // close enough, we'll consolidate.
            let this_time_range = this_max_time - this_min_time + 1;
            let this_density = *this_region_cap as f64 / this_time_range as f64;
            let prior_time_range = consolidated_max_time - consolidated_min_time + 1;
            let prior_density = consolidated_total_cap as f64 / prior_time_range as f64;
            let relative_density = this_density / prior_density;

            let cap_if_merged = consolidated_total_cap + this_region_cap;

            // Combile region 'i' with the prior regions if the density is close, the size is small,
            // or the density is sorta small and size is sorta small.
            if (relative_density > 0.5 && relative_density < 1.5)
                || cap_if_merged < max_compact_size
                || (cap_if_merged < max_compact_size * 2
                    && relative_density > 0.3
                    && relative_density < 2.0)
                || (this_region_cap * 5 < consolidated_total_cap
                    && relative_density > 0.3
                    && relative_density < 2.0)
            {
                // its linear enough
                // This region can be consolidated with the prior region(s).
                consolidated_max_time = this_max_time;
                consolidated_total_cap += this_region_cap;
                consolidated_region_cnt += 1;
                continue;
            }

            // region 'i' is not appropriate to consolidate with the prior region(s).
            // The prior regions will be returned as a file range, and 'i' will start
            // a new range below.
            ranges.push(FileRange {
                min: consolidated_min_time,
                max: consolidated_max_time,
                cap: consolidated_total_cap,
            });
        }

        // region 'i' is the start of the next region.
        consolidated_min_time = this_min_time;
        consolidated_max_time = this_max_time;
        consolidated_total_cap = *this_region_cap;
        consolidated_region_cnt = 1;
    }
    if consolidated_region_cnt > 0 {
        ranges.push(FileRange {
            min: consolidated_min_time,
            max: consolidated_max_time,
            cap: consolidated_total_cap,
        });
    }

    ranges
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
    let mut prior_chain_min: Timestamp = Timestamp::new(0);
    let mut prior_chain_max: Timestamp = Timestamp::new(0);
    for chain in &chains {
        if chain.is_empty() {
            continue;
        }
        let this_chain_bytes = chain.iter().map(|f| f.file_size_bytes as usize).sum();
        let this_chain_min = chain.iter().map(|f| f.min_time).min().unwrap();
        let this_chain_max = chain.iter().map(|f| f.max_time).max().unwrap();

        // matching max_lo_created_at times indicates that the files were deliberately split.  We shouldn't merge
        // chains with matching max_lo_created_at times, because that would encourage undoing the previous split,
        // which minimally increases write amplification, and may cause unproductive split/compact loops.
        // TODO: this may not be necessary long term (with CompactRanges this might be ok)
        let mut matches = 0;
        if prior_chain_bytes > 0 {
            for f in chain {
                for f2 in &merged_chains[prior_chain_idx as usize] {
                    if f.max_l0_created_at == f2.max_l0_created_at {
                        matches += 1;
                        break;
                    }
                }
            }
        }

        // These chains must be merged based on max_l0_created_at (to protect dedup), so its possible the sort order differs
        // from min_time sorted chains.  We can't merge chains if doing so would make the resulting chain overlap another
        // chain without including it.
        let mut ooo_overlapped_chains = false;
        if prior_chain_bytes > 0 {
            for alt_chain in &chains {
                // if alt_chain is not already in prior_chain, we need to investigate further
                if alt_chain[0].min_time < prior_chain_min
                    || alt_chain[0].min_time > prior_chain_max
                {
                    // if alt_chain is not already in chain, we need to investigate further
                    if alt_chain[0].min_time < this_chain_min
                        || alt_chain[0].min_time > this_chain_max
                    {
                        // alt_chain isn't in prior_chain or chain.  If alt_chain would overlap the merged result
                        // of the other two, we can't assume it will be included.  So we can't merge them.
                        let combined_min = prior_chain_min.min(this_chain_min);
                        let combined_max = prior_chain_max.max(this_chain_max);
                        if alt_chain[0].min_time >= combined_min
                            && alt_chain[0].min_time <= combined_max
                        {
                            ooo_overlapped_chains = true;
                        }
                    }
                }
            }
        }

        // Merge it if: there a prior chain to merge with, and merging wouldn't make it too big, or undo a previous split
        if prior_chain_bytes > 0
            && prior_chain_bytes + this_chain_bytes <= max_compact_size
            && matches == 0
            && !ooo_overlapped_chains
        {
            // this chain can be added to the prior chain.
            merged_chains[prior_chain_idx as usize].append(&mut chain.clone());
            prior_chain_bytes += this_chain_bytes;
            prior_chain_min = prior_chain_min.min(this_chain_min);
            prior_chain_max = prior_chain_max.max(this_chain_max);
        } else {
            merged_chains.push(chain.to_vec());
            prior_chain_bytes = this_chain_bytes;
            prior_chain_idx += 1;
            prior_chain_min = this_chain_min;
            prior_chain_max = this_chain_max;
        }
    }

    // Put it back in the standard order.
    merged_chains.sort_by_key(|a| a[0].min_time);

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
    partition: TransitionPartitionId,
) -> (Vec<FileToSplit>, Vec<ParquetFile>) {
    let start_level = target_level.prev();
    assert!(files
        .iter()
        .all(|f| f.compaction_level == target_level || f.compaction_level == start_level),
        "all files to compact must be in either {start_level} or {target_level}, but found files in other levels, partition_id={partition}"
    );

    // Get start-level and target-level files
    let len = files.len();
    let split = TargetLevelSplit::new();
    let (mut start_level_files, mut target_level_files) =
        split.apply(files, start_level, partition.clone());

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

    assert_eq!(
        files_to_split.len() + files_not_to_split.len(),
        len,
        "all files should be accounted after file divisions, expected {len} files but found {} files, partition_id={}",
        files_to_split.len() + files_not_to_split.len(),
        partition
    );

    (files_to_split, files_not_to_split)
}

#[cfg(test)]
mod tests {
    use compactor_test_utils::{
        create_fake_partition_id, create_l1_files, create_overlapped_files,
        create_overlapped_l0_l1_files_2, format_files, format_files_split, format_ranges,
    };
    use data_types::{CompactionLevel, ParquetFile};
    use iox_tests::ParquetFileBuilder;

    #[test]
    fn test_select_split_times() {
        // First some normal cases:

        // splitting 150 bytes based on a max of 100, with a time range 0-100, gives 2 splits, into 3 pieces.
        // 1 split into 2 pieces would have also been ok.
        let mut split_times = super::select_split_times(150, 100, 0, 100, vec![]);
        assert!(split_times == vec![33, 66]);
        // give it hints (overlapping L1s) that are close to the splits it choses by default, and it will use them.
        split_times = super::select_split_times(150, 100, 0, 100, vec![30, 65]);
        assert!(split_times == vec![30, 65]);
        // give it hints (overlapping L1s) that are far the splits it choses by default, and it sticks with the default.
        split_times = super::select_split_times(150, 100, 0, 100, vec![10, 95]);
        assert!(split_times == vec![33, 66]);

        // splitting 199 bytes based on a max of 100, with a time range 0-100, gives 2 splits, into 3 pieces.
        // 1 split into 2 pieces would be a bad choice - its any deviation from perfectly linear distribution
        // would cause the split range to still exceed the max.
        split_times = super::select_split_times(199, 100, 0, 100, vec![]);
        assert!(split_times == vec![33, 66]);

        // splitting 200-299 bytes based on a max of 100, with a time range 0-100, gives 4 splits into 5 pieces.
        // A bit agressive for exactly 2x the max cap, but very reasonable for 1 byte under 3x.
        split_times = super::select_split_times(200, 100, 0, 100, vec![]);
        assert!(split_times == vec![20, 40, 60, 80]);
        split_times = super::select_split_times(299, 100, 0, 100, vec![]);
        assert!(split_times == vec![20, 40, 60, 80]);
        // once a hint shifts the split times, the rest of the split times are shifted too.
        split_times = super::select_split_times(299, 100, 0, 100, vec![43]);
        assert!(split_times == vec![20, 43, 63, 83]);
        // give it a lot of hints, and see it pick the best (closest) ones.
        split_times =
            super::select_split_times(299, 100, 0, 100, vec![15, 19, 23, 35, 41, 55, 61, 82, 83]);
        assert!(split_times == vec![19, 41, 61, 82]);

        // splitting 300-399 bytes based on a max of 100, with a time range 0-100, gives 5 splits, 6 pieces.
        // A bit agressive for exactly 3x the max cap, but very reasonable for 1 byte under 4x.
        split_times = super::select_split_times(300, 100, 0, 100, vec![]);
        assert!(split_times == vec![14, 28, 42, 56, 70, 84]);
        split_times = super::select_split_times(399, 100, 0, 100, vec![]);
        assert!(split_times == vec![14, 28, 42, 56, 70, 84]);

        // splitting 400 bytes based on a max of 100, with a time range 0-100, gives 7 splits, 8 pieces.
        split_times = super::select_split_times(400, 100, 0, 100, vec![]);
        assert!(split_times == vec![11, 22, 33, 44, 55, 66, 77, 88]);

        // Now some pathelogical cases:

        // splitting 400 bytes based on a max of 100, with a time range 0-3, gives 2 splits, into 3 pieces.
        // Some (maybe all) of these will still exceed the max, but this is the most splitting possible for
        // the time range.
        split_times = super::select_split_times(400, 100, 0, 3, vec![]);
        assert!(split_times == vec![1, 2]);
    }

    #[test]
    fn test_split_empty() {
        let files = vec![];
        let (files_to_split, files_not_to_split) = super::identify_start_level_files_to_split(
            files,
            CompactionLevel::Initial,
            create_fake_partition_id(),
        );
        assert!(files_to_split.is_empty());
        assert!(files_not_to_split.is_empty());
    }

    #[test]
    #[should_panic]
    fn test_split_files_wrong_target_level() {
        // all L1 files
        let files = create_l1_files(1);

        // Target is L0 while all files are in L1 --> panic
        let (_files_to_split, _files_not_to_split) = super::identify_start_level_files_to_split(
            files,
            CompactionLevel::Initial,
            create_fake_partition_id(),
        );
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
        let (_files_to_split, _files_not_to_split) = super::identify_start_level_files_to_split(
            files,
            CompactionLevel::FileNonOverlapped,
            create_fake_partition_id(),
        );
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

        let (files_to_split, files_not_to_split) = super::identify_start_level_files_to_split(
            files,
            CompactionLevel::FileNonOverlapped,
            create_fake_partition_id(),
        );
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

        let (files_to_split, files_not_to_split) = super::identify_start_level_files_to_split(
            files,
            CompactionLevel::FileNonOverlapped,
            create_fake_partition_id(),
        );

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

    // test_linear_dist_ranges uses insta to visualize the layout of files and the resulting time ranges that should cover approximately
    // equal density of data within each range.  Judging correctness here is subjective, but the goal is to improve the decision quality in
    // vertical splitting.
    // Note:
    //     linear_dist_ranges is not necessarily trying to identify all split times necessary to get groups of files at max_compact_size.
    //     Its preferable to let select_split_times pick most of the split itmes, because it considers the time boundaries of L1 files,
    //     so the split times it selects are more efficient.  The purpose of linear_dist_ranges is to cope with data that is very unevenly
    //     distributed across time, to prevent select_split_times's ignorance of data distribution from allowing it to make bad decisions.
    //     So if the data is close to evenly distributed, one time range - even if its huge - is what we want to see here.  When the data
    //     is unevenly distributed, separate time ranges should capture each area that has substantially different data density (except that
    //     a range can grow up to max_compact_size no matter how uneven its internal distribution).
    //
    // There is a comment before each insta result showing the regions.  The comment describes what we want to see.  If future code changes
    // cause the result to change, we likely don't care, so long as the general objective of the comment is still met.  This is a game of
    // approximation, not precision.
    #[tokio::test]
    async fn test_linear_dist_ranges() {
        test_helpers::maybe_start_logging();

        let sz_100_mb = 100 * 1024 * 1024;
        let sz_300_mb = 3 * sz_100_mb;
        let fake_partition_id = create_fake_partition_id();

        let mut chain: Vec<ParquetFile> = vec![];

        let f1 = ParquetFileBuilder::new(1)
            .with_time_range(100, 1000000)
            .with_compaction_level(CompactionLevel::Initial)
            .with_file_size_bytes(sz_100_mb)
            .build();

        chain.push(f1);

        insta::assert_yaml_snapshot!(
            format_files("case 1 & 2 files", &chain),
            @r###"
    ---
    - case 1 & 2 files
    - "L0, all files 100mb                                                                                                "
    - "L0.1[100,1000000] 0ns    |------------------------------------------L0.1------------------------------------------|"
    "###
        );

        // // Case 1: 1 file smaller than the max compact size
        let mut chain_cap: usize = chain.iter().map(|f| f.file_size_bytes as usize).sum();
        let linear_ranges = super::linear_dist_ranges(
            &chain,
            chain_cap,
            sz_300_mb as usize,
            fake_partition_id.clone(),
        );

        // expect 1 range for the entire chain
        assert_eq!(linear_ranges.len(), 1);
        assert_eq!(linear_ranges[0].min, 100);
        assert_eq!(linear_ranges[0].max, 1000000);

        // 1 input file always results in 1 output range
        insta::assert_yaml_snapshot!(
            format_ranges("case 1 linear data distribution ranges", &linear_ranges),
            @r###"
    ---
    - case 1 linear data distribution ranges
    - "[100,1000000]            |-----------------------------------------range------------------------------------------|"
    "###
        );

        // Case 2: 1 file, even when its 10x the max compact size is still a single region because its consistent density.
        chain_cap = chain.iter().map(|f| f.file_size_bytes as usize).sum();
        let linear_ranges = super::linear_dist_ranges(
            &chain,
            chain_cap,
            sz_100_mb as usize / 10,
            fake_partition_id.clone(),
        );

        assert_eq!(linear_ranges.len(), 1);

        // 1 input file - even one much larger than max_compact_size -results in a 1 output rnage.
        insta::assert_yaml_snapshot!(
            format_ranges("case 2 linear data distribution ranges", &linear_ranges),
            @r###"
    ---
    - case 2 linear data distribution ranges
    - "[100,999999]             |-----------------------------------------range------------------------------------------|"
    "###
        );

        // Case 3: many files, stataggered overlap, so the data distribution changes significianly but consistently across the chain.
        let mut chain: Vec<ParquetFile> = vec![];
        for i in 0..50 {
            let f1 = ParquetFileBuilder::new(1)
                .with_time_range(100, 100 + i * 10000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(sz_100_mb)
                .build();
            chain.push(f1);
        }
        insta::assert_yaml_snapshot!(
            format_files("case 3 files", &chain),
            @r###"
    ---
    - case 3 files
    - "L0, all files 100mb                                                                                                "
    - "L0.1[100,100] 0ns        |L0.1|                                                                                    "
    - "L0.1[100,10100] 0ns      |L0.1|                                                                                    "
    - "L0.1[100,20100] 0ns      |L0.1|                                                                                    "
    - "L0.1[100,30100] 0ns      |L0.1|                                                                                    "
    - "L0.1[100,40100] 0ns      |L0.1-|                                                                                   "
    - "L0.1[100,50100] 0ns      |-L0.1--|                                                                                 "
    - "L0.1[100,60100] 0ns      |--L0.1---|                                                                               "
    - "L0.1[100,70100] 0ns      |---L0.1---|                                                                              "
    - "L0.1[100,80100] 0ns      |----L0.1----|                                                                            "
    - "L0.1[100,90100] 0ns      |-----L0.1-----|                                                                          "
    - "L0.1[100,100100] 0ns     |------L0.1------|                                                                        "
    - "L0.1[100,110100] 0ns     |-------L0.1-------|                                                                      "
    - "L0.1[100,120100] 0ns     |--------L0.1--------|                                                                    "
    - "L0.1[100,130100] 0ns     |--------L0.1---------|                                                                   "
    - "L0.1[100,140100] 0ns     |---------L0.1----------|                                                                 "
    - "L0.1[100,150100] 0ns     |----------L0.1-----------|                                                               "
    - "L0.1[100,160100] 0ns     |-----------L0.1------------|                                                             "
    - "L0.1[100,170100] 0ns     |------------L0.1-------------|                                                           "
    - "L0.1[100,180100] 0ns     |-------------L0.1--------------|                                                         "
    - "L0.1[100,190100] 0ns     |--------------L0.1--------------|                                                        "
    - "L0.1[100,200100] 0ns     |---------------L0.1---------------|                                                      "
    - "L0.1[100,210100] 0ns     |----------------L0.1----------------|                                                    "
    - "L0.1[100,220100] 0ns     |-----------------L0.1-----------------|                                                  "
    - "L0.1[100,230100] 0ns     |------------------L0.1------------------|                                                "
    - "L0.1[100,240100] 0ns     |-------------------L0.1-------------------|                                              "
    - "L0.1[100,250100] 0ns     |-------------------L0.1--------------------|                                             "
    - "L0.1[100,260100] 0ns     |--------------------L0.1---------------------|                                           "
    - "L0.1[100,270100] 0ns     |---------------------L0.1----------------------|                                         "
    - "L0.1[100,280100] 0ns     |----------------------L0.1-----------------------|                                       "
    - "L0.1[100,290100] 0ns     |-----------------------L0.1------------------------|                                     "
    - "L0.1[100,300100] 0ns     |------------------------L0.1-------------------------|                                   "
    - "L0.1[100,310100] 0ns     |-------------------------L0.1-------------------------|                                  "
    - "L0.1[100,320100] 0ns     |--------------------------L0.1--------------------------|                                "
    - "L0.1[100,330100] 0ns     |---------------------------L0.1---------------------------|                              "
    - "L0.1[100,340100] 0ns     |----------------------------L0.1----------------------------|                            "
    - "L0.1[100,350100] 0ns     |-----------------------------L0.1-----------------------------|                          "
    - "L0.1[100,360100] 0ns     |------------------------------L0.1------------------------------|                        "
    - "L0.1[100,370100] 0ns     |------------------------------L0.1-------------------------------|                       "
    - "L0.1[100,380100] 0ns     |-------------------------------L0.1--------------------------------|                     "
    - "L0.1[100,390100] 0ns     |--------------------------------L0.1---------------------------------|                   "
    - "L0.1[100,400100] 0ns     |---------------------------------L0.1----------------------------------|                 "
    - "L0.1[100,410100] 0ns     |----------------------------------L0.1-----------------------------------|               "
    - "L0.1[100,420100] 0ns     |-----------------------------------L0.1------------------------------------|             "
    - "L0.1[100,430100] 0ns     |------------------------------------L0.1------------------------------------|            "
    - "L0.1[100,440100] 0ns     |-------------------------------------L0.1-------------------------------------|          "
    - "L0.1[100,450100] 0ns     |--------------------------------------L0.1--------------------------------------|        "
    - "L0.1[100,460100] 0ns     |---------------------------------------L0.1---------------------------------------|      "
    - "L0.1[100,470100] 0ns     |----------------------------------------L0.1----------------------------------------|    "
    - "L0.1[100,480100] 0ns     |-----------------------------------------L0.1-----------------------------------------|  "
    - "L0.1[100,490100] 0ns     |------------------------------------------L0.1------------------------------------------|"
    "###
        );

        chain_cap = chain.iter().map(|f| f.file_size_bytes as usize).sum();
        let linear_ranges = super::linear_dist_ranges(
            &chain,
            chain_cap,
            sz_100_mb as usize,
            fake_partition_id.clone(),
        );

        // The 100MB in a single ns is identified and isolated in its own region.  Other than that, the data density gradually
        // diminishes across the time range.  But note that the rate of change accelerates across the time range, so regions get
        // smaller across the range (to prevent excessive data density variation within any single range)
        insta::assert_yaml_snapshot!(
            format_ranges("case 3 linear data distribution ranges", &linear_ranges),
            @r###"
        ---
        - case 3 linear data distribution ranges
        - "[100,137] 102mb          |range|                                                                                   "
        - "[138,320097] 4.44gb      |--------------------------range--------------------------|                               "
        - "[320098,460089] 341mb                                                               |---------range---------|      "
        - "[460090,486499] 12mb                                                                                          |range|"
        "###
        );

        // Case 4: A very big time range small file.  Then one big file overlapping the beginning (e.g. highly lopsided leading edge)
        let mut chain: Vec<ParquetFile> = vec![];
        let f1 = ParquetFileBuilder::new(1)
            .with_time_range(10, 10000000)
            .with_compaction_level(CompactionLevel::Initial)
            .with_file_size_bytes(sz_100_mb)
            .build();
        chain.push(f1);
        let f1 = ParquetFileBuilder::new(1)
            .with_time_range(10, 1000)
            .with_compaction_level(CompactionLevel::Initial)
            .with_file_size_bytes(sz_100_mb * 10)
            .build();
        chain.push(f1);

        insta::assert_yaml_snapshot!(
            format_files("case 4 files", &chain),
            @r###"
    ---
    - case 4 files
    - "L0                                                                                                                 "
    - "L0.1[10,10000000] 0ns 100mb|------------------------------------------L0.1------------------------------------------|"
    - "L0.1[10,1000] 0ns 1000mb |L0.1|                                                                                    "
    "###
        );

        chain_cap = chain.iter().map(|f| f.file_size_bytes as usize).sum();
        let linear_ranges = super::linear_dist_ranges(
            &chain,
            chain_cap,
            sz_100_mb as usize,
            fake_partition_id.clone(),
        );

        // Note that the above files have an exteme nonlinearity in the data distribution, but its a very simple scenario.  A human
        // can recognize an ideal region splitting would be 2 regions divided immediately after the large file, which would produce
        // two regions with perfectly linear internal data distribution in each region).
        // The algoritm comes very close to this perfection.
        insta::assert_yaml_snapshot!(
            format_ranges("case 4 linear data distribution ranges", &linear_ranges),
            @r###"
        ---
        - case 4 linear data distribution ranges
        - "[10,896] 895mb           |range|                                                                                   "
        - "[897,1783] 105mb         |range|                                                                                   "
        - "[1784,9991177] 100mb     |-----------------------------------------range-----------------------------------------| "
        "###
        );

        // Case 5: Minor fluctuations are ignored
        let mut chain: Vec<ParquetFile> = vec![];
        let mut start = 0;
        // very even distribution across 10 million ns
        while start < 10000000 {
            let f = ParquetFileBuilder::new(1)
                .with_time_range(start, start + 250000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(sz_100_mb)
                .build();
            chain.push(f);
            start += 250000;
        }
        // A few little fluctuaitons, spread across the time range
        for i in 0..5 {
            let f1 = ParquetFileBuilder::new(1)
                .with_time_range(2000000 * i, 2000000 * i + 500000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(sz_100_mb / 3)
                .build();
            chain.push(f1);
        }
        insta::assert_yaml_snapshot!(
            format_files("case 5 files", &chain),
            @r###"
    ---
    - case 5 files
    - "L0                                                                                                                 "
    - "L0.1[0,250000] 0ns 100mb |L0.1|                                                                                    "
    - "L0.1[250000,500000] 0ns 100mb  |L0.1|                                                                                  "
    - "L0.1[500000,750000] 0ns 100mb    |L0.1|                                                                                "
    - "L0.1[750000,1000000] 0ns 100mb      |L0.1|                                                                              "
    - "L0.1[1000000,1250000] 0ns 100mb         |L0.1|                                                                           "
    - "L0.1[1250000,1500000] 0ns 100mb           |L0.1|                                                                         "
    - "L0.1[1500000,1750000] 0ns 100mb             |L0.1|                                                                       "
    - "L0.1[1750000,2000000] 0ns 100mb               |L0.1|                                                                     "
    - "L0.1[2000000,2250000] 0ns 100mb                  |L0.1|                                                                  "
    - "L0.1[2250000,2500000] 0ns 100mb                    |L0.1|                                                                "
    - "L0.1[2500000,2750000] 0ns 100mb                      |L0.1|                                                              "
    - "L0.1[2750000,3000000] 0ns 100mb                        |L0.1|                                                            "
    - "L0.1[3000000,3250000] 0ns 100mb                           |L0.1|                                                         "
    - "L0.1[3250000,3500000] 0ns 100mb                             |L0.1|                                                       "
    - "L0.1[3500000,3750000] 0ns 100mb                               |L0.1|                                                     "
    - "L0.1[3750000,4000000] 0ns 100mb                                 |L0.1|                                                   "
    - "L0.1[4000000,4250000] 0ns 100mb                                    |L0.1|                                                "
    - "L0.1[4250000,4500000] 0ns 100mb                                      |L0.1|                                              "
    - "L0.1[4500000,4750000] 0ns 100mb                                        |L0.1|                                            "
    - "L0.1[4750000,5000000] 0ns 100mb                                          |L0.1|                                          "
    - "L0.1[5000000,5250000] 0ns 100mb                                             |L0.1|                                       "
    - "L0.1[5250000,5500000] 0ns 100mb                                               |L0.1|                                     "
    - "L0.1[5500000,5750000] 0ns 100mb                                                 |L0.1|                                   "
    - "L0.1[5750000,6000000] 0ns 100mb                                                   |L0.1|                                 "
    - "L0.1[6000000,6250000] 0ns 100mb                                                      |L0.1|                              "
    - "L0.1[6250000,6500000] 0ns 100mb                                                        |L0.1|                            "
    - "L0.1[6500000,6750000] 0ns 100mb                                                          |L0.1|                          "
    - "L0.1[6750000,7000000] 0ns 100mb                                                            |L0.1|                        "
    - "L0.1[7000000,7250000] 0ns 100mb                                                               |L0.1|                     "
    - "L0.1[7250000,7500000] 0ns 100mb                                                                 |L0.1|                   "
    - "L0.1[7500000,7750000] 0ns 100mb                                                                   |L0.1|                 "
    - "L0.1[7750000,8000000] 0ns 100mb                                                                     |L0.1|               "
    - "L0.1[8000000,8250000] 0ns 100mb                                                                        |L0.1|            "
    - "L0.1[8250000,8500000] 0ns 100mb                                                                          |L0.1|          "
    - "L0.1[8500000,8750000] 0ns 100mb                                                                            |L0.1|        "
    - "L0.1[8750000,9000000] 0ns 100mb                                                                              |L0.1|      "
    - "L0.1[9000000,9250000] 0ns 100mb                                                                                 |L0.1|   "
    - "L0.1[9250000,9500000] 0ns 100mb                                                                                   |L0.1| "
    - "L0.1[9500000,9750000] 0ns 100mb                                                                                     |L0.1|"
    - "L0.1[9750000,10000000] 0ns 100mb                                                                                       |L0.1|"
    - "L0.1[0,500000] 0ns 33mb  |L0.1|                                                                                    "
    - "L0.1[2000000,2500000] 0ns 33mb                  |L0.1|                                                                  "
    - "L0.1[4000000,4500000] 0ns 33mb                                    |L0.1|                                                "
    - "L0.1[6000000,6500000] 0ns 33mb                                                      |L0.1|                              "
    - "L0.1[8000000,8500000] 0ns 33mb                                                                        |L0.1|            "
    "###
        );

        chain_cap = chain.iter().map(|f| f.file_size_bytes as usize).sum();
        let linear_ranges = super::linear_dist_ranges(
            &chain,
            chain_cap,
            sz_100_mb as usize,
            fake_partition_id.clone(),
        );

        // The fluctuations aren't very dense relative to the consistent data (33MB on 500k ns), so they get ignored.
        // we get one range for everything, because its linear enough of a distribution.
        insta::assert_yaml_snapshot!(
            format_ranges("case 5 linear data distribution ranges", &linear_ranges),
            @r###"
    ---
    - case 5 linear data distribution ranges
    - "[0,9999922]              |-----------------------------------------range------------------------------------------|"
    "###
        );

        // Case 6: similar to the prior case, but these fluctuations are denser
        let mut chain: Vec<ParquetFile> = vec![];
        let mut start = 0;
        // very even distribution across 10 million ns
        while start < 10000000 {
            let f = ParquetFileBuilder::new(1)
                .with_time_range(start, start + 250000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(sz_100_mb)
                .build();
            chain.push(f);
            start += 250000;
        }
        // A few sizeable fluctuaitons
        for i in 0..5 {
            let f1 = ParquetFileBuilder::new(1)
                .with_time_range(2000000 * i, 2000000 * i + 1000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(sz_100_mb)
                .build();
            chain.push(f1);
        }
        insta::assert_yaml_snapshot!(
            format_files("case 6 files", &chain),
            @r###"
    ---
    - case 6 files
    - "L0, all files 100mb                                                                                                "
    - "L0.1[0,250000] 0ns       |L0.1|                                                                                    "
    - "L0.1[250000,500000] 0ns    |L0.1|                                                                                  "
    - "L0.1[500000,750000] 0ns      |L0.1|                                                                                "
    - "L0.1[750000,1000000] 0ns       |L0.1|                                                                              "
    - "L0.1[1000000,1250000] 0ns         |L0.1|                                                                           "
    - "L0.1[1250000,1500000] 0ns           |L0.1|                                                                         "
    - "L0.1[1500000,1750000] 0ns             |L0.1|                                                                       "
    - "L0.1[1750000,2000000] 0ns               |L0.1|                                                                     "
    - "L0.1[2000000,2250000] 0ns                  |L0.1|                                                                  "
    - "L0.1[2250000,2500000] 0ns                    |L0.1|                                                                "
    - "L0.1[2500000,2750000] 0ns                      |L0.1|                                                              "
    - "L0.1[2750000,3000000] 0ns                        |L0.1|                                                            "
    - "L0.1[3000000,3250000] 0ns                           |L0.1|                                                         "
    - "L0.1[3250000,3500000] 0ns                             |L0.1|                                                       "
    - "L0.1[3500000,3750000] 0ns                               |L0.1|                                                     "
    - "L0.1[3750000,4000000] 0ns                                 |L0.1|                                                   "
    - "L0.1[4000000,4250000] 0ns                                    |L0.1|                                                "
    - "L0.1[4250000,4500000] 0ns                                      |L0.1|                                              "
    - "L0.1[4500000,4750000] 0ns                                        |L0.1|                                            "
    - "L0.1[4750000,5000000] 0ns                                          |L0.1|                                          "
    - "L0.1[5000000,5250000] 0ns                                             |L0.1|                                       "
    - "L0.1[5250000,5500000] 0ns                                               |L0.1|                                     "
    - "L0.1[5500000,5750000] 0ns                                                 |L0.1|                                   "
    - "L0.1[5750000,6000000] 0ns                                                   |L0.1|                                 "
    - "L0.1[6000000,6250000] 0ns                                                      |L0.1|                              "
    - "L0.1[6250000,6500000] 0ns                                                        |L0.1|                            "
    - "L0.1[6500000,6750000] 0ns                                                          |L0.1|                          "
    - "L0.1[6750000,7000000] 0ns                                                            |L0.1|                        "
    - "L0.1[7000000,7250000] 0ns                                                               |L0.1|                     "
    - "L0.1[7250000,7500000] 0ns                                                                 |L0.1|                   "
    - "L0.1[7500000,7750000] 0ns                                                                   |L0.1|                 "
    - "L0.1[7750000,8000000] 0ns                                                                     |L0.1|               "
    - "L0.1[8000000,8250000] 0ns                                                                        |L0.1|            "
    - "L0.1[8250000,8500000] 0ns                                                                          |L0.1|          "
    - "L0.1[8500000,8750000] 0ns                                                                            |L0.1|        "
    - "L0.1[8750000,9000000] 0ns                                                                              |L0.1|      "
    - "L0.1[9000000,9250000] 0ns                                                                                 |L0.1|   "
    - "L0.1[9250000,9500000] 0ns                                                                                   |L0.1| "
    - "L0.1[9500000,9750000] 0ns                                                                                     |L0.1|"
    - "L0.1[9750000,10000000] 0ns                                                                                       |L0.1|"
    - "L0.1[0,1000] 0ns         |L0.1|                                                                                    "
    - "L0.1[2000000,2001000] 0ns                  |L0.1|                                                                  "
    - "L0.1[4000000,4001000] 0ns                                    |L0.1|                                                "
    - "L0.1[6000000,6001000] 0ns                                                      |L0.1|                              "
    - "L0.1[8000000,8001000] 0ns                                                                        |L0.1|            "
    "###
        );

        chain_cap = chain.iter().map(|f| f.file_size_bytes as usize).sum();
        let linear_ranges =
            super::linear_dist_ranges(&chain, chain_cap, sz_100_mb as usize, fake_partition_id);

        // These fluctuations are quite dense (100mb on 1000ns), so that triggeres the non-linear data distribution code to
        // break it up into regions.  The regions are around 100MB, capturing each of the fluctations.  This roughly carves
        // out the dense areas of data, so vertical splitting can make better decisions within each range.
        insta::assert_yaml_snapshot!(
            format_ranges("case 6 linear data distribution ranges", &linear_ranges),
            @r###"
        ---
        - case 6 linear data distribution ranges
        - "[0,867] 87mb             |range|                                                                                   "
        - "[868,1999871] 813mb      |-----range-----|                                                                         "
        - "[1999872,2001607] 101mb                    |range|                                                                 "
        - "[2001608,3999743] 799mb                    |-----range-----|                                                       "
        - "[3999744,4001479] 101mb                                      |range|                                               "
        - "[4001480,5999615] 799mb                                      |-----range-----|                                     "
        - "[5999616,6001351] 101mb                                                        |range|                             "
        - "[6001352,7999487] 799mb                                                        |-----range-----|                   "
        - "[7999488,8001223] 101mb                                                                          |range|           "
        - "[8001224,9999359] 800mb                                                                          |-----range-----| "
        "###
        );
    }
}
