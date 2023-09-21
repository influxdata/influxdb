use std::{
    cmp::max,
    fmt::{Debug, Display},
    sync::{Arc, Mutex},
};

use crate::components::{
    split_or_compact::start_level_files_to_split::{
        linear_dist_ranges, merge_l1_spanned_chains, merge_small_l0_chains, select_split_times,
        split_into_chains,
    },
    Components,
};
use async_trait::async_trait;
use data_types::{CompactionLevel, ParquetFile, Timestamp, TransitionPartitionId};
use itertools::Itertools;
use observability_deps::tracing::{debug, info};

use crate::{
    error::DynError, round_info::CompactRange, round_info::CompactType, PartitionInfo, RoundInfo,
};

/// Calculates information about what this compaction round does.
/// When we get deeper into the compaction decision making, there
/// may not be as much context information available.  It may not
/// be possible to reach the same conclusions about the intention
/// for this compaction round.  So RoundInfo must contain enough
/// information carry that intention through the compactions.
#[async_trait]
pub trait RoundInfoSource: Debug + Display + Send + Sync {
    async fn calculate(
        &self,
        components: Arc<Components>,
        last_round_info: Option<Arc<RoundInfo>>,
        partition_info: &PartitionInfo,
        files: Vec<ParquetFile>,
    ) -> Result<(Arc<RoundInfo>, bool), DynError>;
}

#[derive(Debug)]
pub struct LoggingRoundInfoWrapper {
    inner: Arc<dyn RoundInfoSource>,
}

impl LoggingRoundInfoWrapper {
    pub fn new(inner: Arc<dyn RoundInfoSource>) -> Self {
        Self { inner }
    }
}

impl Display for LoggingRoundInfoWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LoggingRoundInfoWrapper({})", self.inner)
    }
}

#[async_trait]
impl RoundInfoSource for LoggingRoundInfoWrapper {
    async fn calculate(
        &self,
        components: Arc<Components>,
        last_round_info: Option<Arc<RoundInfo>>,
        partition_info: &PartitionInfo,
        files: Vec<ParquetFile>,
    ) -> Result<(Arc<RoundInfo>, bool), DynError> {
        let res = self
            .inner
            .calculate(components, last_round_info, partition_info, files)
            .await;
        if let Ok((round_info, done)) = &res {
            debug!(round_info_source=%self.inner, %round_info, %done, "running round");
        }
        res
    }
}

/// Computes the type of round based on the levels of the input files
#[derive(Debug)]
pub struct LevelBasedRoundInfo {
    pub max_num_files_per_plan: usize,
    pub max_total_file_size_per_plan: usize,
}

impl Display for LevelBasedRoundInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LevelBasedRoundInfo {}", self.max_num_files_per_plan)
    }
}
impl LevelBasedRoundInfo {
    pub fn new(max_num_files_per_plan: usize, max_total_file_size_per_plan: usize) -> Self {
        Self {
            max_num_files_per_plan,
            max_total_file_size_per_plan,
        }
    }

    /// Returns true if the scenario looks like ManySmallFiles, but we can't group them well into branches.
    /// TODO: use this or remove it.  For now, keep it in case we need the temporary workaround again.
    /// This can be used to identify criteria to trigger a SimulatedLeadingEdge as a temporary workaround
    /// for a situation that isn't well handled, when the desire is to postpone optimal handling to a later PR.
    #[allow(dead_code)]
    pub fn many_ungroupable_files(
        &self,
        files: &[ParquetFile],
        start_level: CompactionLevel,
        max_total_file_size_to_group: usize,
    ) -> bool {
        if self.too_many_small_files_to_compact(files, CompactionLevel::Initial) {
            let start_level_files = files
                .iter()
                .filter(|f| f.compaction_level == start_level)
                .collect::<Vec<_>>();
            let start_count = start_level_files.len();
            let mut chains = split_into_chains(start_level_files.into_iter().cloned().collect());
            chains = merge_small_l0_chains(chains, max_total_file_size_to_group);

            if chains.len() > 1 && chains.len() > start_count / 3 {
                return true;
            }
        }
        false
    }

    /// Returns true if number of files of the given start_level and
    /// their overlapped files in next level is over limit, and if those
    /// files are sufficiently small.
    ///
    /// over the limit means that the maximum number of files that a subsequent compaction
    /// branch may choose to compact in a single plan would exceed `max_num_files_per_plan`
    pub fn too_many_small_files_to_compact(
        &self,
        files: &[ParquetFile],
        start_level: CompactionLevel,
    ) -> bool {
        let start_level_files = files
            .iter()
            .filter(|f| f.compaction_level == start_level)
            .collect::<Vec<_>>();
        let num_start_level = start_level_files.len();
        let size_start_level: usize = start_level_files
            .iter()
            .map(|f| f.file_size_bytes as usize)
            .sum();
        let start_max_l0_created_at = start_level_files
            .iter()
            .map(|f| f.max_l0_created_at)
            .unique()
            .count();

        let next_level_files = files
            .iter()
            .filter(|f| f.compaction_level == start_level.next())
            .collect::<Vec<_>>();

        // The compactor may compact all the target level and next level together in one
        // branch in the worst case, thus if that would result in too many files to compact in a single
        // plan, run a pre-phase to reduce the number of files first
        let num_overlapped_files = get_num_overlapped_files(start_level_files, next_level_files);
        if num_start_level > 1
            && num_start_level + num_overlapped_files > self.max_num_files_per_plan
        {
            // This scaenario meets the simple criteria of start level files + their overlaps are lots of files.
            // But ManySmallFiles implies we must compact only within the start level to reduce the quantity of
            // start level files. There are several reasons why that might be unhelpful.

            // Reason 1: if all the start level files have the same max_l0_created_at, then they were split from
            // the same file.  If we previously decided to split them, we should not undo that now.
            if start_max_l0_created_at == 1 {
                return false;
            }

            // Reason 2: Maybe its many LARGE files making reduction of file count in the start level impossible.
            if size_start_level / num_start_level
                > self.max_total_file_size_per_plan / self.max_num_files_per_plan
            {
                // Average start level file size is more than the average implied by max bytes & files per plan.
                // Even though there are "many files", this is not "many small files".
                // There isn't much (perhaps not any) file reduction to be done, attempting it can get us stuck
                // in a loop.
                return false;
            }

            // Reason 3: Maybe there are so many start level files because we did a bunch of splits.
            // Note that we'll do splits to ensure each start level file overlaps at most one target level file.
            // If the prior round did that, and now we declare this ManySmallFiles, which forces compactions
            // within the start level, we'll undo the splits performed in the prior round, which can get us
            // stuck in a loop.
            let chains = split_into_chains(files.to_vec());
            let mut max_target_level_files: usize = 0;
            let mut max_chain_len: usize = 0;
            for chain in chains {
                let target_file_cnt = chain
                    .iter()
                    .filter(|f| f.compaction_level == start_level.next())
                    .count();
                max_target_level_files = max(max_target_level_files, target_file_cnt);

                let chain_len = chain.len();
                max_chain_len = max(max_chain_len, chain_len);
            }
            if max_target_level_files <= 1 && max_chain_len <= self.max_num_files_per_plan {
                // All of our start level files overlap with at most one target level file.  If the prior round did
                // splits to cause this, declaring this a ManySmallFiles case can lead to an endless loop.
                // If we got lucky and this happened without splits, declaring this ManySmallFiles will waste
                // our good fortune.
                return false;
            }
            return true;
        }

        false
    }

    /// consider_vertical_splitting determines if vertical splitting is necessary, and if so, a vec of split times is
    /// returned.
    pub fn consider_vertical_splitting(
        &self,
        partition_id: TransitionPartitionId,
        files: Vec<ParquetFile>,
        max_compact_size: usize,
    ) -> Vec<i64> {
        let file_cnt = files.len();

        let (start_level_files, target_level_files): (Vec<ParquetFile>, Vec<ParquetFile>) = files
            .into_iter()
            .filter(|f| f.compaction_level != CompactionLevel::Final)
            .partition(|f| f.compaction_level == CompactionLevel::Initial);

        let len = start_level_files.len();
        let mut split_times = Vec::with_capacity(len);

        let cap: usize = start_level_files
            .iter()
            .map(|f| f.file_size_bytes as usize)
            .sum();

        // TODO: remove this:
        if start_level_files.len() > 300 && cap / file_cnt < max_compact_size / 10 {
            info!("skipping vertical splitting on partition_id {} for now, due to excessive file count.  file count: {}, cap: {} MB",
                partition_id, start_level_files.len(), cap/1024/1024);
            return vec![];
        }

        // A single file over max size can just get upgraded to L1, then L2, unless it overlaps other L0s.
        // So multi file filess over the max compact size may need split
        if start_level_files.len() > 1 && cap > max_compact_size {
            // files in this range are too big to compact in one job, so files will be split it into smaller, more manageable ranges.
            // We can't know the data distribution within each file without reading the file (too expensive), but we can
            // still learn a lot about the data distribution accross the set of files by assuming even distribtuion within each
            // file and considering the distribution of files within the files's time range.
            let linear_ranges = linear_dist_ranges(
                &start_level_files,
                cap,
                max_compact_size,
                partition_id.clone(),
            );

            let mut first_range = true;
            for range in linear_ranges {
                // split at every time range of linear distribution.
                if !first_range {
                    split_times.push(range.min - 1);
                }
                first_range = false;

                // how many start level files are in this range?
                let overlaps = start_level_files
                    .iter()
                    .filter(|f| {
                        f.overlaps_time_range(Timestamp::new(range.min), Timestamp::new(range.max))
                    })
                    .count();

                if overlaps > 1 && range.cap > max_compact_size {
                    // Since we'll be splitting the start level files within this range, it would be nice to align the split times to
                    // the min/max times of target level files.  select_split_times will use the min/max time of target level files
                    // as hints, and see what lines up to where the range needs split.
                    let mut split_hints: Vec<i64> =
                        Vec::with_capacity(range.cap * 2 / max_compact_size + 1);

                    // split time is the last time included in the 'left' side of the split.  Our goal with these hints is to avoid
                    // overlaps with L1 files, we'd like the 'left file' to end before this L1 file starts (split=min-1), or it can
                    // include up to the last ns of the L1 file (split=max).
                    for f in &target_level_files {
                        if f.min_time.get() - 1 > range.min && f.min_time.get() < range.max {
                            split_hints.push(f.min_time.get() - 1);
                        }
                        if f.max_time.get() > range.min && f.max_time.get() < range.max {
                            split_hints.push(f.max_time.get());
                        }
                    }

                    // We may have started splitting files, and now there's a new L0 added that spans our previous splitting.
                    // We'll detect multiple L0 files ending at the same time, and add that to the split hints.
                    let end_times = start_level_files
                        .iter()
                        .map(|f| f.max_time.get())
                        .sorted()
                        .dedup_with_count();
                    for (count, time) in end_times {
                        if count > 1 {
                            // wether we previously split here or not, with at least 2 L0s ending here, its a good place to split.
                            split_hints.push(time);
                        }
                    }

                    let splits = select_split_times(
                        range.cap,
                        max_compact_size,
                        range.min,
                        range.max,
                        split_hints.clone(),
                    );
                    split_times.extend(splits);
                }
            }
        }

        split_times.sort();
        split_times.dedup();
        split_times
    }

    // derive_draft_ranges takes a last round info option and a vec of files - one of them must be populated.
    // From this, we'll get a draft of CompactRanges for the current round of compaction.  Its a draft because
    // it partially set up, having only the files, min, max, and cap set.  The op, branches, and files_for_later
    // will be determined shortly.
    // We split up into several ranges to keep the L0->L1 compaction simple (the overlaps allowed in L0 make it messy).
    // But we don't want to create artificial divisions in L2, so L2's get set aside until we've consolidated to
    // a single CompactRange.
    fn derive_draft_ranges(
        &self,
        partition_info: &PartitionInfo,
        last_round_info: Option<Arc<RoundInfo>>,
        files: Vec<ParquetFile>,
    ) -> (Vec<CompactRange>, Option<Vec<ParquetFile>>) {
        // We require exactly 1 source of information: either 'files' because this is the first round, or 'last_round_info' from the prior round.
        if let Some(last_round_info) = last_round_info {
            assert!(
                files.is_empty(),
                "last_round_info and files must not both be populated"
            );
            self.evaluate_prior_ranges(partition_info, last_round_info)
        } else {
            assert!(
                !files.is_empty(),
                "last_round_info and files must not both be empty"
            );
            // This is the first round, so no prior round info.
            // We'll take a look at 'files' and see what we can do.
            self.split_files_into_ranges(files)
        }
    }

    // evaluate_prior_ranges is a helper function for derive_draft_ranges, used when there is prior round info.
    // It takes the prior round's ranges, and splits them if they did vertical splitting, or combines them if
    // they finished compacting their L0s.
    fn evaluate_prior_ranges(
        &self,
        partition_info: &PartitionInfo,
        last_round_info: Arc<RoundInfo>,
    ) -> (Vec<CompactRange>, Option<Vec<ParquetFile>>) {
        // We'll start with the ranges from the prior round.
        let mut ranges = Vec::with_capacity(last_round_info.ranges.len());

        // As we iterate through the last_round_info's ranges, we'll try to consolidate ranges for any that don't have L0s.
        let mut prior_range: Option<CompactRange> = None;

        for range in &last_round_info.ranges {
            // The prior round should have handled its `files_for_now`, so that should `None`.
            // What the prior round considered `files_for_later` will now become `files_for_now`.
            assert!(
                range.files_for_now.lock().unwrap().is_none(),
                "files_for_now should be empty for range {}->{} on partition {}",
                range.min,
                range.max,
                partition_info.partition_id()
            );
            assert!(
                range.branches.lock().unwrap().is_none(),
                "branches should be empty for range {}->{} on partition {}",
                range.min,
                range.max,
                partition_info.partition_id()
            );

            let files_for_now = range.files_for_later.lock().unwrap().take();
            assert!(
                files_for_now.is_some(),
                "files_for_later should not be None for range {}->{} on partition {}",
                range.min,
                range.max,
                partition_info.partition_id()
            );
            let mut files_for_now = files_for_now.unwrap();
            assert!(
                !files_for_now.is_empty(),
                "files_for_later should not be empty for range {}->{} on partition {}",
                range.min,
                range.max,
                partition_info.partition_id()
            );

            if let Some(split_times) = range.op.split_times() {
                // In the prior round, this range did vertical splitting.  Those split times now divide this range into several ranges.

                if prior_range.is_some() {
                    ranges.push(prior_range.unwrap());
                    prior_range = None;
                }

                let mut split_ranges = Vec::with_capacity(split_times.len());
                let mut max = range.max;

                for split_time in split_times.into_iter().rev() {
                    // By iterating in reverse, everything above the split time is in this split
                    let this_split_files_for_now: Vec<ParquetFile>;
                    (this_split_files_for_now, files_for_now) = files_for_now
                        .into_iter()
                        .partition(|f| f.max_time.get() > split_time);
                    let cap = this_split_files_for_now
                        .iter()
                        .map(|f| f.file_size_bytes as usize)
                        .sum::<usize>();

                    let this_split_files_for_now = if this_split_files_for_now.is_empty() {
                        None
                    } else {
                        Some(this_split_files_for_now.clone())
                    };

                    split_ranges.insert(
                        0,
                        CompactRange {
                            op: CompactType::Deferred {},
                            min: split_time + 1,
                            max,
                            cap,
                            has_l0s: true,
                            files_for_now: Mutex::new(this_split_files_for_now),
                            branches: Mutex::new(None),
                            files_for_later: Mutex::new(None),
                        },
                    );

                    // split_time is the highest time in the 'left' file, so that will be max time for the next range.
                    max = split_time;
                }

                if !files_for_now.is_empty() {
                    let cap = files_for_now
                        .iter()
                        .map(|f| f.file_size_bytes as usize)
                        .sum::<usize>();
                    let files_for_now = Some(files_for_now.clone());

                    split_ranges.insert(
                        0,
                        CompactRange {
                            op: CompactType::Deferred {},
                            min: range.min,
                            max,
                            cap,
                            has_l0s: true,
                            files_for_now: Mutex::new(files_for_now),
                            branches: Mutex::new(None),
                            files_for_later: Mutex::new(None),
                        },
                    );
                }

                ranges.append(&mut split_ranges);
            } else {
                // Carry forward the prior range
                let has_l0s = files_for_now
                    .iter()
                    .any(|f| f.compaction_level == CompactionLevel::Initial);

                if prior_range.is_some() && (!prior_range.as_mut().unwrap().has_l0s || !has_l0s) {
                    // This and the prior range don't both have L0s; we can consolidate.
                    let prior = prior_range.as_mut().unwrap();
                    prior.max = range.max;
                    prior.cap += range.cap;
                    prior.has_l0s = prior.has_l0s || has_l0s;
                    prior.add_files_for_now(files_for_now);
                } else {
                    if let Some(prior_range) = prior_range {
                        // we'll not be consolidating with with the prior range, so push it
                        ranges.push(prior_range);
                    }

                    let files_for_now = if files_for_now.is_empty() {
                        None
                    } else {
                        Some(files_for_now.clone())
                    };
                    let this_range = CompactRange {
                        op: range.op.clone(),
                        min: range.min,
                        max: range.max,
                        cap: range.cap,
                        has_l0s,
                        files_for_now: Mutex::new(files_for_now),
                        branches: Mutex::new(None),
                        files_for_later: Mutex::new(None),
                    };
                    prior_range = Some(this_range);
                };
            }
        }
        if let Some(prior_range) = prior_range {
            ranges.push(prior_range);
        }

        // If we still have several ranges, L2s (if any) need to stay in round_info.files_for_later.  If we have 1 range
        // without L0s, the L2s can go in that range.
        let mut deferred_l2s = last_round_info.take_l2_files_for_later();
        if ranges.len() == 1 && !ranges[0].has_l0s && deferred_l2s.is_some() {
            ranges[0].add_files_for_now(deferred_l2s.unwrap());
            deferred_l2s = None;
        }
        (ranges, deferred_l2s)
    }

    // split_files_into_ranges is a helper function for derive_draft_ranges, used when there is no prior round info.
    // Its given the files found in the catalog, and puts them into range(s).
    fn split_files_into_ranges(
        &self,
        files: Vec<ParquetFile>,
    ) -> (Vec<CompactRange>, Option<Vec<ParquetFile>>) {
        let (l0_files, other_files): (Vec<ParquetFile>, Vec<ParquetFile>) = files
            .into_iter()
            .partition(|f| f.compaction_level == CompactionLevel::Initial);
        if !l0_files.is_empty() {
            // We'll get all the L0 files compacted to L1 before dealing with L2 files - so separate them.
            let (l2_files_for_later, mut l1_files): (Vec<ParquetFile>, Vec<ParquetFile>) =
                other_files
                    .into_iter()
                    .partition(|f| f.compaction_level == CompactionLevel::Final);

            // Break up the start level files into chains of files that overlap each other.
            // Then we'll determine if vertical splitting is needed within each chain.
            let chains = split_into_chains(l0_files);

            // This function is detecting what ranges we already have, not identifying splitting to make ranges we want.
            // So we may have to combine some chains based on L1s overlapping.
            let chains = merge_l1_spanned_chains(chains, &l1_files);

            // the goal is nice bite sized chains.  If some are very small, merge them with their neighbor(s).
            let chains = merge_small_l0_chains(chains, self.max_total_file_size_per_plan);

            let mut ranges = Vec::with_capacity(chains.len());
            let mut this_split: Vec<ParquetFile>;

            for mut chain in chains {
                let mut max = chain.iter().map(|f| f.max_time).max().unwrap().get();

                // 'chain' is the L0s that will become a region.  We also need the L1s and L2s that belong in this region.

                (this_split, l1_files) =
                    l1_files.into_iter().partition(|f| f.min_time.get() <= max);

                if !this_split.is_empty() {
                    max = max.max(this_split.iter().map(|f| f.max_time).max().unwrap().get());
                }

                this_split.append(&mut chain);
                let min = this_split.iter().map(|f| f.min_time).min().unwrap().get();
                let cap = this_split
                    .iter()
                    .map(|f| f.file_size_bytes as usize)
                    .sum::<usize>();

                ranges.push(CompactRange {
                    op: CompactType::Deferred {},
                    min,
                    max,
                    cap,
                    has_l0s: true,
                    files_for_now: Mutex::new(Some(this_split)),
                    branches: Mutex::new(None),
                    files_for_later: Mutex::new(None),
                });
            }

            this_split = l1_files;
            if !this_split.is_empty() {
                let min = this_split.iter().map(|f| f.min_time).min().unwrap().get();
                let max = this_split.iter().map(|f| f.max_time).max().unwrap().get();
                let cap = this_split
                    .iter()
                    .map(|f| f.file_size_bytes as usize)
                    .sum::<usize>();

                ranges.push(CompactRange {
                    op: CompactType::Deferred {},
                    min,
                    max,
                    cap,
                    has_l0s: true,
                    files_for_now: Mutex::new(Some(this_split)),
                    branches: Mutex::new(None),
                    files_for_later: Mutex::new(None),
                });
            }

            let l2_files_for_later = if l2_files_for_later.is_empty() {
                None
            } else {
                Some(l2_files_for_later)
            };
            (ranges, l2_files_for_later)
        } else {
            // No start level files, we can put everything in one range.
            let min = other_files.iter().map(|f| f.min_time).min().unwrap().get();
            let max = other_files.iter().map(|f| f.max_time).max().unwrap().get();
            let cap = other_files
                .iter()
                .map(|f| f.file_size_bytes as usize)
                .sum::<usize>();
            (
                vec![CompactRange {
                    op: CompactType::Deferred {},
                    min,
                    max,
                    cap,
                    has_l0s: false,
                    files_for_now: Mutex::new(Some(other_files)),
                    branches: Mutex::new(None),
                    files_for_later: Mutex::new(None),
                }],
                None,
            )
        }
    }
}

#[async_trait]
impl RoundInfoSource for LevelBasedRoundInfo {
    // The calculated RoundInfo is the most impactful decision for this round of compactions.
    // Later decisions should be just working out details to implement what RoundInfo dictates.
    async fn calculate(
        &self,
        components: Arc<Components>,
        last_round_info: Option<Arc<RoundInfo>>,
        partition_info: &PartitionInfo,
        files: Vec<ParquetFile>,
    ) -> Result<(Arc<RoundInfo>, bool), DynError> {
        // Step 1: Establish range boundaries, with files in each range.
        let (prior_ranges, mut l2_files_for_later) =
            self.derive_draft_ranges(partition_info, last_round_info, files);

        let range_cnt = prior_ranges.len();

        // Step 2: Determine the op for each range.
        let mut ranges: Vec<CompactRange> = Vec::with_capacity(range_cnt);
        for mut range in prior_ranges {
            let files_for_now = range.files_for_now.lock().unwrap().take();
            assert!(
                files_for_now.is_some(),
                "files_for_now should not be None for range {}->{} on partition {}",
                range.min,
                range.max,
                partition_info.partition_id()
            );
            let files_for_now = files_for_now.unwrap();

            // If we're down to a single range, we should check if we're done.
            if range_cnt == 1
                && !components
                    .partition_filter
                    .apply(partition_info, &files_for_now)
                    .await?
            {
                return Ok((
                    Arc::new(RoundInfo {
                        ranges,
                        l2_files_for_later: Mutex::new(l2_files_for_later),
                    }),
                    true,
                ));
            }

            range.has_l0s = files_for_now
                .iter()
                .any(|f| f.compaction_level == CompactionLevel::Initial);

            if range.has_l0s {
                let split_times = self.consider_vertical_splitting(
                    partition_info.partition_id(),
                    files_for_now.clone().to_vec(),
                    self.max_total_file_size_per_plan,
                );

                if !split_times.is_empty() {
                    range.op = CompactType::VerticalSplit { split_times };
                } else if self
                    .too_many_small_files_to_compact(&files_for_now, CompactionLevel::Initial)
                {
                    range.op = CompactType::ManySmallFiles {
                        start_level: CompactionLevel::Initial,
                        max_num_files_to_group: self.max_num_files_per_plan,
                        max_total_file_size_to_group: self.max_total_file_size_per_plan,
                    };
                } else {
                    range.op = CompactType::TargetLevel {
                        target_level: CompactionLevel::FileNonOverlapped,
                        max_total_file_size_to_group: self.max_total_file_size_per_plan,
                    };
                }
            } else if range_cnt == 1 {
                range.op = CompactType::TargetLevel {
                    target_level: CompactionLevel::Final,
                    max_total_file_size_to_group: self.max_total_file_size_per_plan,
                };
            } else {
                // The L0s of this range are compacted, but this range needs to hang out a while until its neighbors catch up.
                range.op = CompactType::Deferred {};
            };

            if range.op.is_deferred() {
                range.add_files_for_later(files_for_now);
                ranges.push(range);
            } else {
                // start_level is usually the lowest level we have files in, but occasionally we decide to
                // compact L1->L2 when L0s still exist.  If this comes back as L1, we'll ignore L0s for this
                // round and force an early L1-L2 compaction.
                let (files_for_now, mut files_later) = components.round_split.split(
                    files_for_now,
                    range.op.clone(),
                    partition_info.partition_id(),
                );

                let (branches, more_for_later) = components.divide_initial.divide(
                    files_for_now,
                    range.op.clone(),
                    partition_info.partition_id(),
                );

                files_later.extend(more_for_later);

                if !branches.is_empty() {
                    range.branches = Mutex::new(Some(branches));
                } // else, leave it None, since Some is assumed to be non-empty.

                if !files_later.is_empty() {
                    range.files_for_later = Mutex::new(Some(files_later));
                } // else, leave it None, since Some is assumed to be non-empty.
                ranges.push(range);
            }
        }

        if ranges.len() == 1 && !ranges[0].has_l0s && l2_files_for_later.is_some() {
            // Single range without L0s, its time to work on the L2s.
            ranges[0].add_files_for_now(l2_files_for_later.unwrap());
            l2_files_for_later = None;
        }

        Ok((
            Arc::new(RoundInfo {
                ranges,
                l2_files_for_later: Mutex::new(l2_files_for_later),
            }),
            false,
        ))
    }
}

fn get_num_overlapped_files(
    start_level_files: Vec<&ParquetFile>,
    next_level_files: Vec<&ParquetFile>,
) -> usize {
    // min_time and max_time of files in start_level
    let (min_time, max_time) =
        start_level_files
            .iter()
            .fold((None, None), |(min_time, max_time), f| {
                let min_time = min_time
                    .map(|v: Timestamp| v.min(f.min_time))
                    .unwrap_or(f.min_time);
                let max_time = max_time
                    .map(|v: Timestamp| v.max(f.max_time))
                    .unwrap_or(f.max_time);
                (Some(min_time), Some(max_time))
            });

    // There must be values, otherwise panic
    let min_time = min_time.unwrap();
    let max_time = max_time.unwrap();

    // number of files in next level that overlap with files in start_level
    let count_overlapped = next_level_files
        .iter()
        .filter(|f| f.min_time <= max_time && f.max_time >= min_time)
        .count();

    count_overlapped
}

#[cfg(test)]
mod tests {
    use data_types::CompactionLevel;
    use iox_tests::ParquetFileBuilder;

    use crate::components::round_info_source::LevelBasedRoundInfo;

    #[test]
    fn test_too_many_small_files_to_compact() {
        // L0 files
        let f1 = ParquetFileBuilder::new(1)
            .with_time_range(0, 100)
            .with_compaction_level(CompactionLevel::Initial)
            .with_max_l0_created_at(0)
            .build();
        let f2 = ParquetFileBuilder::new(2)
            .with_time_range(0, 100)
            .with_compaction_level(CompactionLevel::Initial)
            .with_max_l0_created_at(2)
            .build();
        // non overlapping L1 file
        let f3 = ParquetFileBuilder::new(3)
            .with_time_range(101, 200)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .build();
        // overlapping L1 file
        let f4 = ParquetFileBuilder::new(4)
            .with_time_range(50, 150)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .build();

        // max 2 files per plan
        let round_info = LevelBasedRoundInfo {
            max_num_files_per_plan: 2,
            max_total_file_size_per_plan: 1000,
        };

        // f1 and f2 are not over limit
        assert!(!round_info
            .too_many_small_files_to_compact(&[f1.clone(), f2.clone()], CompactionLevel::Initial));
        // f1, f2 and f3 are not over limit
        assert!(!round_info.too_many_small_files_to_compact(
            &[f1.clone(), f2.clone(), f3.clone()],
            CompactionLevel::Initial
        ));
        // f1, f2 and f4 are over limit
        assert!(round_info.too_many_small_files_to_compact(
            &[f1.clone(), f2.clone(), f4.clone()],
            CompactionLevel::Initial
        ));
        // f1, f2, f3 and f4 are over limit
        assert!(
            round_info.too_many_small_files_to_compact(&[f1, f2, f3, f4], CompactionLevel::Initial)
        );
    }
}
