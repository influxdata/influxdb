//! Information about the current compaction round

use std::{fmt::Display, sync::Mutex};

use data_types::{CompactionLevel, ParquetFile};

/// Information about the current compaction round (see driver.rs for
/// more details about a round)

/// FileRange describes a range of files by the min/max time and the sum of their capacities.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum CompactType {
    /// compacting to target level
    TargetLevel {
        /// compaction level of target fles
        target_level: CompactionLevel,
        /// max total size limit of files to group in each plan
        max_total_file_size_to_group: usize,
    },
    /// In many small files mode
    ManySmallFiles {
        /// start level of files in this round
        start_level: CompactionLevel,
        /// max number of files to group in each plan
        max_num_files_to_group: usize,
        /// max total size limit of files to group in each plan
        max_total_file_size_to_group: usize,
    },

    /// This scenario is not 'leading edge', but we'll process it like it is.
    /// We'll start with the L0 files we must start with (the first by max_l0_created_at),
    /// and take as many as we can (up to max files | bytes), and compact them down as if
    /// that's the only L0s there are.  This will be very much like if we got the chance
    /// to compact a while ago, when those were the only files in L0.
    /// Why would we do this:
    /// The diagnosis of various scenarios (vertical splitting, ManySmallFiles, etc)
    /// sometimes get into conflict with each other.  When we're having trouble with
    /// an efficient "big picture" approach, this is a way to get some progress.
    /// Its sorta like pushing the "easy button".
    SimulatedLeadingEdge {
        // level: always Initial
        /// max number of files to group in each plan
        max_num_files_to_group: usize,
        /// max total size limit of files to group in each plan
        max_total_file_size_to_group: usize,
    },

    /// Vertical Split always applies to L0.  This is triggered when we have too many overlapping L0s to
    /// compact in one batch, so we'll split files so they don't all overlap.  Its called "vertical" because
    /// if the L0s were drawn on a timeline, we'd then draw some vertical lines across L0s, and every place
    /// a line crosses a file, its split there.
    VerticalSplit {
        /// split_times are the exact times L0 files will be split at.  Only L0 files overlapping these times
        /// need split.
        split_times: Vec<i64>,
    },

    /// Deferred is holding place for regions we're not ready to work on yet.
    Deferred {},
}

impl Display for CompactType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TargetLevel { target_level, max_total_file_size_to_group  } => write!(f, "TargetLevel: {target_level} {max_total_file_size_to_group}"),
            Self::ManySmallFiles {
                start_level,
                max_num_files_to_group,
                max_total_file_size_to_group,
            } => write!(f, "ManySmallFiles: {start_level}, {max_num_files_to_group}, {max_total_file_size_to_group}",),
            Self::SimulatedLeadingEdge {
                max_num_files_to_group,
                max_total_file_size_to_group,
            } => write!(f, "SimulatedLeadingEdge: {max_num_files_to_group}, {max_total_file_size_to_group}",),
            Self::VerticalSplit  { split_times } => write!(f, "VerticalSplit: {split_times:?}"),
            Self::Deferred {} => write!(f, "Deferred"),
        }
    }
}

impl CompactType {
    /// what levels should the files in this round be?
    pub fn target_level(&self) -> CompactionLevel {
        match self {
            Self::TargetLevel { target_level, .. } => *target_level,
            // For many files, start level is the target level
            Self::ManySmallFiles { start_level, .. } => *start_level,
            Self::SimulatedLeadingEdge { .. } => CompactionLevel::FileNonOverlapped,
            Self::VerticalSplit { .. } => CompactionLevel::Initial,
            Self::Deferred {} => CompactionLevel::Initial, // n/a
        }
    }

    /// Is this round in many small files mode?
    pub fn is_many_small_files(&self) -> bool {
        matches!(self, Self::ManySmallFiles { .. })
    }

    /// Is this round in simulated leading edge mode?
    pub fn is_simulated_leading_edge(&self) -> bool {
        matches!(self, Self::SimulatedLeadingEdge { .. })
    }

    /// return max_num_files_to_group, when available.
    pub fn max_num_files_to_group(&self) -> Option<usize> {
        match self {
            Self::TargetLevel { .. } => None,
            Self::ManySmallFiles {
                max_num_files_to_group,
                ..
            } => Some(*max_num_files_to_group),
            Self::SimulatedLeadingEdge {
                max_num_files_to_group,
                ..
            } => Some(*max_num_files_to_group),
            Self::VerticalSplit { .. } => None,
            Self::Deferred {} => None,
        }
    }

    /// return max_total_file_size_to_group, when available.
    pub fn max_total_file_size_to_group(&self) -> Option<usize> {
        match self {
            Self::TargetLevel { .. } => None,
            Self::ManySmallFiles {
                max_total_file_size_to_group,
                ..
            } => Some(*max_total_file_size_to_group),
            Self::SimulatedLeadingEdge {
                max_total_file_size_to_group,
                ..
            } => Some(*max_total_file_size_to_group),
            Self::VerticalSplit { .. } => None,
            Self::Deferred {} => None,
        }
    }

    /// return split_times, when available.
    pub fn split_times(&self) -> Option<Vec<i64>> {
        match self {
            Self::TargetLevel { .. } => None,
            Self::ManySmallFiles { .. } => None,
            Self::SimulatedLeadingEdge { .. } => None,
            Self::VerticalSplit { split_times } => Some(split_times.clone()),
            Self::Deferred {} => None,
        }
    }

    pub fn is_deferred(&self) -> bool {
        matches!(self, Self::Deferred {})
    }
}

// CompactRange describes a range of files by the min/max time and allows these files to be compacted without
// consideration of other ranges.  Each range may go through many rounds of compaction before being
// consolidated with adjacent ranges.
#[derive(Debug)]
pub struct CompactRange {
    // The type of operation required for this range.
    pub op: CompactType,
    /// The minimum time of any file in the range
    pub min: i64,
    /// The maximum time of any file in the range
    pub max: i64,
    /// The sum of the sizes of all files in the range
    pub cap: usize,
    // Inidcates if L0s are present in this range (used when consolidating ranges)
    pub has_l0s: bool,
    /// Files to be potentially operated on now.  Files start here, then go to branches or files_for_later.
    pub files_for_now: Mutex<Option<Vec<ParquetFile>>>,
    /// Compaction branches within this range
    pub branches: Mutex<Option<Vec<Vec<ParquetFile>>>>,
    /// Files ignored for now, but will be considered later
    pub files_for_later: Mutex<Option<Vec<ParquetFile>>>,
}

impl Display for CompactRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CompactRange: {}, {}->{}, {}, has_l0s:{}, {}, {}, {}",
            self.op,
            self.min,
            self.max,
            self.cap,
            self.has_l0s,
            if let Some(files_for_now) = &*self.files_for_now.lock().unwrap() {
                format!("files_for_now: {}", files_for_now.len())
            } else {
                "".to_string()
            },
            if let Some(branches) = &*self.branches.lock().unwrap() {
                format!("branches: {}", branches.len())
            } else {
                "".to_string()
            },
            if let Some(files_for_later) = &*self.files_for_later.lock().unwrap() {
                format!("files_for_later: {}", files_for_later.len())
            } else {
                "".to_string()
            },
        )
    }
}

impl Clone for CompactRange {
    fn clone(&self) -> Self {
        Self {
            op: self.op.clone(),
            min: self.min,
            max: self.max,
            cap: self.cap,
            has_l0s: self.has_l0s,
            branches: Mutex::new(self.branches.lock().unwrap().clone()),
            files_for_later: Mutex::new(self.files_for_later.lock().unwrap().clone()),
            files_for_now: Mutex::new(self.files_for_now.lock().unwrap().clone()),
        }
    }
}

impl CompactRange {
    pub fn add_files_for_now(&self, files: Vec<ParquetFile>) {
        if !files.is_empty() {
            let mut files_for_now = self.files_for_now.lock().unwrap();
            if let Some(files_for_now) = &mut *files_for_now {
                files_for_now.extend(files);
            } else {
                *files_for_now = Some(files);
            }
        }
    }

    pub fn add_files_for_later(&self, files: Vec<ParquetFile>) {
        if !files.is_empty() {
            let mut files_for_later = self.files_for_later.lock().unwrap();
            if let Some(files_for_later) = &mut *files_for_later {
                files_for_later.extend(files);
            } else {
                *files_for_later = Some(files);
            }
        }
    }
}

/// RoundInfo is comprised of CompactRanges of files to be compacted.
#[derive(Debug)]
pub struct RoundInfo {
    /// ranges are the CompactRanges of files which do not overlap at the L0/L1 levels.
    /// When we have large numbers of overlapped L0s, dividing them into ranges keeps them in bite sized chunks that
    /// are easier to deal with.  As L0s are comparted to L1s, the ranges are combined.  Its preferable to do the L1->L2
    /// compaction with a single range to avoid unnecessary divisions within the L2 files.
    pub ranges: Vec<CompactRange>,

    /// l2_files_for_later holds L2 files while the ranges have L0s.  Splitting into many ranges is necessary for the L0->L1
    /// compactions, to keep the problem size manageable.  It would be unfortunate (inefficient) to split L2 files to prevent
    /// them from spanning multiple CompactRanges.  So instead, the L2 files are held in files_for_later until the L0s are gone,
    /// at which point we'll have a single range, which gets the L2 files.
    /// Note that each CompactRange also has its own files_for_later, which are L0/L1 files within that range that aren't being
    /// compacted in the current round.
    pub l2_files_for_later: Mutex<Option<Vec<ParquetFile>>>,
}

impl Display for RoundInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for range in &self.ranges {
            writeln!(f, "{:?}", range)?;
        }
        Ok(())
    }
}

impl RoundInfo {
    /// take_files_for_later takes the files_for_later from the RoundInfo.
    pub fn take_l2_files_for_later(&self) -> Option<Vec<ParquetFile>> {
        let l2_files_for_later = self.l2_files_for_later.lock().unwrap().take();
        l2_files_for_later
    }
}
