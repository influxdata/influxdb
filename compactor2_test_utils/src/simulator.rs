use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use async_trait::async_trait;
use data_types::{
    ColumnSet, CompactionLevel, ParquetFile, ParquetFileParams, SequenceNumber, ShardId, Timestamp,
};
use datafusion::physical_plan::SendableRecordBatchStream;
use iox_time::Time;
use observability_deps::tracing::{debug, info};
use uuid::Uuid;

use compactor2::{DynError, ParquetFilesSink, PartitionInfo, PlanIR};

use crate::{display::total_size, display_size, format_files};

/// Simulates the result of running a compaction plan that
/// produces multiple parquet files.
///
/// In general, since the Parquet format has high and variable
/// compression, it is not possible to predict what the output size of
/// a particular input will be, given just the schema and input
/// sizes. The output size depends heavily on the actual input data as
/// well as the parquet writer settings (e.g. row group size).
///
/// Rather than writing actual files during compactor tests, this
/// simulator produces [`ParquetFileParams`] describing a simulated
/// output of compacting one or more input parquet files, using rules
/// which can be alterted to testing how the compactor behaves
/// in different scenarios.
///
/// Scenarios that this simulator may offer in the future include:
///
/// 1. The output file size is significantly smaller than the
/// sum of the input files due to deduplication & delete application
///
/// 2. The distribution of data is nonuniform between the start and
/// end boundaries of the input files
///
/// 3. The output file time ranges are different than the the union of
/// the input files, due to delete predicate application or non
/// uniform distribution.
#[derive(Debug, Default)]
pub struct ParquetFileSimulator {
    /// entries that are added to while running
    run_log: Arc<Mutex<Vec<String>>>,
    /// Used to generate run ids for display
    run_id_generator: AtomicUsize,
}

impl std::fmt::Display for ParquetFileSimulator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ParquetFileSimulator")
    }
}

impl ParquetFileSimulator {
    /// Create a new simulator for creating parquet files, which
    /// appends its output to `run_log`
    pub fn new(run_log: Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            run_log,
            run_id_generator: AtomicUsize::new(0),
        }
    }

    fn next_run_id(&self) -> usize {
        self.run_id_generator.fetch_add(1, Ordering::SeqCst)
    }
}

#[async_trait]
impl ParquetFilesSink for ParquetFileSimulator {
    async fn stream_into_file_sink(
        &self,
        _streams: Vec<SendableRecordBatchStream>,
        partition_info: Arc<PartitionInfo>,
        target_level: CompactionLevel,
        plan_ir: &PlanIR,
    ) -> Result<Vec<ParquetFileParams>, DynError> {
        // compute max_l0_created_at
        let max_l0_created_at: Time = plan_ir
            .input_files()
            .iter()
            .map(|f| f.file.max_l0_created_at)
            .max()
            .expect("max_l0_created_at should have value")
            .into();

        info!("Simulating {plan_ir}");
        let (plan_type, split_times): (String, &[i64]) = match plan_ir {
            // pretend it is an empty split
            PlanIR::Compact { files: _ } => (plan_ir.to_string(), &[]),
            PlanIR::Split {
                files: _,
                split_times,
            } => {
                let plan_type = format!("{plan_ir}(split_times={split_times:?})");
                (plan_type, split_times)
            }
        };

        let input_files: Vec<_> = plan_ir
            .input_files()
            .iter()
            .map(|f| SimulatedFile::from(&f.file))
            .collect();

        let input_parquet_files: Vec<_> = plan_ir
            .input_files()
            .iter()
            .map(|f| f.file.clone())
            .collect();
        let column_set = overall_column_set(input_parquet_files.iter());
        let output_files = even_time_split(&input_files, split_times, target_level);
        let partition_info = partition_info.as_ref();

        // Compute final output
        let output_params: Vec<_> = output_files
            .into_iter()
            .map(|f| {
                f.into_parquet_file_params(max_l0_created_at, column_set.clone(), partition_info)
            })
            .collect();

        // record what the simulator did
        let run = SimulatedRun {
            run_id: self.next_run_id(),
            plan_type,
            input_parquet_files,
            output_params: output_params.clone(),
        };
        self.run_log.lock().unwrap().extend(run.into_strings());

        Ok(output_params)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Parameters of a `ParquetFile` that are part of the simulation
#[derive(Debug, Clone, Copy)]
pub struct SimulatedFile {
    /// the min timestamp of data in this file
    pub min_time: Timestamp,
    /// the max timestamp of data in this file
    pub max_time: Timestamp,
    /// file size in bytes
    pub file_size_bytes: i64,
    /// the number of rows of data in this file
    pub row_count: i64,
    /// the compaction level of the file
    pub compaction_level: CompactionLevel,
}

impl From<&ParquetFile> for SimulatedFile {
    fn from(value: &ParquetFile) -> Self {
        Self {
            min_time: value.min_time,
            max_time: value.max_time,
            file_size_bytes: value.file_size_bytes,
            row_count: value.row_count,
            compaction_level: value.compaction_level,
        }
    }
}

impl std::fmt::Display for SimulatedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ID.{}", self.compaction_level as i16)
    }
}

impl SimulatedFile {
    fn into_parquet_file_params(
        self,
        max_l0_created_at: Time,
        column_set: ColumnSet,
        partition_info: &PartitionInfo,
    ) -> ParquetFileParams {
        let Self {
            min_time,
            max_time,
            file_size_bytes,
            row_count,
            compaction_level,
        } = self;

        ParquetFileParams {
            shard_id: ShardId::new(1),
            namespace_id: partition_info.namespace_id,
            table_id: partition_info.table.id,
            partition_id: partition_info.partition_id,
            object_store_id: Uuid::new_v4(),
            max_sequence_number: SequenceNumber::new(0),
            min_time,
            max_time,
            file_size_bytes,
            row_count,
            compaction_level,
            created_at: Timestamp::new(1),
            column_set,
            max_l0_created_at: max_l0_created_at.into(),
        }
    }
}

/// Records the information about what the simulator did, for testing
/// purposes
#[derive(Debug, Clone)]
pub struct SimulatedRun {
    run_id: usize,
    // fields are used in testing
    plan_type: String,
    input_parquet_files: Vec<ParquetFile>,
    output_params: Vec<ParquetFileParams>,
}

impl SimulatedRun {
    /// Convert this simulated run into a set of human readable strings
    fn into_strings(self) -> impl Iterator<Item = String> {
        let Self {
            run_id,
            plan_type,
            input_parquet_files,
            output_params,
        } = self;

        let input_title = format!(
            "**** Simulation run {}, type={}. {} Input Files, {} total:",
            run_id,
            plan_type,
            input_parquet_files.len(),
            display_size(total_size(&input_parquet_files))
        );

        let output_title = format!(
            "**** {} Output Files (parquet_file_id not yet assigned), {} total:",
            output_params.len(),
            display_size(total_size(&output_params))
        );

        // hook up inputs and outputs
        format_files(input_title, &input_parquet_files)
            .into_iter()
            .chain(format_files(output_title, &output_params).into_iter())
    }
}

fn overall_column_set<'a>(files: impl IntoIterator<Item = &'a ParquetFile>) -> ColumnSet {
    let all_columns = files
        .into_iter()
        .fold(BTreeSet::new(), |mut columns, file| {
            columns.extend(file.column_set.iter().cloned());
            columns
        });
    ColumnSet::new(all_columns)
}

/// Calculate simulated output files based on splitting files
/// according to `split_times`, assuming the data is uniformly
/// distributed between min and max times
fn even_time_split(
    files: &[SimulatedFile],
    split_times: &[i64],
    target_level: CompactionLevel,
) -> Vec<SimulatedFile> {
    let overall_min_time = files.iter().map(|f| f.min_time).min().unwrap();
    let overall_max_time = files.iter().map(|f| f.max_time).max().unwrap();
    let overall_time_range = overall_max_time - overall_min_time;

    let total_rows: i64 = files.iter().map(|f| f.row_count).sum();
    let total_size: i64 = files.iter().map(|f| f.file_size_bytes).sum();

    // compute the timeranges data each file will have
    let mut last_time = overall_min_time;
    let mut time_ranges: Vec<_> = split_times
        .iter()
        .map(|time| {
            let time = Timestamp::new(*time);
            let ret = (last_time, time);
            last_time = time;
            ret
        })
        .collect();

    // add the entry for the last bucket
    time_ranges.push((last_time, overall_max_time));

    debug!(
        ?overall_min_time,
        ?overall_max_time,
        ?overall_time_range,
        ?total_rows,
        ?total_size,
        ?time_ranges,
        "creating output file from input files"
    );

    time_ranges
        .into_iter()
        .map(|(min_time, max_time)| {
            let p = ((max_time - min_time).get() as f64) / ((overall_time_range).get() as f64);

            let file_size_bytes = (total_size as f64 * p) as i64;
            let row_count = (total_rows as f64 * p) as i64;
            let compaction_level = target_level;

            info!(
                ?p,
                ?min_time,
                ?max_time,
                ?file_size_bytes,
                ?row_count,
                ?compaction_level,
                "creating output file with fraction of output"
            );
            SimulatedFile {
                min_time,
                max_time,
                file_size_bytes,
                row_count,
                compaction_level,
            }
        })
        .collect()
}
