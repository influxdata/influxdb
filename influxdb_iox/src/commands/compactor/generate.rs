//! Implements the `compactor generate` command.

use bytes::Bytes;
use clap::ValueEnum;
use clap_blocks::{
    catalog_dsn::CatalogDsnConfig,
    object_store::{make_object_store, ObjectStoreConfig, ObjectStoreType},
};
use object_store::DynObjectStore;
use snafu::prelude::*;
use std::{
    ffi::OsStr, fmt::Write, fs, num::NonZeroUsize, path::PathBuf, process::Command, sync::Arc,
};

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(flatten)]
    object_store_config: ObjectStoreConfig,

    #[clap(flatten)]
    catalog_dsn: CatalogDsnConfig,

    /// The type of compaction to be done on the files. If `hot` is specified, the generated
    /// files will have compaction level 0, will overlap with each other slightly, and will be
    /// marked that they were created within the last (approximately) 30 minutes. If `cold` is
    /// specified, the generated files will have compaction level 1, won't overlap with each other,
    /// and will be marked that they were created between 8 and 24 hours ago.
    #[clap(
        value_enum,
        value_parser,
        long = "compaction-type",
        env = "INFLUXDB_IOX_COMPACTOR_GENERATE_TYPE",
        default_value = "hot",
        action
    )]
    compaction_type: CompactionType,

    /// The number of IOx partitions to generate files for. Each partition will have the number
    /// of files specified by `--num-files` generated.
    #[clap(
        long = "num-partitions",
        env = "INFLUXDB_IOX_COMPACTOR_GENERATE_NUM_PARTITIONS",
        default_value = "1",
        action
    )]
    num_partitions: NonZeroUsize,

    /// The number of parquet files to generate per partition.
    #[clap(
        long = "num-files",
        env = "INFLUXDB_IOX_COMPACTOR_GENERATE_NUM_FILES",
        default_value = "1",
        action
    )]
    num_files: NonZeroUsize,

    /// The number of columns to generate in each file. One column will always be the
    /// timestamp. Additional columns will be given a type in I64, F64, String, Bool, and
    /// Tag in equal proportion.
    #[clap(
        long = "num-cols",
        env = "INFLUXDB_IOX_COMPACTOR_GENERATE_NUM_COLS",
        default_value = "6",
        action
    )]
    num_columns: NonZeroUsize,

    /// The number of rows to generate in each file.
    #[clap(
        long = "num-rows",
        env = "INFLUXDB_IOX_COMPACTOR_GENERATE_NUM_ROWS",
        default_value = "1",
        action
    )]
    num_rows: NonZeroUsize,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum CompactionType {
    Hot,
    Cold,
}

pub async fn run(config: Config) -> Result<()> {
    if !matches!(
        &config.object_store_config.object_store,
        Some(ObjectStoreType::File)
    ) {
        panic!("Sorry, this tool only works with 'file' object stores.");
    }

    let object_store = make_object_store(&config.object_store_config)?;

    let root_dir: PathBuf = config
        .object_store_config
        .database_directory
        .as_ref()
        .expect("--data-dir is required and has already been checked")
        .into();

    let compactor_data_dir = root_dir.join("compactor_data");
    let parquet_dir = compactor_data_dir.join("parquet");

    if compactor_data_dir
        .try_exists()
        .context(FileExistenceSnafu {
            path: &compactor_data_dir,
        })?
    {
        fs::remove_dir_all(&compactor_data_dir).context(RemoveSnafu {
            path: &compactor_data_dir,
        })?;
    }

    let spec_location = "compactor_data/spec.toml";
    let spec_in_root = compactor_data_dir.join("spec.toml");

    let Config {
        compaction_type,
        num_rows,
        num_files,
        ..
    } = config;

    let TimeValues {
        sampling_interval_ns,
        start_end_args,
    } = TimeValues::new(compaction_type, num_rows.get(), num_files.get());

    for (file_id, &start_end) in start_end_args
        .iter()
        .enumerate()
        .take(config.num_files.get())
    {
        write_data_generation_spec(
            file_id,
            Arc::clone(&object_store),
            config.num_columns.get(),
            sampling_interval_ns,
            spec_location,
        )
        .await?;

        let StartEndMinutesAgo { start, end } = start_end;

        generate_data(&spec_in_root, &parquet_dir, num_rows.get(), start, end)?;
    }

    Ok(())
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Could not parse the object store configuration"))]
    #[snafu(context(false))]
    ObjectStoreConfigParsing {
        source: clap_blocks::object_store::ParseError,
    },

    #[snafu(display("Could not write file to object storage"))]
    ObjectStoreWriting { source: object_store::Error },

    #[snafu(display("Could not parse object store path"))]
    ObjectStorePathParsing { source: object_store::path::Error },

    #[snafu(display("Subcommand failed: {status}"))]
    Subcommand { status: String },

    #[snafu(display("Could not check for existence of path {}", path.display()))]
    FileExistence {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Could not remove directory {}", path.display()))]
    Remove {
        path: PathBuf,
        source: std::io::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

async fn write_data_generation_spec(
    file_id: usize,
    object_store: Arc<DynObjectStore>,
    num_columns: usize,
    sampling_interval_ns: usize,
    spec_location: &str,
) -> Result<()> {
    let object_store_spec_path =
        object_store::path::Path::parse(spec_location).context(ObjectStorePathParsingSnafu)?;

    let contents = data_generation_spec_contents(file_id, sampling_interval_ns, num_columns);
    let data = Bytes::from(contents);

    object_store
        .put(&object_store_spec_path, data)
        .await
        .context(ObjectStoreWritingSnafu)?;

    Ok(())
}

fn generate_data(
    spec_in_root: impl AsRef<OsStr>,
    parquet_dir: impl AsRef<OsStr>,
    num_rows: usize,
    start: usize,
    end: usize,
) -> Result<()> {
    let status = Command::new("cargo")
        .arg("run")
        .arg("-p")
        .arg("iox_data_generator")
        .arg("--")
        .arg("--specification")
        .arg(&spec_in_root)
        .arg("--parquet")
        .arg(&parquet_dir)
        .arg("--start")
        .arg(&format!("{start} minutes ago"))
        .arg("--end")
        .arg(&format!("{end} minutes ago"))
        .arg("--batch-size")
        .arg(num_rows.to_string())
        .status()
        .expect("Running the data generator should have worked");

    ensure!(
        status.success(),
        SubcommandSnafu {
            status: status.to_string()
        }
    );

    Ok(())
}

fn data_generation_spec_contents(
    file_id: usize,
    sampling_interval_ns: usize,
    num_columns: usize,
) -> String {
    let mut spec = format!(
        r#"
name = "for_compaction"

[[database_writers]]
database_ratio = 1.0
agents = [{{name = "data_{file_id}", sampling_interval = "{sampling_interval_ns}ns"}}]

[[agents]]
name = "data_{file_id}"

[[agents.measurements]]
name = "measure"
"#
    );

    // The 1st column is always time, and the data generator always generates a timestamp without
    // any configuration needed, so the number of columns that need configuration is one less.
    let num_columns = num_columns - 1;

    // Every 5th column will be a tag.
    let num_tags = num_columns / 5;
    // The remaining columns will be fields of various types.
    let num_fields = num_columns - num_tags;

    // Tags go with the measurement, so they have to be specified in the config first.
    if num_tags > 0 {
        spec.push_str("tag_pairs = [\n");
        for tag_id in 1..=num_tags {
            let _ = write!(
                spec,
                r#"    {{key = "tag_{tag_id}", template = "{{{{random 1}}}}", regenerate_after_lines = 1}},"#
            );
            spec.push('\n');
        }
        spec.push_str("]\n")
    }

    for field_id in 0..num_fields {
        spec.push_str(&field_spec(field_id));
        spec.push('\n');
    }

    spec
}

fn field_spec(field_id: usize) -> String {
    match field_id % 4 {
        0 => format!(
            r#"
[[agents.measurements.fields]]
name = "i64_{field_id}"
i64_range = [0, 100]"#
        ),
        1 => format!(
            r#"
[[agents.measurements.fields]]
name = "f64_{field_id}"
f64_range = [0.0, 100.0]"#
        ),
        2 => format!(
            r#"
[[agents.measurements.fields]]
name = "string_{field_id}"
template = "{{{{random 4}}}}""#
        ),
        3 => format!(
            r#"
[[agents.measurements.fields]]
name = "bool_{field_id}"
bool = true"#
        ),
        _ => unreachable!("% 4 can only result in 0 - 3"),
    }
}

#[derive(Debug, PartialEq, Clone)]
struct TimeValues {
    sampling_interval_ns: usize,
    start_end_args: Vec<StartEndMinutesAgo>,
}

#[derive(Debug, PartialEq, Copy, Clone)]
struct StartEndMinutesAgo {
    start: usize,
    end: usize,
}

impl TimeValues {
    fn new(compaction_type: CompactionType, num_rows: usize, num_files: usize) -> Self {
        match compaction_type {
            CompactionType::Hot => {
                // Make the range approximately 30 min ago to now.
                let full_range_start_minutes = 30;
                let full_range_end_minutes = 0;

                // Overlap each file by this many minutes on the start and end with other files to
                // create realistic level 0 files for hot compaction.
                let overlap_minutes = 1;

                Self::inner(
                    full_range_start_minutes,
                    full_range_end_minutes,
                    overlap_minutes,
                    num_rows,
                    num_files,
                )
            }
            CompactionType::Cold => {
                // Make the range approximately 24 hours ago to 8 hours ago.
                let full_range_start_minutes = 24 * 60;
                let full_range_end_minutes = 8 * 60;

                // Don't overlap level 1 files
                let overlap_minutes = 0;

                Self::inner(
                    full_range_start_minutes,
                    full_range_end_minutes,
                    overlap_minutes,
                    num_rows,
                    num_files,
                )
            }
        }
    }

    // Clippy suggests changing `if overlap_minutes == 0 { 1 } else { 0 }` to
    // `usize::from(overlap_minutes == 0)`, but I think the original is clearer
    #[allow(clippy::bool_to_int_with_if)]
    fn inner(
        full_range_start_minutes: usize,
        full_range_end_minutes: usize,
        overlap_minutes: usize,
        num_rows: usize,
        num_files: usize,
    ) -> Self {
        // Divide the full range evenly across all files, plus the overlap on each end.
        let full_range_length_minutes = full_range_start_minutes - full_range_end_minutes;
        let minutes_per_file = full_range_length_minutes / num_files + overlap_minutes * 2;

        // Tell the generator to create one point every this many nanoseconds to create the
        // specified number of rows in each file.
        let fencepost_num_rows = if num_rows != 1 {
            num_rows - 1
        } else {
            num_rows
        };
        let sampling_interval_ns = (minutes_per_file * 60 * 1_000_000_000) / fencepost_num_rows;

        let start_end_args = (0..num_files)
            .rev()
            .map(|file_id| StartEndMinutesAgo {
                start: minutes_per_file * (file_id + 1) - overlap_minutes * file_id
                    + full_range_end_minutes,
                end: minutes_per_file * file_id - overlap_minutes * file_id
                    + full_range_end_minutes
                    // When the overlap is 0, subtract 1 because the data generator is inclusive
                    - (if overlap_minutes == 0 { 1 } else { 0 }),
            })
            .collect();

        Self {
            sampling_interval_ns,
            start_end_args,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod hot {
        use super::*;

        const COMPACTION_TYPE: CompactionType = CompactionType::Hot;

        #[test]
        fn one_row_one_file() {
            let num_rows = 1;
            let num_files = 1;
            let TimeValues {
                sampling_interval_ns,
                start_end_args,
            } = TimeValues::new(COMPACTION_TYPE, num_rows, num_files);

            assert_eq!(sampling_interval_ns, 1_920_000_000_000);
            assert_eq!(
                start_end_args,
                vec![StartEndMinutesAgo { start: 32, end: 0 }]
            );
        }

        #[test]
        fn one_thousand_rows_one_file() {
            let num_rows = 1_000;
            let num_files = 1;
            let TimeValues {
                sampling_interval_ns,
                start_end_args,
            } = TimeValues::new(COMPACTION_TYPE, num_rows, num_files);

            assert_eq!(sampling_interval_ns, 1_921_921_921);
            assert_eq!(
                start_end_args,
                vec![StartEndMinutesAgo { start: 32, end: 0 }]
            );
        }

        #[test]
        fn one_row_three_files() {
            let num_rows = 1;
            let num_files = 3;
            let TimeValues {
                sampling_interval_ns,
                start_end_args,
            } = TimeValues::new(COMPACTION_TYPE, num_rows, num_files);

            assert_eq!(sampling_interval_ns, 720_000_000_000);
            assert_eq!(
                start_end_args,
                vec![
                    StartEndMinutesAgo { start: 34, end: 22 },
                    StartEndMinutesAgo { start: 23, end: 11 },
                    StartEndMinutesAgo { start: 12, end: 0 },
                ]
            );
        }

        #[test]
        fn one_thousand_rows_three_files() {
            let num_rows = 1_000;
            let num_files = 3;
            let TimeValues {
                sampling_interval_ns,
                start_end_args,
            } = TimeValues::new(COMPACTION_TYPE, num_rows, num_files);

            assert_eq!(sampling_interval_ns, 720_720_720);
            assert_eq!(
                start_end_args,
                vec![
                    StartEndMinutesAgo { start: 34, end: 22 },
                    StartEndMinutesAgo { start: 23, end: 11 },
                    StartEndMinutesAgo { start: 12, end: 0 },
                ]
            );
        }
    }

    mod cold {
        use super::*;

        const COMPACTION_TYPE: CompactionType = CompactionType::Cold;

        #[test]
        fn one_row_one_file() {
            let num_rows = 1;
            let num_files = 1;
            let TimeValues {
                sampling_interval_ns,
                start_end_args,
            } = TimeValues::new(COMPACTION_TYPE, num_rows, num_files);

            assert_eq!(sampling_interval_ns, 57_600_000_000_000);
            assert_eq!(
                start_end_args,
                vec![StartEndMinutesAgo {
                    start: 24 * 60,
                    end: 8 * 60 - 1,
                }]
            );
        }

        #[test]
        fn one_thousand_rows_one_file() {
            let num_rows = 1_000;
            let num_files = 1;
            let TimeValues {
                sampling_interval_ns,
                start_end_args,
            } = TimeValues::new(COMPACTION_TYPE, num_rows, num_files);

            assert_eq!(sampling_interval_ns, 57_657_657_657);
            assert_eq!(
                start_end_args,
                vec![StartEndMinutesAgo {
                    start: 24 * 60,
                    end: 8 * 60 - 1,
                }]
            );
        }

        #[test]
        fn one_row_three_files() {
            let num_rows = 1;
            let num_files = 3;
            let TimeValues {
                sampling_interval_ns,
                start_end_args,
            } = TimeValues::new(COMPACTION_TYPE, num_rows, num_files);

            assert_eq!(sampling_interval_ns, 19_200_000_000_000);
            assert_eq!(
                start_end_args,
                vec![
                    StartEndMinutesAgo {
                        start: 1440,
                        end: 1119,
                    },
                    StartEndMinutesAgo {
                        start: 1120,
                        end: 799,
                    },
                    StartEndMinutesAgo {
                        start: 800,
                        end: 479,
                    },
                ]
            );
        }

        #[test]
        fn one_thousand_rows_three_files() {
            let num_rows = 1_000;
            let num_files = 3;
            let TimeValues {
                sampling_interval_ns,
                start_end_args,
            } = TimeValues::new(COMPACTION_TYPE, num_rows, num_files);

            assert_eq!(sampling_interval_ns, 19_219_219_219);
            assert_eq!(
                start_end_args,
                vec![
                    StartEndMinutesAgo {
                        start: 1440,
                        end: 1119,
                    },
                    StartEndMinutesAgo {
                        start: 1120,
                        end: 799,
                    },
                    StartEndMinutesAgo {
                        start: 800,
                        end: 479,
                    },
                ]
            );
        }
    }

    #[test]
    fn minimal_spec_contents() {
        let spec = data_generation_spec_contents(1, 1, 2);

        assert_eq!(
            spec,
            r#"
name = "for_compaction"

[[database_writers]]
database_ratio = 1.0
agents = [{name = "data_1", sampling_interval = "1ns"}]

[[agents]]
name = "data_1"

[[agents.measurements]]
name = "measure"

[[agents.measurements.fields]]
name = "i64_0"
i64_range = [0, 100]
"#
        );
    }

    #[test]
    fn many_columns_spec_contents() {
        let spec = data_generation_spec_contents(3, 100, 12);

        assert_eq!(
            spec,
            r#"
name = "for_compaction"

[[database_writers]]
database_ratio = 1.0
agents = [{name = "data_3", sampling_interval = "100ns"}]

[[agents]]
name = "data_3"

[[agents.measurements]]
name = "measure"
tag_pairs = [
    {key = "tag_1", template = "{{random 1}}", regenerate_after_lines = 1},
    {key = "tag_2", template = "{{random 1}}", regenerate_after_lines = 1},
]

[[agents.measurements.fields]]
name = "i64_0"
i64_range = [0, 100]

[[agents.measurements.fields]]
name = "f64_1"
f64_range = [0.0, 100.0]

[[agents.measurements.fields]]
name = "string_2"
template = "{{random 4}}"

[[agents.measurements.fields]]
name = "bool_3"
bool = true

[[agents.measurements.fields]]
name = "i64_4"
i64_range = [0, 100]

[[agents.measurements.fields]]
name = "f64_5"
f64_range = [0.0, 100.0]

[[agents.measurements.fields]]
name = "string_6"
template = "{{random 4}}"

[[agents.measurements.fields]]
name = "bool_7"
bool = true

[[agents.measurements.fields]]
name = "i64_8"
i64_range = [0, 100]
"#
        );
    }
}
