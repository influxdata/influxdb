//! Implements the `compactor generate` command.

use bytes::Bytes;
use clap::ValueEnum;
use clap_blocks::{
    catalog_dsn::CatalogDsnConfig,
    object_store::{make_object_store, ObjectStoreConfig},
};
use object_store::{path::Path, DynObjectStore};
use snafu::prelude::*;
use std::{fmt::Write, num::NonZeroUsize, sync::Arc};

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(flatten)]
    object_store_config: ObjectStoreConfig,

    #[clap(flatten)]
    catalog_dsn: CatalogDsnConfig,

    /// The type of compaction to be done on the files. If `hot` is specified, the generated
    /// files will have compaction level 0. If `cold` is specified, the generated files will
    /// have compaction level 1 and will be marked that they were created at least 8 hours ago.
    #[clap(
        arg_enum,
        value_parser,
        long = "--compaction-type",
        env = "INFLUXDB_IOX_COMPACTOR_GENERATE_TYPE",
        default_value = "hot",
        action
    )]
    compaction_type: CompactionType,

    /// The number of IOx partitions to generate files for. Each partition will have the number
    /// of files specified by `--num-files` generated.
    #[clap(
        long = "--num-partitions",
        env = "INFLUXDB_IOX_COMPACTOR_GENERATE_NUM_PARTITIONS",
        default_value = "1",
        action
    )]
    num_partitions: NonZeroUsize,

    /// The number of parquet files to generate per partition.
    #[clap(
        long = "--num-files",
        env = "INFLUXDB_IOX_COMPACTOR_GENERATE_NUM_FILES",
        default_value = "1",
        action
    )]
    num_files: NonZeroUsize,

    /// The number of columns to generate in each file. One column will always be the
    /// timestamp. Additional columns will be given a type in I64, F64, String, Bool, and
    /// Tag in equal proportion.
    #[clap(
        long = "--num-cols",
        env = "INFLUXDB_IOX_COMPACTOR_GENERATE_NUM_COLS",
        default_value = "6",
        action
    )]
    num_columns: NonZeroUsize,

    /// The number of rows to generate in each file.
    #[clap(
        long = "--num-rows",
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
    let object_store = make_object_store(&config.object_store_config)?;

    write_data_generation_spec(object_store, &config).await?;

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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

async fn write_data_generation_spec(
    object_store: Arc<DynObjectStore>,
    config: &Config,
) -> Result<()> {
    let path = Path::parse("compactor_data/line_protocol/spec.toml")
        .context(ObjectStorePathParsingSnafu)?;

    let contents = data_generation_spec_contents(1, 1, config.num_columns.get());
    let data = Bytes::from(contents);

    object_store
        .put(&path, data)
        .await
        .context(ObjectStoreWritingSnafu)?;

    Ok(())
}

fn data_generation_spec_contents(
    file_id: usize,
    sampling_interval_seconds: usize,
    num_columns: usize,
) -> String {
    let mut spec = format!(
        r#"
name = "for_compaction"

[[database_writers]]
database_ratio = 1.0
agents = [{{name = "data_{file_id}", sampling_interval = "{sampling_interval_seconds}s"}}]

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimal_spec_contents() {
        let spec = data_generation_spec_contents(1, 1, 2);

        assert_eq!(
            spec,
            r#"
name = "for_compaction"

[[database_writers]]
database_ratio = 1.0
agents = [{name = "data_1", sampling_interval = "1s"}]

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
agents = [{name = "data_3", sampling_interval = "100s"}]

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
