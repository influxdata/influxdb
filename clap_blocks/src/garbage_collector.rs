//! Garbage Collector configuration
use clap::Parser;
use humantime::parse_duration;
use std::{fmt::Debug, time::Duration};

/// Configuration specific to the object store garbage collector
#[derive(Debug, Clone, Parser, Copy)]
pub struct GarbageCollectorConfig {
    /// If this flag is specified, don't delete the files in object storage. Only print the files
    /// that would be deleted if this flag wasn't specified.
    #[clap(long, env = "INFLUXDB_IOX_GC_DRY_RUN")]
    pub dry_run: bool,

    /// Items in the object store that are older than this duration that are not referenced in the
    /// catalog will be deleted.
    /// Parsed with <https://docs.rs/humantime/latest/humantime/fn.parse_duration.html>
    ///
    /// If not specified, defaults to 14 days ago.
    #[clap(
        long,
        default_value = "14d",
        value_parser = parse_duration,
        env = "INFLUXDB_IOX_GC_OBJECTSTORE_CUTOFF"
    )]
    pub objectstore_cutoff: Duration,

    /// Number of minutes to sleep between iterations of the objectstore list loop.
    /// This is the sleep between entirely fresh list operations.
    /// Defaults to 30 minutes.
    #[clap(
        long,
        default_value_t = 30,
        env = "INFLUXDB_IOX_GC_OBJECTSTORE_SLEEP_INTERVAL_MINUTES"
    )]
    pub objectstore_sleep_interval_minutes: u64,

    /// Number of milliseconds to sleep between listing consecutive chunks of objecstore files.
    /// Object store listing is processed in batches; this is the sleep between batches.
    /// Defaults to 1000 milliseconds.
    #[clap(
        long,
        default_value_t = 1000,
        env = "INFLUXDB_IOX_GC_OBJECTSTORE_SLEEP_INTERVAL_BATCH_MILLISECONDS"
    )]
    pub objectstore_sleep_interval_batch_milliseconds: u64,

    /// Parquet file rows in the catalog flagged for deletion before this duration will be deleted.
    /// Parsed with <https://docs.rs/humantime/latest/humantime/fn.parse_duration.html>
    ///
    /// If not specified, defaults to 14 days ago.
    #[clap(
        long,
        default_value = "14d",
        value_parser = parse_duration,
        env = "INFLUXDB_IOX_GC_PARQUETFILE_CUTOFF"
    )]
    pub parquetfile_cutoff: Duration,

    /// Number of minutes to sleep between iterations of the parquet file deletion loop.
    ///
    /// Defaults to 30 minutes.
    ///
    /// If both INFLUXDB_IOX_GC_PARQUETFILE_SLEEP_INTERVAL_MINUTES and
    /// INFLUXDB_IOX_GC_PARQUETFILE_SLEEP_INTERVAL are specified, the smaller is chosen
    #[clap(long, env = "INFLUXDB_IOX_GC_PARQUETFILE_SLEEP_INTERVAL_MINUTES")]
    pub parquetfile_sleep_interval_minutes: Option<u64>,

    /// Duration to sleep between iterations of the parquet file deletion loop.
    ///
    /// Defaults to 30 minutes.
    ///
    /// If both INFLUXDB_IOX_GC_PARQUETFILE_SLEEP_INTERVAL_MINUTES and
    /// INFLUXDB_IOX_GC_PARQUETFILE_SLEEP_INTERVAL are specified, the smaller is chosen
    #[clap(
        long,
        value_parser = parse_duration,
        env = "INFLUXDB_IOX_GC_PARQUETFILE_SLEEP_INTERVAL"
    )]
    pub parquetfile_sleep_interval: Option<Duration>,

    /// Number of minutes to sleep between iterations of the retention code.
    /// Defaults to 35 minutes to reduce incidence of it running at the same time as the parquet
    /// file deleter.
    #[clap(
        long,
        default_value_t = 35,
        env = "INFLUXDB_IOX_GC_RETENTION_SLEEP_INTERVAL_MINUTES"
    )]
    pub retention_sleep_interval_minutes: u64,
}

impl GarbageCollectorConfig {
    /// Returns the parquet_file sleep interval
    pub fn parquetfile_sleep_interval(&self) -> Duration {
        match (
            self.parquetfile_sleep_interval,
            self.parquetfile_sleep_interval_minutes,
        ) {
            (None, None) => Duration::from_secs(30 * 60),
            (Some(d), None) => d,
            (None, Some(m)) => Duration::from_secs(m * 60),
            (Some(d), Some(m)) => d.min(Duration::from_secs(m * 60)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gc_config() {
        let a: &[&str] = &[];
        let config = GarbageCollectorConfig::parse_from(a);
        assert_eq!(
            config.parquetfile_sleep_interval(),
            Duration::from_secs(30 * 60)
        );

        let config =
            GarbageCollectorConfig::parse_from(["something", "--parquetfile-sleep-interval", "3d"]);

        assert_eq!(
            config.parquetfile_sleep_interval(),
            Duration::from_secs(24 * 60 * 60 * 3)
        );

        let config = GarbageCollectorConfig::parse_from([
            "something",
            "--parquetfile-sleep-interval-minutes",
            "34",
        ]);
        assert_eq!(
            config.parquetfile_sleep_interval(),
            Duration::from_secs(34 * 60)
        );

        let config = GarbageCollectorConfig::parse_from([
            "something",
            "--parquetfile-sleep-interval-minutes",
            "34",
            "--parquetfile-sleep-interval",
            "35m",
        ]);
        assert_eq!(
            config.parquetfile_sleep_interval(),
            Duration::from_secs(34 * 60)
        );
    }
}
