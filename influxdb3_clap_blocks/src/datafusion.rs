use std::collections::HashMap;

use datafusion::config::ConfigExtension;
use iox_query::config::IoxConfigExt;

/// Extends the standard [`HashMap`] based DataFusion config option in the CLI with specific
/// options (along with defaults) for InfluxDB 3 Core/Enterprise. This is intended for customization of
/// options that are defined in the `iox_query` crate, e.g., those defined in [`IoxConfigExt`]
/// that are relevant to the monolithinc versions of InfluxDB 3.
#[derive(Debug, clap::Parser, Clone)]
pub struct IoxQueryDatafusionConfig {
    /// When multiple parquet files are required in a sorted way (e.g. for de-duplication), we have
    /// two options:
    ///
    /// 1. **In-mem sorting:** Put them into `datafusion.target_partitions` DataFusion partitions.
    ///    This limits the fan-out, but requires that we potentially chain multiple parquet files into
    ///    a single DataFusion partition. Since chaining sorted data does NOT automatically result in
    ///    sorted data (e.g. AB-AB is not sorted), we need to preform an in-memory sort using
    ///    `SortExec` afterwards. This is expensive.
    /// 2. **Fan-out:** Instead of chaining files within DataFusion partitions, we can accept a
    ///    fan-out beyond `target_partitions`. This prevents in-memory sorting but may result in OOMs
    ///    (out-of-memory) if the fan-out is too large.
    ///
    /// We try to pick option 2 up to a certain number of files, which is configured by this
    /// setting.
    #[clap(
        long = "datafusion-max-parquet-fanout",
        env = "INFLUXDB3_DATAFUSION_MAX_PARQUET_FANOUT",
        default_value = "1000",
        action
    )]
    pub max_parquet_fanout: usize,

    /// Use a cached parquet loader when reading parquet files from object store
    ///
    /// This reduces IO operations to a remote object store as parquet is typically read via
    /// multiple read_range requests which would each require a IO operation. This will cache the
    /// entire parquet file in memory and serve the read_range requests from the cached data, thus
    /// requiring a single IO operation.
    #[clap(
        long = "datafusion-use-cached-parquet-loader",
        env = "INFLUXDB3_DATAFUSION_USE_CACHED_PARQUET_LOADER",
        default_value = "true",
        action
    )]
    pub use_cached_parquet_loader: bool,

    /// Provide custom configuration to DataFusion as a comma-separated list of key:value pairs.
    ///
    /// # Example
    /// ```text
    /// --datafusion-config "datafusion.key1:value1, datafusion.key2:value2"
    /// ```
    #[clap(
    long = "datafusion-config",
    env = "INFLUXDB3_DATAFUSION_CONFIG",
    default_value = "",
    value_parser = parse_datafusion_config,
    action
    )]
    pub datafusion_config: HashMap<String, String>,
}

impl IoxQueryDatafusionConfig {
    /// Build a [`HashMap`] to be used as the DataFusion config for the query executor
    ///
    /// This takes the provided `--datafusion-config` and extends it with options available on this
    /// [`IoxQueryDatafusionConfig`] struct. Note, any IOx extension parameters that are defined
    /// in the `datafusion_config` will be overridden by the provided values or their default. For
    /// example, if the user provides:
    /// ```
    /// --datafusion-config "iox.max_arquet_fanout:50"
    /// ```
    /// This will be overridden with with the default value for `max_parquet_fanout` of `1000`, or
    /// with the value provided for the `--datafusion-max-parquet-fanout` argument.
    pub fn build(mut self) -> HashMap<String, String> {
        self.datafusion_config.insert(
            format!("{prefix}.max_parquet_fanout", prefix = IoxConfigExt::PREFIX),
            self.max_parquet_fanout.to_string(),
        );
        self.datafusion_config.insert(
            format!(
                "{prefix}.use_cached_parquet_loader",
                prefix = IoxConfigExt::PREFIX
            ),
            self.use_cached_parquet_loader.to_string(),
        );
        self.datafusion_config
    }
}

fn parse_datafusion_config(
    s: &str,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(HashMap::with_capacity(0));
    }

    let mut out = HashMap::new();
    for part in s.split(',') {
        let kv = part.trim().splitn(2, ':').collect::<Vec<_>>();
        match kv.as_slice() {
            [key, value] => {
                let key_owned = key.trim().to_owned();
                let value_owned = value.trim().to_owned();
                let existed = out.insert(key_owned, value_owned).is_some();
                if existed {
                    return Err(format!("key '{key}' passed multiple times").into());
                }
            }
            _ => {
                return Err(
                    format!("Invalid key value pair - expected 'KEY:VALUE' got '{s}'").into(),
                );
            }
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use iox_query::{config::IoxConfigExt, exec::Executor};

    use super::IoxQueryDatafusionConfig;

    #[test_log::test]
    fn max_parquet_fanout() {
        let datafusion_config =
            IoxQueryDatafusionConfig::parse_from(["", "--datafusion-max-parquet-fanout", "5"])
                .build();
        let exec = Executor::new_testing();
        let mut session_config = exec.new_session_config();
        for (k, v) in &datafusion_config {
            session_config = session_config.with_config_option(k, v);
        }
        let ctx = session_config.build();
        let inner_ctx = ctx.inner().state();
        let config = inner_ctx.config();
        let iox_config_ext = config.options().extensions.get::<IoxConfigExt>().unwrap();
        assert_eq!(5, iox_config_ext.max_parquet_fanout);
    }
}
