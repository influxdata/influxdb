//! Report component system state.

use observability_deps::tracing::info;

use crate::config::{Config, ShardConfig};

use super::Components;

/// Log config at info level.
pub fn log_config(config: &Config) {
    // use struct unpack so we don't forget any members
    let Config {
        shard_id,
        // no need to print the internal state of the registry
        metric_registry: _,
        catalog,
        parquet_store_real,
        parquet_store_scratchpad,
        exec,
        time_provider,
        backoff_config,
        partition_concurrency,
        job_concurrency,
        partition_scratchpad_concurrency,
        partition_threshold,
        max_desired_file_size_bytes,
        percentage_max_file_size,
        split_percentage,
        partition_timeout,
        partitions_source,
        shadow_mode,
        ignore_partition_skip_marker,
        max_input_parquet_bytes_per_partition,
        shard_config,
        min_num_l1_files_to_compact,
        process_once,
        parquet_files_sink_override,
        commit_wrapper,
        simulate_without_object_store,
        all_errors_are_fatal,
        max_num_columns_per_table,
        max_num_files_per_plan,
    } = &config;

    let (shard_cfg_n_shards, shard_cfg_shard_id) = match shard_config {
        None => (None, None),
        Some(shard_config) => {
            // use struct unpack so we don't forget any members
            let ShardConfig { n_shards, shard_id } = shard_config;
            (Some(n_shards), Some(shard_id))
        }
    };

    let parquet_files_sink_override = parquet_files_sink_override
        .as_ref()
        .map(|_| "Some")
        .unwrap_or("None");

    let commit_wrapper = commit_wrapper.as_ref().map(|_| "Some").unwrap_or("None");

    info!(
        shard_id=shard_id.get(),
        %catalog,
        %parquet_store_real,
        %parquet_store_scratchpad,
        %exec,
        %time_provider,
        ?backoff_config,
        partition_concurrency=partition_concurrency.get(),
        job_concurrency=job_concurrency.get(),
        partition_scratchpad_concurrency=partition_scratchpad_concurrency.get(),
        partition_threshold_secs=partition_threshold.as_secs_f32(),
        max_desired_file_size_bytes,
        percentage_max_file_size,
        split_percentage,
        partition_timeout_secs=partition_timeout.as_secs_f32(),
        %partitions_source,
        shadow_mode,
        ignore_partition_skip_marker,
        max_input_parquet_bytes_per_partition,
        ?shard_cfg_n_shards,
        ?shard_cfg_shard_id,
        min_num_l1_files_to_compact,
        process_once,
        simulate_without_object_store,
        %parquet_files_sink_override,
        %commit_wrapper,
        all_errors_are_fatal,
        max_num_columns_per_table,
        max_num_files_per_plan,
        "config",
    );
}

/// Log component system at info level.
pub fn log_components(components: &Components) {
    // use struct unpack so we don't forget any members
    let Components {
        partition_stream,
        partition_info_source,
        partition_files_source,
        round_info_source,
        partition_filter,
        partition_resource_limit_filter,
        partition_done_sink,
        commit,
        ir_planner,
        df_planner,
        df_plan_exec,
        parquet_files_sink,
        round_split,
        divide_initial,
        scratchpad_gen,
        file_classifier,
    } = components;

    info!(
        %partition_stream,
        %partition_info_source,
        %partition_files_source,
        %round_info_source,
        %partition_filter,
        %partition_resource_limit_filter,
        %partition_done_sink,
        %commit,
        %ir_planner,
        %df_planner,
        %df_plan_exec,
        %parquet_files_sink,
        %round_split,
        %divide_initial,
        %scratchpad_gen,
        %file_classifier,
        "component setup",
    );
}
