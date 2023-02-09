//! Report component system state.

use observability_deps::tracing::info;

use crate::config::{Config, ShardConfig};

use super::Components;

/// Log config at info level.
pub fn log_config(config: &Config) {
    // use struct unpack so we don't forget any members
    let Config {
        shard_id,
        metric_registry,
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
        max_input_files_per_partition,
        max_input_parquet_bytes_per_partition,
        shard_config,
        compact_version,
        min_num_l1_files_to_compact,
        process_once,
        simulate_without_object_store,
        all_errors_are_fatal,
    } = &config;

    let (shard_cfg_n_shards, shard_cfg_shard_id) = match shard_config {
        None => (None, None),
        Some(shard_config) => {
            // use struct unpack so we don't forget any members
            let ShardConfig { n_shards, shard_id } = shard_config;
            (Some(n_shards), Some(shard_id))
        }
    };

    info!(
        shard_id=shard_id.get(),
        ?metric_registry,
        ?catalog,
        ?parquet_store_real,
        ?parquet_store_scratchpad,
        ?exec,
        ?time_provider,
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
        max_input_files_per_partition,
        max_input_parquet_bytes_per_partition,
        ?shard_cfg_n_shards,
        ?shard_cfg_shard_id,
        ?compact_version,
        min_num_l1_files_to_compact,
        process_once,
        simulate_without_object_store,
        all_errors_are_fatal,
        "config",
    );
}

/// Log component system at info level.
pub fn log_components(components: &Components) {
    // use struct unpack so we don't forget any members
    let Components {
        partition_stream,
        partition_source,
        partition_files_source,
        files_filter,
        partition_filter,
        partition_resource_limit_filter,
        partition_done_sink,
        commit,
        tables_source,
        namespaces_source,
        ir_planner,
        df_planner,
        df_plan_exec,
        parquet_file_sink,
        round_split,
        divide_initial,
        scratchpad_gen,
        target_level_chooser,
        target_level_split,
        non_overlap_split,
        upgrade_split,
    } = components;

    info!(
        %partition_stream,
        %partition_source,
        %partition_files_source,
        %files_filter,
        %partition_filter,
        %partition_resource_limit_filter,
        %partition_done_sink,
        %commit,
        %tables_source,
        %namespaces_source,
        %ir_planner,
        %df_planner,
        %df_plan_exec,
        %parquet_file_sink,
        %round_split,
        %divide_initial,
        %scratchpad_gen,
        %target_level_chooser,
        %target_level_split,
        %non_overlap_split,
        %upgrade_split,
        "component setup",
    );
}
