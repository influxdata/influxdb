//! Report component system state.

use observability_deps::tracing::info;

use crate::config::Config;

use super::Components;

/// Log config at info level.
pub fn log_config(config: &Config) {
    // use struct unpack so we don't forget any members
    let Config {
        // no need to print the internal state of the registry
        metric_registry: _,
        // no need to print the internal state of the trace collector
        trace_collector: _,
        catalog,
        scheduler_config,
        parquet_store_real,
        parquet_store_scratchpad,
        exec,
        time_provider,
        backoff_config,
        partition_concurrency,
        df_concurrency,
        partition_scratchpad_concurrency,
        max_desired_file_size_bytes,
        percentage_max_file_size,
        split_percentage,
        partition_timeout,
        shadow_mode,
        enable_scratchpad,
        min_num_l1_files_to_compact,
        process_once,
        parquet_files_sink_override,
        simulate_without_object_store,
        all_errors_are_fatal,
        max_num_columns_per_table,
        max_num_files_per_plan,
        max_partition_fetch_queries_per_second,
    } = &config;

    let parquet_files_sink_override = parquet_files_sink_override
        .as_ref()
        .map(|_| "Some")
        .unwrap_or("None");

    info!(
        %catalog,
        %scheduler_config,
        %parquet_store_real,
        %parquet_store_scratchpad,
        %exec,
        %time_provider,
        ?backoff_config,
        partition_concurrency=partition_concurrency.get(),
        df_concurrency=df_concurrency.get(),
        partition_scratchpad_concurrency=partition_scratchpad_concurrency.get(),
        max_desired_file_size_bytes,
        percentage_max_file_size,
        split_percentage,
        partition_timeout_secs=partition_timeout.as_secs_f32(),
        shadow_mode,
        enable_scratchpad,
        min_num_l1_files_to_compact,
        process_once,
        simulate_without_object_store,
        %parquet_files_sink_override,
        all_errors_are_fatal,
        max_num_columns_per_table,
        max_num_files_per_plan,
        max_partition_fetch_queries_per_second,
        "config",
    );
}

/// Log component system at info level.
pub fn log_components(components: &Components) {
    // use struct unpack so we don't forget any members
    let Components {
        compaction_job_stream,
        partition_info_source,
        partition_files_source,
        round_info_source,
        partition_filter,
        post_classification_partition_filter: partition_too_large_to_compact_filter,
        compaction_job_done_sink,
        commit,
        ir_planner,
        df_planner,
        df_plan_exec,
        parquet_files_sink,
        round_split,
        divide_initial,
        scratchpad_gen,
        file_classifier,
        changed_files_filter,
    } = components;

    info!(
        %compaction_job_stream,
        %partition_info_source,
        %partition_files_source,
        %round_info_source,
        %partition_filter,
        %partition_too_large_to_compact_filter,
        %compaction_job_done_sink,
        %commit,
        %ir_planner,
        %df_planner,
        %df_plan_exec,
        %parquet_files_sink,
        %round_split,
        %divide_initial,
        %scratchpad_gen,
        %file_classifier,
        %changed_files_filter,
        "component setup",
    );
}
