//! Report component system state.

use observability_deps::tracing::info;

use super::Components;

/// Log component system at info level.
pub fn log_components(components: &Components) {
    // use struct unpack so we don't forget any members
    let Components {
        partitions_source,
        partition_source,
        partition_files_source,
        files_filter,
        partition_filter,
        partition_done_sink,
        commit,
        tables_source,
        namespaces_source,
        df_planner,
        df_plan_exec,
        parquet_file_sink,
        round_split,
        divide_initial,
        scratchpad_gen,
    } = components;

    info!(
        %partitions_source,
        %partition_source,
        %partition_files_source,
        %files_filter,
        %partition_filter,
        %partition_done_sink,
        %commit,
        %tables_source,
        %namespaces_source,
        %df_planner,
        %df_plan_exec,
        %parquet_file_sink,
        %round_split,
        %divide_initial,
        %scratchpad_gen,
        "component setup",
    );
}
