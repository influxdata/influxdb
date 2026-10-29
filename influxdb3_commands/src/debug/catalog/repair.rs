use std::error::Error;

use super::{CatalogFormat, CommonArgs, render};

#[derive(Debug, clap::Parser)]
pub struct Args {
    /// Write the repaired snapshot back to the object store. Without this
    /// flag the command is a dry run that only reports what it would change.
    /// Stop every node in the cluster first and restart them only after the
    /// write completes: a node still accepting catalog changes can overwrite
    /// the repaired snapshot when it checkpoints. The write is refused while
    /// the node registry lists nodes not confirmed stopped (see
    /// --allow-unstopped-nodes) or if object-store activity appears during
    /// --quiesce-window. On stores without conditional-write support (e.g.
    /// the Core local file store) the final write is a plain, non-atomic
    /// overwrite.
    #[clap(long = "execute")]
    pub execute: bool,
    /// How long to watch the object store before an --execute write, as a
    /// duration such as 30s or 2m. A new catalog log, a rewritten snapshot,
    /// or a new object under a registered node's `<node-id>/wal/` prefix or
    /// PachaTree snapshot directory during the window refuses the write. For
    /// PachaTree, make the window longer than the nodes' --snapshot-duration.
    #[clap(long = "quiesce-window", default_value = "30s", value_parser = humantime::parse_duration, requires = "execute")]
    pub quiesce_window: std::time::Duration,
    /// Proceed with --execute even when the node registry lists nodes not
    /// confirmed stopped. Needed when a node crashed rather than shut down
    /// cleanly, since it never records its stop; the object-store activity
    /// check still applies.
    #[clap(long = "allow-unstopped-nodes", requires = "execute")]
    pub allow_unstopped_nodes: bool,
    #[clap(flatten)]
    pub common: CommonArgs,
}

pub async fn run(args: Args) -> Result<(), Box<dyn Error>> {
    let store = args.common.store()?;
    let prefix = args.common.prefix()?.to_string();
    use influxdb3_catalog::repair::RepairError;
    let outcome = match influxdb3_catalog::repair::repair_catalog(
        store,
        &prefix,
        args.execute,
        args.quiesce_window,
        args.allow_unstopped_nodes,
        &mut |msg| eprintln!("{msg}"),
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(e @ (RepairError::ClusterActive { .. } | RepairError::NodesNotStopped { .. })) => {
            return Err(format!("{e}\n(run without --execute to inspect the repair plan)").into());
        }
        Err(e) => return Err(e.into()),
    };

    let status = if outcome.executed {
        format!(
            "repaired snapshot written at sequence {}; previous snapshot backed up to {}",
            outcome.plan.snapshot_sequence,
            outcome.backup_path.as_deref().unwrap_or("<none>")
        )
    } else if outcome.plan.renames.is_empty() && outcome.plan.superseded.is_empty() {
        "no duplicate-name or duplicate-id records found; nothing to repair".to_string()
    } else {
        "dry run: no changes written (pass --execute to write the repaired snapshot)".to_string()
    };

    println!("{}", render::structured(&outcome, args.common.format)?);
    if matches!(args.common.format, CatalogFormat::Pretty) {
        println!("\n{status}");
    } else {
        eprintln!("{status}");
    }
    Ok(())
}
