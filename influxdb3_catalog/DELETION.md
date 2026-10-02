# Deletion

To fully comply with GDPR, the catalog must be able to fully delete all log files that contain data relevant to hard-deleted resources. For example, if a database is hard-deleted, then we must find and delete all log files which contain `ColumnsAdded` records where the relevant table was within that (as well as finding and deleting all other relevant log files).

To do this, we hook into a few core parts of the catalog and modify the behavior somewhat.

## Snapshots

Snapshots will be compacted to remove all records that are removed as a part of deletion. The records that are removed as a part of deletion will not be in log files and will not be in snapshots.

Once deletion is enabled, the compactor will monitor the catalog and delete (from object store; not from snapshots) all log files whose contents have been written into a snapshot. You may note that this is more work than is necessary to support deletes, but we believe it is better and easier than only deleting log files which contain hard-deleted resources. It allows us to avoid potentially-confusing catalog states where the object store is missing some log files but not all (and we don't know which ones are gone) and also allows the communication between the catalog and compactor to be simpler (since we can simply tell the compactor what is the maximum sequence number to delete instead of giving it a specific list of files to delete).

## Write/Update path

Once log files can be deleted, we can no longer rely on the presence of a log file to tell us whether or not any records were already persisted at that sequence number. A separate node could've written a log file, then persisted a snapshot, then the compactor could've deleted the original log file before the original node even checked for its existence. Note that this issue would be present whether we only deleted hard-deleted log files or every snapshot-persisted log file.

Here's a diagram to demonstrate a problematic case:

```
               Catalog Files

                   ┌ ─ ┐
                     4
                   └ ─ ┘
                              ┌───────────┐
                   ┌───┐      │           │
                   │ 3 │◀─────│  Node B   │
                   └───┘      │           │
                              └───────────┘
                   ┌───┐
                   │ 2 │
                   └───┘
┌───────────┐
│           │      ┌───┐
│  Node A   │─────▶│ 1 │
│           │      └───┘
└───────────┘
```

Two nodes:
- `A` is pointing at an older log sequence `1`, because it is not fully up-to-date
- `B` is pointing at the latest log sequence `3`

Then, `B` persists log `4` which is a deletion and which would result in the removal of catalog file `2`:
```
               Catalog Files

                   ┌───┐
                   │ 4 │◀─┐
                   └───┘  │
                       Delete ┌───────────┐
                   ┌───┐  │   │           │
                   │ 3 │  └───│  Node B   │
                   └───┘      │           │
                              └───────────┘
                   ┌ ─ ┐
                     2   This is deleted
                   └ ─ ┘
┌───────────┐
│           │      ┌───┐
│  Node A   │─────▶│ 1 │
│           │      └───┘
└───────────┘
```

Now, if `A` goes to persist a new log, then it will wrongly persist to sequence `2` (because there is no object there to fail the precondition):
```
               Catalog Files

                   ┌───┐
                   │ 4 │◀─┐
                   └───┘  │
                          │   ┌───────────┐
                   ┌───┐  │   │           │
                   │ 3 │  └───│  Node B   │
                   └───┘      │           │
                              └───────────┘
                   ╔═══╗
               ┌──▶║ 2 ║  ⚠ This is a false log!! ⚠
               │   ╚═══╝
┌───────────┐  │
│           │  │   ┌───┐
│  Node A   │──┘   │ 1 │
│           │      └───┘
└───────────┘
```

Because of this, we must now treat snapshots as the ultimate source of truth. To work with this new model, we need to alter the Write and Update paths slightly. Those ways are, in summary:
1. Require snapshots to be uploaded with the `if-match` header to ensure that each node only overwrites a snapshot if it ensures that it has already ingested and incorporated every bit of data that was previously in the snapshot.
2. When writing a log file, ensure that no new snapshot was written during the upload. If such a snapshot was written, then the sequence which was written to may have already been taken.
3. When reading a log file, ensure that no new snapshot was written during the read. If one was, its truthfulness trumps the log file's, and it should be applied instead.

### Snapshots must use `if-match`

To ensure that no data is lost, new snapshots must only be written if they verify that they will not be losing any data. To this end, they must populate the `if-match` header with the etag of the snapshot that the writing node most recently ingested. If a new snapshot was written in between this node ingesting a snapshot and trying to write a new one, the write will be rejected. The node will then need to read and ingest the new snapshot before continuing.

### Log files writes must watch for new snapshots

The exact flow that log-file-writing must follow is illustrated by this diagram:

```text
                 ┌───────────────────┐
                 │ Write the logfile │◄──────────────────────────┐
                 └────────┬──────────┘                           │
                          ▼                                      │
                ┌──────────────────────┐                         │
                │ Read latest snapshot │                         │
                │ with `if-none-match` │                         │
                └─────────┬────────────┘                         │
                          ▼                                      │
               Was the snapshot updated?                         │
                          │                                      │
                  ┌───────┴───────┐                              │
                  No             Yes                             │
                  │               ▼                              │
                  │          ┌──────────┐                        │
                  │          │ Apply it │                        │
                  │          └────┬─────┘                        │
                  │               │                              │
                  └───────┬───────┘                              │
                          ▼                                      │
               Did the initial write conflict?                   │
                          │                                      │
           ┌──────────────┴───────────────────────────────┐      │
           No                                            Yes     │
           │                                              │      │
           ▼                                              ▼      │
Was the snapshot just            ┌────────────┐    ┌─────────────┴────────┐
updated to include the ──Yes────►│ Delete the ├───►│ Run one iteration of │
sequence number that             │ logfile    │    │ background update    │
was just written?                └────────────┘    └──────────────────────┘
           │
           No
           │
           ▼
   ┌──────────────────┐
   │┌────────────────┐│
   ││  SUCCESS !!!!  ││
   │└────────────────┘│
   └──────────────────┘
```

In this diagram, there are a few things to note:
1. **A false log file may exist in object store for a short amount of time**. This is mitigated by the read-path semantics that always ingest snapshots instead of log files whenever new instances of both are found. This false log file will also be deleted soon by the catalog, so the chances of another node noticing it and reading it are very small anyways.
2. **The likelihood of hitting an edge case is quite small and accommodating for them is cheap**. Checking if another snapshot exists will be a single S3 request with `if-none-match` set so that no data (besides headers) will be returned in the vast majority of cases. If a snapshot does exist, we were going to read and ingest it on the next background update loop anyways, so it won't cost us anything extra to read and ingest here.

### Log file reads must watch for new snapshots

Because false log files might exist for very short amounts of time in object store, we must value snapshot records higher than log file records.

A false log file can only exist at sequence `N` if the following steps have occurred on a different node, in the following order:
1. A log file was successfully written to sequence `N`
2. A snapshot was written which contained sequence `N`
3. The compactor successfully deleted the original sequence-`N` log file

Because we know that these steps must occur in this order before a false log file can exist, we also know that whenever we retrieve both a new snapshot AND a new log file while running an iteration of the background update, the snapshot is either *just as truthful* or *more truthful* than the log file. So, in the case that we retrieve both, we can simply apply the snapshot and discard the log file.

Because of this same step order, we can also know that whenever we retrieve a log file and NOT a new snapshot, that log file will be true. Ingesting a snapshot requires that we update the in-memory sequence number (which tracks which log file we're going to try to retrieve next) to the number immediately past all the records that we just ingested. So if we retrieve a log file and not a new snapshot, we know that that log file's sequence number is not yet contained in a snapshot, and thus the log file cannot be false.
