# Catalog v3

## Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Component → file map](#component--file-map)
- [Binary framing format](#binary-framing-format)
- [Record catalog](#record-catalog)
- [Persistence](#persistence)
- [Field families & limits](#field-families--limits)
- [Forward compatibility](#forward-compatibility)
- [Upgrades](#upgrades)
- [Developer guide](#developer-guide)
- [Limitations](#limitations)
- [References & tracked work](#references--tracked-work)

## Overview

This is the reference for the v3 catalog. It is written for readers encountering the catalog for the first time, for developers implementing new catalog features, and as a refresher for seasoned maintainers: what the catalog is, how it works, where each component lives, how to extend it, how upgrades behave, and its current limitations.

For the rationale and alternatives behind the design, see the original [Catalog v3 Design][orig-design].

[orig-design]: https://github.com/influxdata/influxdb_pro/blob/tjh/cat-v3-design/docs/design/2026-02-25-catalog-v3-design.md

The catalog is the single source of truth for cluster metadata — databases, tables, columns, field families, caches, triggers, tokens, nodes, retention, and storage configuration. It is persisted to object store and observed by in-process subscribers (the write buffer, caches, processing engine, compactor, etc.) through domain events.

v3 does not change that role. It replaces v2's JSON serialization with a binary framing format, introduces a persistence boundary (frozen record types) separate from runtime types, and adds a forward-compatibility mechanism (version-gated records with a cluster-wide committed feature level) so that mixed-version clusters can upgrade without a coordinated all-node restart.

### What v3 provides

v2 serialized the catalog as JSON, with mutations modeled as `CatalogBatch` enum variants. v3 keeps the catalog's role but changes how it is represented and evolved, to achieve three things:

- **Forward compatibility.** Version-gated records and a cluster-wide committed feature level let a mixed-version cluster upgrade without a coordinated all-node restart; an `UPGRADE_SAFE` escape hatch covers records that older nodes can safely ignore.
- **A persistence boundary.** Frozen `CatalogRecord` types are separate from the runtime types, so the on-disk format and the in-memory API evolve independently.
- **A compact binary format.** `bitcode`-encoded record bodies in a fixed framing format are roughly 5–6× smaller than the equivalent v2 JSON and avoid full-JSON deserialization on load.

Two characteristics carry over from v2 and remain [limitations](#limitations): the in-memory catalog is mutated under a single lock with sequential apply, and persistence is a single globally sequenced log.

---

## Architecture

### Three-layer model

Catalog mutations flow through three layers, each a distinct trait:

| Layer | Trait | Role | Lifetime |
|-------|-------|----------|
| Operation | `CatalogOp` | Validate input, enforce limits, produce records | Transient — exists between `prepare` and `output` |
| Record | `CatalogRecord` | Binary persistence unit | **Frozen** — immutable once shipped |
| Event | `CatalogEvent` | Domain event for subscribers | Transient — derived from a record after apply |

**`CatalogOp`** (`oss/influxdb3_catalog/src/catalog/versions/v3/ops/mod.rs`) is the entry point for explicit DDL-style mutations. It is `pub(crate)` — Ops are an internal mechanism, invoked through `Catalog`'s public methods. The actual trait:

```rust
pub(crate) trait CatalogOp: Sized {
    type Input;
    type Output;

    // Optional: enforce resource limits against current usage. Default: no-op.
    fn limits_check(
        _args: &Self::Input,
        _catalog: &InnerCatalog,
        _usage: &CurrentCatalogUsage,
        _limiter: &dyn CatalogLimiter,
    ) -> Result<(), CatalogError> {
        Ok(())
    }

    // Validate input against current state; push records into the batch.
    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError>;

    // After apply, retrieve the affected resource and return it to the caller.
    fn output(&self, catalog: &InnerCatalog) -> Self::Output;
}
```

`Output` is the affected resource (e.g. `Arc<DatabaseSchema>`). `limits_check` is the hook for per-table/per-database limit enforcement (see [Field families & limits](#field-families--limits)).

**`CatalogRecord`** (`oss/influxdb3_catalog/src/format/registry.rs`) is the persistence boundary. Records have a fixed `RecordId`, flags, and a name; they `apply` to `InnerCatalog` and derive their `event`. Record struct layouts are **frozen** once shipped — new functionality is added by defining new record types, never by modifying existing ones.

```rust
pub trait CatalogRecord: Sized + Encode {
    const ID: RecordId;
    const FLAGS: RecordFlags;
    const NAME: &'static str;

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError>;
    fn event(&self) -> CatalogEvent;
}
```

The body encoding is abstracted behind `Encode`/`Decode` (`oss/influxdb3_catalog/src/format/mod.rs`), so the serialization library (currently `bitcode`) can be swapped without touching record types:

```rust
pub trait Encode { fn encode(&self, buf: &mut Vec<u8>); }
pub trait Decode: Sized { fn decode(buf: &[u8]) -> Result<Self, FormatError>; }
```

`Decode: Sized` is kept off the `CatalogRecord` trait to preserve dyn-safety; the registry stores per-type decode function pointers.

**`CatalogEvent`** (`oss/influxdb3_catalog/src/catalog/versions/v3/events.rs`) is a coarse-grained domain event. Events are not persisted — they are derived from records at apply time, carry only identifiers, and are `#[non_exhaustive]` so out-of-crate subscribers must include a wildcard arm. Subscribers look up catalog state on receipt and are idempotent.

Relationships: one `CatalogOp` produces one or more `CatalogRecord`s (1:N); one `CatalogRecord` produces exactly one `CatalogEvent` (1:1).

### Lifecycle

A mutation through the Op path flows:

```
  Input
    │  prepare()                       (validate + limits, produce records)
    ▼
 RecordBatch ──serialize──▶ file bytes
    │
    │  check_batch_against_committed() (feature-level write gate)
    ▼
 persist to object store               (conditional write, under write_permit)
    │
    ├─ apply()    each record → InnerCatalog   (sequential)
    ├─ event()    each record → CatalogEvent
    ▼
 broadcast (ack-gated: wait for every subscriber to ACK)
    │  output()
    ▼
  Output
```

1. **prepare** — `CatalogOp::prepare()` validates against current state and produces a `RecordBatch`.
2. **gate** — `check_batch_against_committed()` rejects any record above the committed feature level (unless `UPGRADE_SAFE`); see [Forward compatibility](#forward-compatibility).
3. **serialize** — records are serialized to the binary framing format.
4. **persist** — the file is written to object store with an optimistic-concurrency conditional write, under the `write_permit`.
5. **apply** — on success, each record's `apply()` mutates `InnerCatalog`, in order.
6. **broadcast** — each record's `event()` is collected into a `CatalogUpdate` and broadcast; the broadcaster waits for all subscribers to ACK.
7. **output** — `CatalogOp::output()` returns the affected resource.

### In-memory state and locking

`InnerCatalog` (`oss/influxdb3_catalog/src/catalog/versions/v3/inner.rs`) holds the authoritative in-memory state: `nodes`, `databases` (each `DatabaseSchema` carrying its tables/columns/caches/triggers), `tokens` + `token_permissions`, `query_groups`, `sequence`, `committed_feature_level`, `storage_mode`, `generation_config`, and `ordered_records` (the snapshot payload).

`Catalog` (`oss/influxdb3_catalog/src/catalog/versions/v3/catalog.rs`) wraps it as a single `RwLock<InnerCatalog>` plus an async `Mutex<CatalogSequenceNumber>` (the `write_permit`). The `write_permit` serializes the prepare→serialize→persist step within a node; cross-node serialization is handled by the object store conditional write. Apply and broadcast take the `inner` write lock, and `apply_records` processes a file's records sequentially.

### Write paths

**Op path — `Catalog::update::<Op>()`.** Used for explicit mutations: create/delete database, register/stop node, create/delete cache, create/enable/disable/delete trigger, token operations, retention, config. Acquires the `write_permit`, prepares, gates, serializes, persists (retrying on `AlreadyExists` by loading remote logs and re-preparing), then applies + broadcasts.

**Transaction path — `Catalog::begin_database_transaction()` / commit.** Used for schema-on-write during line-protocol ingestion **and** for DDL table creation. The buffer (or the `create_table` API) opens a `DatabaseCatalogTransaction`, accumulates `CreateTable` / `AddColumns` changes through `TableTransaction` (which enforces column/tag/field-family limits and assigns field families as records are added), and commits atomically. On commit the accumulated `RecordBatch` is gated, serialized, persisted, applied, and broadcast — same as the Op path. If the catalog sequence advanced since `begin`, the commit returns a retry prompt. Because table creation shares this path, limit enforcement and field-family assignment are identical for DDL and line protocol; the `CreateTable` record is produced at transaction commit.

### Read / catch-up path

When a node must catch up to remote state — after an optimistic-concurrency conflict, or when the compactor synchronizes to a sequence — it loads consecutive log files from object store, applies each record, and emits its event so subscribers observe remote changes. This uses the same apply + broadcast machinery as the local write path.

### Event broadcasting

Each `CatalogRecord` derives one `CatalogEvent`. After a file is applied, its events are wrapped in a `CatalogUpdate` and broadcast via `send_update`. The broadcast is **ack-gated**, not fire-and-forget:

- Each subscriber registers by a unique static name (`Catalog::subscribe_to_updates(name)`) and receives a `CatalogUpdateMessage` over an `mpsc` channel of buffer size **1**.
- `send_update` sends one message per non-stopped subscriber, then waits for every subscriber to **drop** its `CatalogUpdateMessage` — the `Drop` impl fires a `oneshot` ACK. `send_update` returns only once all subscribers have ACK'd (or transitioned to stopped while the message was in flight).
- Because the broadcaster blocks until all ACKs return, subscribers must drop the message promptly and defer heavy work (queue it, or mark state stale) rather than processing inline before drop.

This back-pressures the apply loop to the slowest subscriber, guaranteeing that no catalog file advances until every subscriber has at least received the prior file's events.

Subscribers as of this writing include: the processing engine (trigger events), the write buffer / replica shutdown coordination (node events), the compactor producer (node events), and the last/distinct cache providers and PachaTree value caches (cache + delete events). All use wildcard catch-alls for unmatched events and re-read catalog state on receipt.

During `Catalog` initialization the snapshot and logs are replayed before any subscriber registers, so no events are emitted for startup replay.

---

## Component → file map

Where each component lives, rooted at `oss/influxdb3_catalog/src/`:

```
oss/influxdb3_catalog/src/
catalog.rs                          active `Catalog` re-export (→ versions::v3)
catalog/
  versions/{v1,v2,v3}.rs            v3 active; v1/v2 retained for migration; limit constants in v3.rs
  versions/v3/
    catalog.rs                      `Catalog`: RwLock<InnerCatalog> + write_permit; write paths; subscribe
    inner.rs                        `InnerCatalog`: collections, sequence, committed feature level, consensus
    events.rs                       `CatalogEvent`, `CatalogUpdate(Message)`, `send_update`, subscriptions
    transaction.rs                  `DatabaseCatalogTransaction`, `TableTransaction`, `parse_qualified_field_name`
    ops/                            `CatalogOp` trait + per-resource ops (database, table, cache, trigger, …)
    schema/                         runtime types; `FieldFamilyMode` in column.rs
    usage.rs                        `CatalogLimiter`, `CatalogLimits`, `MaximumColumnCountLimiter`
    deletes.rs                      `DeletionScope`
  migrations/v3.rs                  v2→v3: `FromV2`, synthesis, `UpgradedLog` (+ v3/conversions.rs)
format/
  mod.rs                            `Encode`/`Decode`, `RecordFlags`, MAGIC="IDB3", FORMAT_VERSION=1
  registry.rs                       `CatalogRecord` trait + `inventory` registry (BTreeMap<RecordId, _>)
  record_id.rs, record_ids.rs       `RecordId` (bit-15 core/enterprise partition); numeric ID constants
  records/                          the frozen record structs (database, table, cache, …)
  header.rs                         64-byte file header
  record.rs                         16-byte record header; `Record`, `RecordBatch`
  apply.rs                          `apply_records` (sequential), `apply_catalog_file`
  reader.rs                         `CatalogFile` parsing
  feature_level.rs                  `FeatureLevel`, `derive_feature_level`
object_store/versions/v3.rs         object-store layout: catalog/v3/{snapshot, logs/…}
```

---

## Binary framing format

Two file kinds share a 64-byte header and the same payload shape (records back-to-back): **log files** (one per mutation) and **snapshot files** (`SNAPSHOT` flag set, all records in application order). All multi-byte fields are little-endian and 4-byte aligned.

### File header (64 bytes)

| Offset | Size | Type | Field | Description |
|--------|------|------|-------|-------------|
| 0x00 | 4 | `[u8;4]` | magic | `"IDB3"` |
| 0x04 | 4 | `u32` | format_version | `1` |
| 0x08 | 4 | `u32` | header_crc | CRC32 of header bytes 0x0C–0x3F |
| 0x0C | 2 | `u16` | flags | File flags (below) |
| 0x0E | 2 | — | reserved | zeros |
| 0x10 | 16 | `[u8;16]` | catalog_uuid | catalog instance UUID |
| 0x20 | 8 | `u64` | sequence_number | catalog sequence for this file |
| 0x28 | 4 | `u32` | record_count | total records in payload |
| 0x2C | 4 | — | reserved | zeros |
| 0x30 | 8 | `u64` | payload_len | payload byte length |
| 0x38 | 4 | `u32` | payload_crc | CRC32 of payload |
| 0x3C | 4 | — | reserved | zeros |

### File flags (`u16`)

| Bit | Name | Meaning |
|-----|------|---------|
| 0 | SNAPSHOT (`1<<0`) | File is a snapshot. Unset ⇒ log file. |
| 1–15 | — | reserved |

### Record header (16 bytes)

The two `u16` fields pack into the first 4-byte word.

| Offset | Size | Type | Field | Description |
|--------|------|------|-------|-------------|
| 0x00 | 2 | `u16` | id | `RecordId` raw value |
| 0x02 | 2 | `u16` | flags | `RecordFlags` bitfield |
| 0x04 | 8 | `u64` | sequence | catalog sequence when the record was written |
| 0x0C | 4 | `u32` | length | byte length of the record body |

### RecordFlags (`u16`)

| Value | Name | Meaning |
|-------|------|---------|
| `0x0000` | NONE | feature-gated (default) |
| `0x0001` | UPGRADE_SAFE | writable before the committed level includes it; unknown such records are skipped on read |
| others | — | reserved |

### Record body and alignment

Bodies are `bitcode`-encoded via `Encode`/`Decode`. Headers (file, record) are aligned; record **bodies** are variable-length and not padded — the next record header follows immediately. Padding bodies for zero-copy reads is not worthwhile under `bitcode` (which requires full decode); revisit only if the body encoding changes to a zero-copy format.

---

## Record catalog

`RecordId` partitions the `u16` ID space by bit 15: core (`0x0001`–`0x7FFF`), enterprise (`0x8001`–`0xFFFF`).

**Rules:** never delete, never reorder, assign the next sequential ID within the partition. Deprecated records keep their ID and remain decodable (read-only).

### Core records (`src/format/record_ids.rs`)

| ID | Record | Event |
|----|--------|-------|
| 1 | AdvanceFeatureLevel | FeatureLevelAdvanced |
| 2 | RegisterNode | NodeRegistered |
| 3 | StopNode | NodeStopped |
| 4 | CreateDatabase | DatabaseCreated |
| 5 | SoftDeleteDatabase | DatabaseSoftDeleted |
| 6 | CreateTable | TableCreated |
| 7 | SoftDeleteTable | TableSoftDeleted |
| 8 | AddColumns | TableUpdated |
| 9 | CreateDistinctCache | DistinctCacheCreated |
| 10 | DeleteDistinctCache | DistinctCacheDeleted |
| 11 | CreateLastCache | LastCacheCreated |
| 12 | DeleteLastCache | LastCacheDeleted |
| 13 | CreateTrigger | TriggerCreated |
| 14 | DeleteTrigger | TriggerDeleted |
| 15 | EnableTrigger | TriggerEnabled |
| 16 | DisableTrigger | TriggerDisabled |
| 17 | SetDbRetentionPeriod | DatabaseRetentionPeriodChanged |
| 18 | ClearDbRetentionPeriod | DatabaseRetentionPeriodChanged |
| 19 | CreateAdminToken | TokenCreated |
| 20 | RegenerateAdminToken | TokenRegenerated |
| 21 | DeleteToken | TokenDeleted |
| 22 | HardDeleteDatabase | DatabaseHardDeleted |
| 23 | HardDeleteTable | TableHardDeleted |
| 24 | SetGenerationDuration | GenerationDurationChanged |
| 25 | SetStorageMode | StorageModeChanged |
| 26 | SetNextId | NextIdSet |

### Enterprise records

| Raw ID | Record | Event |
|--------|--------|-------|
| `0x8001` | CreateResourceScopedToken | TokenCreated |
| `0x8002` | SetTableRetentionPeriod | TableRetentionPeriodChanged |
| `0x8003` | ClearTableRetentionPeriod | TableRetentionPeriodChanged |
| `0x8004` | CreateLoginIdentityOAuth | LoginIdentityCreated |
| `0x8005` | DeleteLoginIdentityOAuth | LoginIdentityDeleted |
| `0x8006` | RestoreCatalog | CatalogRestored |
| `0x8007` | CreateQueryGroup | QueryGroupCreated |
| `0x8008` | UpdateQueryGroup | QueryGroupUpdated |
| `0x8009` | DeleteQueryGroup | QueryGroupDeleted |

`SetNextId` (26) explicitly sets a repository's id counter; it is produced during migration and compaction rather than by user operations.

`RestoreCatalog` (e6) is special: applying it does not mutate part of `InnerCatalog` — it loads a backup snapshot + log files from the recorded paths and replaces the entire state. The body carries only `{ time_ns, restore_id, checkpoint_path, log_paths }`. Because the load is async I/O against the object store, the apply driver special-cases this record: callers pre-load the backup state via `preload_restore_for_records` (async, off-lock) and the sync `apply_records` consumes the pre-loaded `InnerCatalog` when it encounters the record. The restore record is **not** retained in `ordered_records` — `Catalog::restore` forces a checkpoint right after apply, so the next snapshot captures the restored state directly and the backup paths in the persisted restore log stop being load-bearing for cold-starting peers.

### `CatalogEvent` variants

`CatalogEvent` is `#[non_exhaustive]`. Multiple records can map to one event (e.g. both retention records → `*RetentionPeriodChanged`; both admin and resource-scoped token creation → `TokenCreated`). Because it is not persisted, the enum may add fields and variants freely. Keep in mind, the variant must derive its properties from the catalog record type persisted to object store.

---

## Persistence

### Object-store layout (`src/object_store/versions/v3.rs`)

```
{prefix}/catalog/v3/
  snapshot                       # latest snapshot (single file)
  logs/
    00000000000000000001.catalog # zero-padded 20-digit sequence
    00000000000000000002.catalog
    ...
```

v3 files live under a `v3/` prefix, separate from v2's path, which is what enables dual-path operation during migration. Log filenames are the catalog sequence zero-padded to 20 digits for lexicographic ordering.

### Log files and snapshots

Each mutation (one `update`/commit) produces one log file whose sequence is the catalog sequence after applying it. Snapshots contain all records up to the current sequence, in application order. Snapshots are **not** compacted — they retain the full record history. They are written periodically per a `CheckpointPolicy` (log-count and/or time interval). On startup the catalog loads the snapshot, then applies log files with later sequences.

### Optimistic concurrency

Multiple nodes can write concurrently. Each attempts a conditional write to `logs/{next_sequence}.catalog`; if it already exists the write fails with `AlreadyExists`, the node loads remote logs, and retries from prepare. The `write_permit` serializes this step within a node; the object store serializes across nodes. Persistence stays globally sequenced (single log sequence).

---

## Field families & limits

### Field families

A table's columns are tags, a timestamp, and **field families** — each family is a named group holding up to a fixed number of field columns. `FieldFamilyMode` (`src/catalog/versions/v3/schema/column.rs`) controls how incoming field names map to families:

- **`Aware`** — field names are qualified `family::field`. The qualifier selects/creates the family; the stored field name is the full string and the qualifier is used only for routing. Parsed by `parse_qualified_field_name` (`src/catalog/versions/v3/transaction.rs`), which splits on the first `::` and rejects empty halves.
- **`Auto`** — field names are unqualified and assigned to auto-numbered families, filling each before opening the next.

Both DDL table creation and line-protocol schema-on-write share the family-assignment logic in `TableTransaction`.

### Limits

Two kinds of limits apply.

**Pluggable resource limits** — database count, table count, and columns-per-table — go through the `CatalogLimiter` trait (`src/catalog/versions/v3/usage.rs`), chosen when the catalog is constructed. The current `CurrentCatalogUsage` (live counts) is passed in, so a limiter can vary its caps by usage. These limits are therefore enforced differently per edition and storage mode:

The catalog crate does not hardcode per-product limit values; each binary supplies its own via `CatalogLimits::new(..)`. `CatalogLimits::none()` returns effectively-unbounded limits for tests and internal paths (e.g. migration) that must not enforce them.

- **Core:** constructed by the Core binary (`oss/influxdb3`) from its own constants — `5` databases, `2000` tables, `500` columns per table. Not user-configurable.
- **Enterprise, Parquet:** seeded from CLI configuration — `enterprise_config.parquet.{num_database_limit, num_table_limit, num_total_columns_per_table_limit}` — whose defaults (`100` / `4000` / `500`) live in the Enterprise `clap_blocks` crate.
- **Enterprise, PachaTree:** `MaximumColumnCountLimiter`, a single **total-column budget** across the whole catalog (`enterprise_config.pacha_tree.max_columns`; default `Catalog::MAX_TOTAL_COLUMNS = 10_000_000`). Database and table caps are derived as the remaining headroom in the budget rather than fixed counts.

**Structural per-table limits** are fixed constants enforced directly in `TableTransaction` (not part of the pluggable limiter), with the timestamp column exempt from the column count:

| Constant (`src/catalog/versions/v3.rs`) | Value |
|---|---|
| `NUM_TAG_COLUMNS_LIMIT` | 4096 |
| `NUM_FIELDS_PER_FAMILY_LIMIT` | 100 |
| `NUM_FIELD_FAMILIES_LIMIT` | `FieldFamilyId::MAX` |

Violations surface as `CatalogError::TooMany*` (`TooManyDbs`, `TooManyTables`, `TooManyColumns`, `TooManyTagColumns`, `TooManyFields`, `TooManyFieldFamilies`).

---

## Forward compatibility

The mechanism prevents new record types from being written until all nodes can understand them.

### Feature level

A node's **feature level** is `FeatureLevel { core: u16, enterprise: u16 }` (`src/format/feature_level.rs`) — the highest sequential record ID in each partition known to that binary, computed at startup from the linked [`inventory`](https://docs.rs/inventory) registry by `derive_feature_level()`. Adding a new record type raises the level automatically; a release with no new records leaves it unchanged. `FeatureLevel` has a deliberately **partial** order so incomparable levels (e.g. core-ahead vs enterprise-ahead) are caught rather than silently ordered.

`RegisterNode` carries the node's feature level — every v3 node advertises its capability on registration, so there is no bootstrap problem.

### Committed feature level

The **committed feature level** is a cluster-wide value stored in `InnerCatalog`. Records with IDs above it (in their partition) are rejected on the write path unless `UPGRADE_SAFE`. It is raised by the `AdvanceFeatureLevel` record (core ID 1), whose `apply` sets `committed_feature_level`.

A freshly created or migrated catalog initializes its committed level to `derive_feature_level()` — the feature level of the binary that created it — so v3 is immediately active at that level. It is **not** `FeatureLevel::ZERO`. (`ZERO` appears only as a placeholder on the synthesized `RegisterNode` records for pre-v3 nodes carried over by migration, since v2 had no feature level; those nodes re-register at their real level when they come up on v3.)

### Write-path gate

Before persisting, `check_batch_against_committed()` (`catalog.rs`) checks every record: if its ID exceeds the committed level for its partition and it is not `UPGRADE_SAFE`, the write is rejected with `RecordExceedsCommittedFeatureLevel`. The check is O(1) per record (a comparison against the partition maximum). User-initiated operations surface this as an error indicating the cluster hasn't advanced; background processes can retry after advancement.

### UPGRADE_SAFE

A record may set `RecordFlags::UPGRADE_SAFE` only when (1) it adds state old code does not read, (2) it does not modify state old code depends on, and (3) an old node behaves identically without it. Such records are exempt from the gate and may be written during upgrade windows; a node that reads an unknown `UPGRADE_SAFE` record skips it (no apply, no event) but retains its opaque bytes so it can be re-emitted in the node's own snapshots. An unknown record **without** the flag is a hard error (`UnknownNonUpgradeSafeRecord`).

### Consensus and advancement (auto)

When a `RegisterNode` event changes the running set, the node evaluates `min(feature_level)` across running nodes (excluding the just-registering node's stale view, and excluding stopped nodes). If that minimum exceeds the committed level, the processing node writes `AdvanceFeatureLevel` to raise it. A node stuck `running` on an old version blocks advancement until the operator stops it.

### Startup fast-fail

On load, if the committed level is greater than (or incomparable with) the node's own derived level, the node fails immediately with `NodeBelowCommittedFeatureLevel` — before it would write `RegisterNode` or encounter unknown records deeper in the catalog.

---

## Upgrades

### v2 → v3 migration (`src/catalog/migrations/v3.rs`)

The first upgraded node freezes v2 and synthesizes the initial v3 state:

- **Freeze:** an `UpgradedLog` marker is written to the **v2** log folder (`object_store/versions/v2.rs`), after which no further v2 catalog changes are accepted by any node.
- **Synthesis:** the `FromV2` trait converts each v2 snapshot type (generation config, node, database, table, cache, token, trigger) into the equivalent v3 `CatalogRecord`s; `synthesize_records()` assembles them into the initial v3 snapshot. Synthesis is deterministic and idempotent — re-running from the same v2 snapshot yields the same output; the first conditional snapshot write wins and other nodes load from it.
- **Placeholders** (data v2 did not retain): `registered_time_ns = 0` for nodes that were stopped in v2; soft-delete timestamps recovered from the v2 name suffix where possible (else `0`); `process_uuid = [0u8; 16]` (audit-only field, not read by apply).

**Dual-path operation:** v3 logs go under `catalog/v3/logs/`, separate from v2. Non-upgraded nodes keep reading v2 state but, seeing the `UpgradedLog`, enter a semi-degraded mode: they serve reads and existing work but cannot make catalog changes — so **schema-on-write (new tables/columns) and DDL work only on upgraded nodes** during the window. Writes to existing tables with known columns continue everywhere.

**Constraints:** the upgrade is one-way (no v3→v2 revert). Recommended node order: ingest → query → compact, to minimize the schema-on-write gap. Once all nodes are on v3, v2 files remain in object store but are no longer read.

### v3 → v3 feature upgrades (rolling)

After migration, all changes use the feature-level mechanism above. Lifecycle, starting from all nodes and the committed level at `(N, M)`:

1. A release adds records with IDs above `N`/`M`; its binary's level is higher.
2. Operators roll-restart. Each upgraded node loads the catalog (all records ≤ committed, understood), registers at the higher level, and serves traffic. Features needing the new records are gated — the write-path gate returns an error until advancement.
3. During the window, old nodes operate normally and process the new nodes' `RegisterNode` records but don't trigger advancement (not all running nodes are at the new level). Pre-existing features work everywhere.
4. When the last old node upgrades and registers, consensus is reached and `AdvanceFeatureLevel` is written (auto). New records are now writable; the next snapshot captures them.

The window is bounded by the rolling-restart duration. Log files accumulate during it (see [Limitations](#limitations) re: snapshot suppression).

---

## Developer guide

### Adding a new record / feature

**1. Define the record struct and implement the traits** (`src/format/records/<resource>.rs`). Use the next sequential ID in the correct partition; feature-gated (`RecordFlags::none()`) unless it genuinely meets the `UPGRADE_SAFE` criteria.

```rust
#[derive(Debug, Clone, PartialEq, Eq, bitcode::Encode, bitcode::Decode)]
pub struct SetDatabaseDescription { pub db_id: u32, pub description: String }

impl CatalogRecord for SetDatabaseDescription {
    const ID: RecordId = record_ids::SET_DATABASE_DESCRIPTION; // next core seq
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "SetDatabaseDescription";
    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> { /* mutate */ Ok(()) }
    fn event(&self) -> CatalogEvent { CatalogEvent::DatabaseUpdated { db_id: DbId::new(self.db_id) } }
}
impl_bitcode_encoding!(SetDatabaseDescription);
```

**2. Register it** so the registry (and the derived feature level) pick it up:

```rust
inventory::submit! { RegisteredRecord::new::<SetDatabaseDescription>() }
```

**3. Add the `CatalogEvent` variant** (if new). Subscribers in other crates need a wildcard arm; the enum is `#[non_exhaustive]`.

In the above example, we might add a `CatalogEvent::DatabaseUpdated{ db_id: u32 }` variant, from which event subscribers will use to lookup the modified database with the new description.

**4. Wire the producer.** Either a `CatalogOp` (`src/catalog/versions/v3/ops/<resource>.rs`) whose `prepare` validates and pushes the record and whose `output` returns the affected resource, or the transaction path (for tables/columns). Expose a method on `Catalog`. The above example could potentially be added into the existing `CatalogOp` implementation for the `CreateDatabaseOp` by extending its `Input` type and consuming APIs to allow for an optional description field. The new record would be appended to the `RecordBatch` in the `CatalogOp::prepare` implementation for that type.

**5. Frozen-record discipline.** To add a field to an existing resource, **_DO NOT_** modify the shipped record; define a new record (e.g. `SetDatabaseDescription` rather than changing `CreateDatabase`) and give the runtime types a defaulted field. Existing records are immutable.

**6. Feature-gating on the write path.** A new record is rejected by the gate until the committed level includes it. If a write path would otherwise always emit a new record (and thus reject writes old nodes accept during the window), guard its inclusion on `committed_feature_level()` so new nodes fall back to the old record set until advancement.

### Startup writes (UPGRADE_SAFE)

Some records must be written by a node **at startup, before it accepts traffic** — e.g. a per-node digest or capability the node has to persist before serving. During a rolling upgrade this collides with feature gating: older nodes are still running, so the committed feature level has not advanced to include a record introduced by the new release, and the [write-path gate](#write-path-gate) would reject the write — blocking startup.

`UPGRADE_SAFE` exists for this scenario. The only declaration difference from an ordinary record is the flag:

```rust
impl CatalogRecord for SetNodeLicenseDigest {
    const ID: RecordId = record_ids::SET_NODE_LICENSE_DIGEST;
    const FLAGS: RecordFlags = RecordFlags::upgrade_safe();   // gate-exempt
    const NAME: &'static str = "SetNodeLicenseDigest";
    // apply / event as usual
}
```

`check_batch_against_committed` skips any record whose `flags().is_upgrade_safe()` is set, so the write persists regardless of the committed level.

Use this only when the record meets all three [UPGRADE_SAFE criteria](#upgrade_safe): it adds state old code does not read, does not modify state old code depends on, and an old node behaves identically without it. An old node that does not recognize the ID skips the record (no apply, no event) but retains its bytes so its own snapshots stay complete. If those criteria do not hold, the record must remain feature-gated and the feature waits for [feature-level advancement](#consensus-and-advancement-auto).

### Conventions

- **_NEVER_** modify or reorder a shipped record or its ID; only append. Deprecate by removing the producer code while keeping the struct, its `Encode`/`Decode`, and its `inventory::submit!`.
- Events carry only IDs; subscribers look up state and stay idempotent.

---

## Limitations

- **Feature-level advancement is automatic only.** Advancement is consensus-driven and happens as soon as all running nodes reach a higher level. There is no manual mode — no operator soak/commit step and no explicit downgrade window to roll back before new records are written.
- **Snapshots are not suppressed during upgrades.** Checkpointing is unconditional (driven by `CheckpointPolicy`); a trailing node at a lower feature level is not prevented from writing snapshots.
- **Single lock, sequential apply.** `InnerCatalog` is behind one `RwLock` and `apply_records` processes records sequentially. There is no per-collection locking or parallel database-scoped apply.
- **No event de-duplication on runtime snapshot load.** The broadcast path does not skip events for state a subscriber already holds; subscribers rely on idempotent re-application.
- **Restore feature-gates on the cluster's committed level.** `RestoreCatalog` is a normal feature-gated record (not `UPGRADE_SAFE`), so it cannot be written until the cluster's committed feature level has advanced to include it. Operators rolling out the release that introduces restore must wait for [feature-level advancement](#consensus-and-advancement-auto) before issuing a restore.
- **Restore extends the write-permit critical section by one preload.** `RestoreOp`'s batch preloads the backup snapshot + log files (async object-store I/O) under the `write_permit` before the apply. Other writers serialize behind the restore for the duration of that load — acceptable because restore is a serializing operator action, not a hot path.
- **Write-path contention.** Persistence is a single globally sequenced log, so a high schema-mutation rate can cause optimistic-concurrency retries.
- **No log compaction or true deletion.** Snapshots retain full record history, so snapshot size grows with mutation count rather than live-entity count, and metadata (e.g. table and column names) persists indefinitely — relevant to data-privacy removal requirements. True deletion remains feasible later, once per-resource delete semantics are defined.
- **Inconsistent HTTP status mapping** for some catalog errors (e.g. `TooManyFieldFamilies` → 500 while sibling `TooMany*` → 422; a missing database on token-create surfacing as 500). See [#3768][i3768].
- **Some limits are fixed constants.** Database, table, and columns-per-table limits are configurable in Enterprise (via the `CatalogLimiter`), but the structural per-table limits — tag columns, fields per family, field families per table — are compile-time constants.

---

## References & tracked work

- Original design (rationale, motivation, alternatives): [Catalog v3 Design][orig-design].
- **[#3768][i3768]** — follow-ups from the v3-flip PR review (hard-delete token-name capture, HTTP status normalization, cosmetics).
- **[#3757][i3757]** — v3 restore coordination (`RestoreCatalog` record + `CatalogRestored` event + `Catalog::restore`); landed alongside this README revision.
- **[#3600][i3600]** — v2 restore-coordination (`CatalogBatch::Restore`), the source mechanism ported to v3.

Open future-work themes: write-path concurrency (per-database log files, batched commit windows, or a coordinator), log compaction and true deletion, a binary-format dump CLI for debugging, and extracting enterprise catalog code into its own crate.

[i3768]: https://github.com/influxdata/influxdb_pro/issues/3768
[i3757]: https://github.com/influxdata/influxdb_pro/issues/3757
[i3600]: https://github.com/influxdata/influxdb_pro/issues/3600
