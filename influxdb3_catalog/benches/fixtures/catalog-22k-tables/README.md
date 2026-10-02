# `catalog-22k-tables`

A real persisted v3 catalog, used by `benches/catalog_load.rs` and
`examples/catalog_load_profile.rs`. It is checked in rather than generated
because cold-start cost is dominated by the *shape* of a real catalog — the
record mix and the table/column counts — which is easy to get subtly wrong when
fabricating one.

## Contents

```
snapshot                            3,532,881 bytes, sequence 10, 44,011 records
logs/00000000000000000011.catalog   \
...                                  |  25 files, 7,891 bytes, 25 records
logs/00000000000000000035.catalog   /
```

Loading it yields **2 databases and 22,000 tables**. The snapshot's records are
almost entirely table schema: 22,000 `CreateTable` and 22,000 `AddColumns`, plus
a handful of `RegisterNode` / `StopNode` / `CreateDatabase` / `SetStorageMode`.

The layout mirrors what `CatalogFilePath` expects under a catalog prefix, so the
directory can be staged into an object store as `{prefix}/catalog/v3/`.

## Why the log files start at 11

`load_catalog` reads the snapshot and then lists logs *strictly after* the
snapshot's sequence number, so logs 1–10 are already folded into the snapshot
and are never opened. They were dropped when this fixture was captured — log 5
alone was 3.5 MB, which would have doubled the fixture for bytes no benchmark
reads.

Consequently this fixture exercises log replay only lightly (25 small records).
It is a snapshot-load benchmark; a fixture with a long post-snapshot log chain
would be a separate capture.

## Capturing another one

Copy `{prefix}/catalog/v3/` out of an object store, keeping the `snapshot` file
and the `logs/` directory, then point the benchmark at it:

```console
INFLUXDB3_CATALOG_BENCH_DIR=/path/to/catalog/v3 \
    cargo bench -p influxdb3_catalog --bench catalog_load
```

Catalog files are versioned binary blobs with CRC-checked headers; they are not
editable by hand. If the v3 on-disk format ever gains a breaking change, this
fixture must be recaptured rather than migrated in place.
