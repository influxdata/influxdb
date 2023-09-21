# Job of Garbage Collector

The `garbage_collector` service is responsible for cleaning up the
`parquet_file` table in the catalog, as well as eventually removing
old uneeded parquet files in the object store.

## Background
As IOx ingests data, it is stored as parquet files on object
store. Each parquet file created is recorded in a row of the
`parquet_file` table in the catalog.

Over time, the [compactor](compactor.md) creates new, more optimized
parquet files by combining several pre-existing files. When a new file
is successfully created on object_store, a new row is added to
`parquet_file` and the previous files with the same data are "soft
deleted" by setting the value of the `parquet_file.to_delete` to the
current time.

Without the garbage collector, the size of the `parquet_file` table
and the number of files in object store would grow without bound.

# Interaction with Querier

The Querier caches entries from the `parquet_file` table to answer
queries and periodically refreshes this cache. This cache means that
even after a row in `parquet_file` has been marked as `to_delete` or
actually deleted, the querier may still attempt to read the underlying
parquet file from object store until its cache is refreshed.

# Configuration

There are two key configuration knobs that control the behavior of the
garbage collector:

* `INFLUXDB_IOX_GC_PARQUETFILE_CUTOFF`: this setting controls when
  rows are deleted from the `parquet_file` table. Any row that has a
  value of `to_delete` that is greater than this setting will be
  deleted from the catalog.

* `INFLUXDB_IOX_GC_OBJECTSTORE_CUTOFF`: this setting controls when
  objects are actually deleted from the object store. Any object that
  was created (according to the object store timestamp) longer than
  this interval ago and is not referenced in the catalog's `parquet_file` table
  will be deleted.

# Frequently Asked Questions

Q: Why do we need two cutoffs?

A: The querier relies on objects not being deleted until its caches
are refreshed. For example, if `INFLUXDB_IOX_GC_OBJECTSTORE_CUTOFF` is
set to `90 days` but a parquet file is a year old, as soon as the row
is removed from `parquet_file` the object may be deleted from the
object store, even though it is still referred to in the Querier
cache. Thus `INFLUXDB_IOX_GC_PARQUETFILE_CUTOFF` must be set
sufficiently high to ensure the querier cache is refreshed before
objects are candidates for deletion.

Q: Why not delete objects immediately when
`INFLUXDB_IOX_GC_PARQUETFILE_CUTOFF` expires?

A: Database backups contain references to parquet files. In order to
ensure all files referred to by these backups are not deleted,
`INFLUXDB_IOX_GC_OBJECTSTORE_CUTOFF` can be set sufficiently high.
