/*

Package tsi1 provides a memory-mapped index implementation that supports
high cardinality series.

Overview

The top-level object in tsi1 is the Index. It is the primary access point from
the rest of the system. The Index is composed of LogFile and IndexFile objects.

Log files are small write-ahead log files that record new series immediately
in the order that they are received. The data within the file is indexed
in-memory so it can be quickly accessed. When the system is restarted, this log
file is replayed and the in-memory representation is rebuilt.

Index files also contain series information, however, they are highly indexed
so that reads can be performed quickly. Index files are built through a process
called compaction where a log file or multiple index files are merged together.


Operations

The index can perform many tasks related to series, measurement, & tag data.
All data is inserted by adding a series to the index. When adding a series,
the measurement, tag keys, and tag values are all extracted and indexed
separately.

Once a series has been added, it can be removed in several ways. First, the
individual series can be removed. Second, it can be removed as part of a bulk
operation by deleting the entire measurement.

The query engine needs to be able to look up series in a variety of ways such
as by measurement name, by tag value, or by using regular expressions. The
index provides an API to iterate over subsets of series and perform set
operations such as unions and intersections.


Log File Layout

The write-ahead file that series initially are inserted into simply appends
all new operations sequentially. It is simply composed of a series of log
entries. An entry contains a flag to specify the operation type, the measurement
name, the tag set, and a checksum.

	┏━━━━━━━━━LogEntry━━━━━━━━━┓
	┃ ┌──────────────────────┐ ┃
	┃ │         Flag         │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │     Measurement      │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │      Key/Value       │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │      Key/Value       │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │      Key/Value       │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │       Checksum       │ ┃
	┃ └──────────────────────┘ ┃
	┗━━━━━━━━━━━━━━━━━━━━━━━━━━┛

When the log file is replayed, if the checksum is incorrect or the entry is
incomplete (because of a partially failed write) then the log is truncated.


Index File Layout

The index file is composed of 3 main block types: one series block, one or more
tag blocks, and one measurement block. At the end of the index file is a
trailer that records metadata such as the offsets to these blocks.


Series Block Layout

The series block stores raw series keys in sorted order. It also provides hash
indexes so that series can be looked up quickly. Hash indexes are inserted
periodically so that memory size is limited at write time. Once all the series
and hash indexes have been written then a list of index entries are written
so that hash indexes can be looked up via binary search.

The end of the block contains two HyperLogLog++ sketches which track the
estimated number of created series and deleted series. After the sketches is
a trailer which contains metadata about the block.

	┏━━━━━━━SeriesBlock━━━━━━━━┓
	┃ ┌──────────────────────┐ ┃
	┃ │      Series Key      │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │      Series Key      │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │      Series Key      │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │                      │ ┃
	┃ │      Hash Index      │ ┃
	┃ │                      │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │      Series Key      │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │      Series Key      │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │      Series Key      │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │                      │ ┃
	┃ │      Hash Index      │ ┃
	┃ │                      │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │    Index Entries     │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │     HLL Sketches     │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │       Trailer        │ ┃
	┃ └──────────────────────┘ ┃
	┗━━━━━━━━━━━━━━━━━━━━━━━━━━┛


Tag Block Layout

After the series block is one or more tag blocks. One of these blocks exists
for every measurement in the index file. The block is structured as a sorted
list of values for each key and then a sorted list of keys. Each of these lists
has their own hash index for fast direct lookups.

	┏━━━━━━━━Tag Block━━━━━━━━━┓
	┃ ┌──────────────────────┐ ┃
	┃ │        Value         │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │        Value         │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │        Value         │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │                      │ ┃
	┃ │      Hash Index      │ ┃
	┃ │                      │ ┃
	┃ └──────────────────────┘ ┃
	┃ ┌──────────────────────┐ ┃
	┃ │        Value         │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │        Value         │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │                      │ ┃
	┃ │      Hash Index      │ ┃
	┃ │                      │ ┃
	┃ └──────────────────────┘ ┃
	┃ ┌──────────────────────┐ ┃
	┃ │         Key          │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │         Key          │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │                      │ ┃
	┃ │      Hash Index      │ ┃
	┃ │                      │ ┃
	┃ └──────────────────────┘ ┃
	┃ ┌──────────────────────┐ ┃
	┃ │       Trailer        │ ┃
	┃ └──────────────────────┘ ┃
	┗━━━━━━━━━━━━━━━━━━━━━━━━━━┛

Each entry for values contains a sorted list of offsets for series keys that use
that value. Series iterators can be built around a single tag key value or
multiple iterators can be merged with set operators such as union or
intersection.


Measurement block

The measurement block stores a sorted list of measurements, their associated
series offsets, and the offset to their tag block. This allows all series for
a measurement to be traversed quickly and it allows fast direct lookups of
measurements and their tags.

This block also contains HyperLogLog++ sketches for new and deleted
measurements.

	┏━━━━Measurement Block━━━━━┓
	┃ ┌──────────────────────┐ ┃
	┃ │     Measurement      │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │     Measurement      │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │     Measurement      │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │                      │ ┃
	┃ │      Hash Index      │ ┃
	┃ │                      │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │     HLL Sketches     │ ┃
	┃ ├──────────────────────┤ ┃
	┃ │       Trailer        │ ┃
	┃ └──────────────────────┘ ┃
	┗━━━━━━━━━━━━━━━━━━━━━━━━━━┛


Manifest file

The index is simply an ordered set of log and index files. These files can be
merged together or rewritten but their order must always be the same. This is
because series, measurements, & tags can be marked as deleted (aka tombstoned)
and this action needs to be tracked in time order.

Whenever the set of active files is changed, a manifest file is written to
track the set. The manifest specifies the ordering of files and, on startup,
all files not in the manifest are removed from the index directory.


Compacting index files

Compaction is the process of taking files and merging them together into a
single file. There are two stages of compaction within TSI.

First, once log files exceed a size threshold then they are compacted into an
index file. This threshold is relatively small because log files must maintain
their index in the heap which TSI tries to avoid. Small log files are also very
quick to convert into an index file so this is done aggressively.

Second, once a contiguous set of index files exceed a factor (e.g. 10x) then
they are all merged together into a single index file and the old files are
discarded. Because all blocks are written in sorted order, the new index file
can be streamed and minimize memory use.


Concurrency

Index files are immutable so they do not require fine grained locks, however,
compactions require that we track which files are in use so they are not
discarded too soon. This is done by using reference counting with file sets.

A file set is simply an ordered list of index files. When the current file set
is obtained from the index, a counter is incremented to track its usage. Once
the user is done with the file set, it is released and the counter is
decremented. A file cannot be removed from the file system until this counter
returns to zero.

Besides the reference counting, there are no other locking mechanisms when
reading or writing index files. Log files, however, do require a lock whenever
they are accessed. This is another reason to minimize log file size.


*/
package tsi1
