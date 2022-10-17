# Job of a Compactor

Compactor is one of the servers in an IOx cluster and its main job is to compact many small and time-overlaped files into larger and non-time-overlapped files. Duplicated and soft deleted data will also be removed during compaction. There may be one or many Compactors in a cluster, each will be responsible for compacting files of a set of specififed shards. 

- The purpose of compaction to increase query performance by
   1. Avoiding reading too many small files
   2. Avoiding doing deduplication and tombstone application during query time

- The existence of compaction will enable
   1. The Ingesters to ingest as many small files as they need to reduce TTBR (time to be ready)
   2. The Querier to run queries faster while consuming less memory

There are 3 kinds of `compaction_level` files in IOx: level-0, level-1 and level-2.
   - Level-0 files are small files ingested by the Ingesters
   - Level-1 files are files created by a Compactor as a result of compacting one or many level-0 files with their overlapped level-1 files.
   - Level-2 files are files created by a Compactor as a result of compacting one or many level-1 files with their overlapped level-2 files.
Regarless of level, a file in IOx must belong to a partition which represents data of a time range which is usually a day. Two files of different partitions never overlap in time and hence the Compactor only needs to compact files that belong to the same partition.

A level-0 file may overlap with other level-0, level-1 and level-2 files. A Level-1 file do not overlap with any other level-1 files but may overlap with level-2 files. A level-2 file does not overlap with any other level-2 files.

Compaction process in IOx is a background task that repeats selecting and compacting partitions with new files. Each repeat is called a `compaction cycle` that in turn includes N `hot cycles` and one `cold cycle`. N is configurable and the default is 4. 

- A **Hot cycle** is responsible for compacting level-0 files and their overlapped level-1 files into non-overlapped level-1 files. Only partitions with at least two level-0 files that are ingested within 24 hours hours are selected to compact. In order to deduplicate data correctly, level-0 files with smaller sequence numbers are always compacted before the ones with larger sequence numbers. The compacted output will go to one or many non-time-overlapped level-1 each is estimated around the size of a configurable max-desired-file-size.
- A **cold cycle** is responsible for compacting partitions that have no new level-0 files ingested the last 8 hours. A partition selected in cold cycle will go through two steps of compaction:
    - Step 1: similar to hot cycle, level-0 files will be compacted with their overlapped level-1 files to produce non-overlapped level-1 files.
    - Step 2: level-1 files will be compacted with their overlapped level-2 files to produce non-overlapped level-2 files. Max size of level-2 files is also configurable.

The number of files in a partition selected to compact depends on the available memory. The number of partitions to be compacted concurrently in each cycle also depends on the compactor memory and is different in each cycle depending on the number of partitions needed to compact and the number and sizes of their compacting files. See below for how to estimate memory needed for each partition.

In the case the Compactor cannot compact the smallest set of files of a partition due to memory limits, that partition will be put into a `skipped_compactions` table and won't be considered for compaction again. A partition that is not compacted will provide bad query performance or even cannot be queried if the Querier does not have enough memory to read and process their files. See below for the details of when this can happen, how to avoid it, and how to solve if that happens.

# What to do if a Compactor hits OOMs

Even though we have tried to avoid OOMs by estimating needed memory, it still happens in extreme cases. Currently, OOMs in compactor won't be resolved by themselves without human actions becasue the compactor will likely chooose the same heavy partitions to conpact after it is restarted. The easiest way to stop OOMs is to increase memory. Doubling memory or even more is recommended. We can bring the memory down after all the high volumn partitions are compacted.

If increasing memory a lot does not help, consider changing one or a combination of config parameters below but please be aware that by chooing doing this, you tell the compactor that it has a lot less memory budget to use and it will push one or many partitions into the `skipped_compactions`. Moreover, reducing memory budget also means reducing the concurrency capacity of the compactor. It is recommended that you do not try this unless you know the workload of the compactor very well.
- INFLUXDB_IOX_COMPACTION_MIN_ROWS_PER_RECORD_BATCH_TO_PLAN: double or triple this value. This will tell the compactor the compaction plans need more memory, and will reduce the number of files that can be compacted at once
- INFLUXDB_IOX_COMPACTION_MAX_COMPACTING_FILES: reduce this value in half or more. This puts a hard cap on the maximun number of files of a partition it can compact, even if its memory budget estimate would allow for more.
- INFLUXDB_IOX_COMPACTION_MEMORY_BUDGET_BYTES: reduce this value in half or more. This tells the compact its total budget is less so it will reduce the number of partition it can compact concurrenly or reduce number of files to be compacted for a partition.

# Avoid and deal with files in skipped_compactions
todo

# Memory Estimation
todo

# Usual SQL to verify compaction status
todo

