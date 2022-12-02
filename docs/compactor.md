# Job of a Compactor

Compactor is one of the servers in an IOx cluster and its main job is to compact many small and time-overlapped files into larger and non-time-overlapped files. Duplicated and soft deleted data will also be removed during compaction. There may be one or many Compactors in a cluster, each will be responsible for compacting files of a set of specified shards.

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

Regardless of level, a file in IOx must belong to a partition which represents data of a time range which is usually a day. Two files of different partitions never overlap in time and hence the Compactor only needs to compact files that belong to the same partition.

A level-0 file may overlap with other level-0, level-1 and level-2 files. A Level-1 file does not overlap with any other level-1 files but may overlap with level-2 files. A level-2 file does not overlap with any other level-2 files.

Compaction process in IOx is a background task that repeats selecting and compacting partitions with new files. Each repeat is called a `compaction cycle` that in turn includes N `hot cycles` and one `cold cycle`. N is configurable and the default is 4.

- A **Hot cycle** is responsible for compacting level-0 files and their overlapped level-1 files into non-overlapped level-1 files. Only partitions with at least two level-0 files that are ingested within 24 hours are selected to compact. In order to deduplicate data correctly, level-0 files with smaller sequence numbers are always compacted before the ones with larger sequence numbers. The compacted output will go to one or many non-time-overlapped level-1 files, each estimated to be around the size of a configurable max-desired-file-size.
- A **cold cycle** is responsible for compacting partitions that have no new level-0 files ingested in the last 8 hours. A partition selected in cold cycle will go through two steps of compaction:
    - Step 1: similar to hot cycle, level-0 files will be compacted with their overlapped level-1 files to produce non-overlapped level-1 files.
    - Step 2: level-1 files will be compacted with their overlapped level-2 files to produce non-overlapped level-2 files. Max size of level-2 files is also configurable.

The number of files in a partition selected to compact depends on the available memory. The number of partitions to be compacted concurrently in each cycle also depends on the compactor memory and is different in each cycle depending on the number of partitions needed to compact and the number and sizes of their compacting files. See below for how to estimate memory needed for each partition.

In the case the Compactor cannot compact the smallest set of files of a partition due to memory limits, that partition will be put into a `skipped_compactions` table and won't be considered for compaction again. A partition that is not compacted will provide bad query performance or even cannot be queried if the Querier does not have enough memory to read and process its files. See below for the details of when this can happen, how to avoid it, and how to solve if it happens.

# What to do if a Compactor hits OOMs

Even though we have tried to avoid OOMs by estimating needed memory, it still happens in extreme cases. Currently, OOMs in compactor won't be resolved by themselves without human actions because the compactor will likely choose the same heavy partitions to compact after it is restarted. The easiest way to stop OOMs is to increase memory. Doubling memory or even more is recommended. We can bring the memory down after all the high volume partitions are compacted.

If increasing memory a lot does not help, consider changing one or a combination of config parameters below but please be aware that by choosing to do this, you may be telling the compactor that it has a lot less memory budget to use and it will push one or many partitions into the `skipped_compactions` list. Moreover, reducing memory budget also means reducing the concurrency capacity of the compactor. It is recommended that you do not try this unless you know the workload of the compactor very well.
- `INFLUXDB_IOX_COMPACTION_MAX_PARALLEL_PARTITIONS`: Reduce this value in half or more. This will put a hard cap on the maximun number of partitions to be compacted in parallel. This will help reduce both CPU and memory usage a lot and should be your first choice to adjust.
- `INFLUXDB_IOX_COMPACTION_MIN_ROWS_PER_RECORD_BATCH_TO_PLAN`: double or triple this value. This will tell the compactor the compaction plans need more memory, and will reduce the number of files that can be compacted at once
- `INFLUXDB_IOX_COMPACTION_MEMORY_BUDGET_BYTES`: reduce this value in half or more. This tells the compactor its total budget is less so it will reduce the number of partitions it can compact concurrently or reduce the number of files to be compacted for a partition.
- `INFLUXDB_IOX_COMPACTION_MAX_COMPACTING_FILES`: reduce this value in half or more. This puts a hard cap on the maximum number of files of a partition it can compact, even if its memory budget estimate would allow for more.
- `INFLUXDB_IOX_COMPACTION_MAX_COMPACTING_FILES_FIRST_IN_PARTITION`: This should be the last choice to adjust. Reduce this value in half or more but this action only helps if the number of L1s that overlap with the first L0 are large. This is similar to `INFLUXDB_IOX_COMPACTION_MAX_COMPACTING_FILES` but on the set of files that the compactor must compact first for a partition. If the number of files in this set is larger than the settings, the compactor will ignore compacting this partition and put it in `skipped_comapctions` catalog table.

# Compactor Config Parameters

These are [up-to-date configurable parameters](https://github.com/influxdata/influxdb_iox/blob/main/clap_blocks/src/compactor.rs). Here are a few key parameters you may want to tune for your needs:

 - **Size of the files:** The compactor cannot control the sizes of level-0 files but they are usually small and can be adjusted by config params of the Ingesters. The compactor decides the max desired size of level-1 and level-2 files which is around `INFLUXDB_IOX_COMPACTION_MAX_DESIRED_FILE_SIZE_BYTES * (100 + INFLUXDB_IOX_COMPACTION_PERCENTAGE_MAX_FILE_SIZE) / 100`.
 - **Map a compactor to several shards:**  Depending on your Ingester setup, there may be several shards. A compactor can be set up to compact all or a fraction of the shards. Use range `[INFLUXDB_IOX_SHARD_INDEX_RANGE_START, INFLUXDB_IOX_SHARD_INDEX_RANGE_END]` to map them.
- **Number of partitions considered to compact per shard:** If there is enough memory, which is usually the case, the compactor will compact many partitions of the same or different shards concurrently. Depending on how many shards a compactor handles and how much memory that compactor is configured to use, you can increase/reduce the concurrent compaction level by increasing/reducing the number of partitions per shard by adjusting `INFLUXDB_IOX_COMPACTION_MAX_NUMBER_PARTITIONS_PER_SHARD`.
- **Concurrency capacity:** to configure this based on your available memory, you need to understand how IOx estimates memory to compact files in the next section.

# Memory Estimation

The idea of a single compaction is to compact as many small input files as possible into one or few larger output files as follows:

```text
     ┌────────┐         ┌────────┐     
     │        │         │        │     
     │ Output │  .....  │ Output │     
     │ File 1 │         │ File m │     
     │        │         │        │     
     └────────┘         └────────┘     
          ▲                  ▲         
          │                  │         
          │                  │         
      .─────────────────────────.      
 _.──'                           `───. 
(               Compact               )
 `────.                         _.───' 
       `───────────────────────'       
       ▲        ▲              ▲       
       │        │              │       
       │        │              │       
   ┌───────┐┌──────┐       ┌──────┐    
   │       ││      │       │      │    
   │ Input ││Input │       │Input │    
   │File 1 ││File 2│ ..... │File n│    
   │       ││      │       │      │    
   └───────┘└──────┘       └──────┘    
Figure 1: Compact a Partition
```


Currently, in order to avoid over committing memory (and OOMing), the compactor computes an estimate of the memory needed for loading full input files into memory, memory for streaming input files in parallel, and memory for output streams. The boxes in the diagram below illustrate the memory needed to run the query plan above. IOx picks the number of input files to compact based on their sizes, number of columns and columns types of the file's table, max desired output files, and the memory budget the compactor is provided. Details:

```text
          ┌───────────┐          ┌────────────┐     
          │Memory for │          │ Memory for │     
          │  Output   │  .....   │   Output   │     
          │ Stream 1  │          │  Stream m  │     
          │           │          │            │     
          └───────────┘          └────────────┘     
                                                    
               ▲                        ▲           
               │                        │           
               │                        │           
               │                        │           
                 .─────────────────────.            
          _.────'                       `─────.     
       ,─'                                     '─.  
      ╱            Run Compaction Plan            ╲ 
     (                                             )
      `.                                         ,' 
        '─.                                   ,─'   
           `─────.                     _.────'      
                  `───────────────────'             
       ▲              ▲                      ▲      
       │              │                      │      
       │              │                      │      
       │              │                      │      
┌────────────┐ ┌────────────┐         ┌────────────┐
│ Memory for │ │ Memory for │         │ Memory for │
│ Streaming  │ │ Streaming  │         │ Streaming  │
│   File 1   │ │   File 2   │  .....  │   File n   │
│            │ │            │         │            │
└────────────┘ └────────────┘         └────────────┘
┌────────────┐ ┌────────────┐         ┌────────────┐
│ Memory for │ │ Memory for │         │ Memory for │
│Loading Full│ │Loading Full│  .....  │Loading Full│
│   File 1   │ │   File 2   │         │   File n   │
│            │ │            │         │            │
└────────────┘ └────────────┘         └────────────┘
Figure 2:  Memory Estimation for a Compaction Plan
```

- Memory for loading a full file: Twice the file size.
- Memory for streaming an input file: See [estimate_arrow_bytes_for_file](https://github.com/influxdata/influxdb_iox/blob/bb7df22aa1783e040ea165153876f1fe36838d4e/compactor/src/compact.rs#L504) for the details but, briefly, each column of the file will need `size_of_a_row_of_a_column * INFLUXDB_IOX_COMPACTION_MIN_ROWS_PER_RECORD_BATCH_TO_PLAN` in which `size_of_a_row_of_a_column` depends on column type and the file cardinality.
- Memory for an output stream is similar to memory for streaming an input file. Number of output streams is estimated based on the sizes of the input files and max desired file size `INFLUXDB_IOX_COMPACTION_MAX_DESIRED_FILE_SIZE_BYTES`.

IOx limits the number of input files using the budget provided in `INFLUXDB_IOX_COMPACTION_MEMORY_BUDGET_BYTES`. However, IOx also caps the number of input files based on `INFLUXDB_IOX_COMPACTION_MAX_COMPACTING_FILES` even if more could fit under the memory budget.

Since the compactor keeps running to look for new files to compact, the number of input files are usually small (fewer then 10) and, thus, memory needed to run such a plan is usually small enough for a compactor to be able to run many of them concurrently.

**Best Practice Recommendation for your configuration:**

- `INFLUXDB_IOX_COMPACTION_MEMORY_BUDGET_BYTES`: 1/3 (or at most 1/2) your total actual memory.
- `INFLUXDB_IOX_COMPACTION_MAX_COMPACTING_FILES`: 20
- `INFLUXDB_IOX_COMPACTION_MIN_ROWS_PER_RECORD_BATCH_TO_PLAN`: 32 * 1024
- `INFLUXDB_IOX_COMPACTION_MAX_DESIRED_FILE_SIZE_BYTES`: 100 * 1024 * 1024
- `INFLUXDB_IOX_COMPACTION_PERCENTAGE_MAX_FILE_SIZE`: 5

# Avoid and deal with partitions in `skipped_compactions`

To deduplicate data correctly, the Compactor must compact level-0 files in ascending order of their sequence numbers and with their overlapped level-1 files. If the first level-0 and its overlapped level-1 files are too large and their memory estimation in Figure 2 is over the budget defined in `INFLUXDB_IOX_COMPACTION_MEMORY_BUDGET_BYTES`, the compactor won't be able to compact that partition. To avoid considering that same partition again and again, the compactor will put that partition into the catalog table `skipped_compactions`.

If you find your queries on data of a partition that is in `skipped_compactions` are slow, we may want to force the Compactor to compact that partition by increasing your memory budget and then removing that partition from the `skipped_compactions` table. If you remove the partition without adjusting your config params, the Compactor will skip it again without compacting it.

To list the records in the `skipped_compactions` table, get the gRPC address of a compactor and use this command:

```
$ influxdb_iox debug skipped-compactions list -h <compactor gRPC address>
```

and you'll see a list of the records similar to this:

```
+--------------+--------------------+-------------------------------------+-----------------+-------------+-----------+-----------------+
| partition_id | reason             | skipped_at                          | estimated_bytes | limit_bytes | num_files | limit_num_files |
+--------------+--------------------+-------------------------------------+-----------------+-------------+-----------+-----------------+
| 51           | over memory budget | 2023-01-01T00:00:00.000000003+00:00 | 10000           | 500         | 10        | 8               |
+--------------+--------------------+-------------------------------------+-----------------+-------------+-----------+-----------------+
```

To remove a partition from the `skipped_compactions` table to attempt compaction on that partition again, use this command with the gRPC address of a compactor and the `partition_id` of the partition whose record you'd like to delete:

```
$ influxdb_iox debug skipped-compactions delete -h <compactor gRPC address> <partition ID>
```

If the record was removed successfully, this command will print it out.

Increasing the memory budget can be as simple as increasing your actual memory size and then increasing the value of `INFLUXDB_IOX_COMPACTION_MEMORY_BUDGET_BYTES` accordingly. You can also try to reduce the value of `INFLUXDB_IOX_COMPACTION_MIN_ROWS_PER_RECORD_BATCH_TO_PLAN` to at minimum 8 * 1024 but you may hit OOMs if you do so. This depends on the column types and number of columns of your files.

If your partition is put into the `skipped_compactions` table with the reason `over limit of num files`, you have to increase `INFLUXDB_IOX_COMPACTION_MAX_COMPACTING_FILES` accordingly but you may hit OOMs if you do not increase your actual memory.

# Avoid Deduplication in Querier

Deduplication is known to be expensive. To avoid deduplication work during query time in Queriers, your files should not be overlapped in time range. This can be achieved by having all files of a partition in either level-1 or level-2. With the current design, if your compactor catches up well, partitions with recent level-0 files within the last 4 hours should have at most two level-2 files. Partitions without new level-0 files in the last 8 hours should have all level-2 files. Depending on the performance in the Querier, we can adjust the Compactor (a future feature) to have all files in level-1 or level-2.

# Common SQL to verify compaction status

If your Compactors catch up well with your Ingesters and do not hit memory issues, you should see:

1. Table `skipped_compactions` is empty
2. Most partitions have at most 2 level-0 files. If a partition has a lot of level-0 files, it signals either your compactor is behind and does not compact it, or that partition is put in `skipped_compactions`.
3. Most partitions without new level-0 files in the last 8 hours should have all level-2 files.
4. Most non-used files (files with `to_delete is not null`) are removed by garbage collector

Here are SQL to verify them:

```sql
-- Content of skipped_compactions
SELECT * FROM skipped_compactions;

-- remove partitions from the skipped_compactions
DELETE FROM skipped_compactions WHERE partition_id in ([your_ids]);

-- Content of skipped_compactions with their shard index, partition key and table id
SELECT shard_index, table_id, partition_id, partition_key, left(reason, 25),
   num_files, limit_num_files, estimated_bytes, limit_bytes, to_timestamp(skipped_at) skipped_at
FROM skipped_compactions, partition, shard
WHERE partition.id = skipped_compactions.partition_id and partition.shard_id = shard.id
ORDER BY shard_index, table_id, partition_key, skipped_at;

-- Number of files per level for top 50 partitions with most files of a specified day
SELECT s.shard_index, pf.table_id, pf.partition_id, p.partition_key,
   count(case when pf.to_delete is null then 1 end) total_not_deleted,
   count(case when pf.compaction_level=0 and pf.to_delete is null then 1 end) num_l0,
   count(case when pf.compaction_level=1 and pf.to_delete is null then 1 end) num_l1,
   count(case when pf.compaction_level=2 and pf.to_delete is null then 1 end) num_l2 ,
   count(case when pf.compaction_level=0 and pf.to_delete is not null then 1 end) deleted_num_l0,
   count(case when pf.compaction_level=1 and pf.to_delete is not null then 1 end) deleted_num_l1,
   count(case when pf.compaction_level=2 and pf.to_delete is not null then 1 end) deleted_num_l2
FROM parquet_file pf, partition p, shard s
WHERE pf.partition_id = p.id AND pf.shard_id = s.id
  AND p.partition_key = '2022-10-11'
GROUP BY s.shard_index, pf.table_id, pf.partition_id, p.partition_key
ORDER BY count(case when pf.to_delete is null then 1 end) DESC
LIMIT 50;

-- Partitions with level-0 files ingested within the last 4 hours
SELECT partition_key, id as partition_id
FROM partition p, (
   SELECT partition_id, max(created_at)
   FROM parquet_file
   WHERE compaction_level = 0 AND to_delete IS NULL
   GROUP BY partition_id
   HAVING to_timestamp(max(created_at)/1000000000) > now() - '(4 hour)'::interval
) sq
WHERE sq.partition_id = p.id;
```

These are other SQL you may find useful

```sql
-- Number of columns in a table
select left(t.name, 50), c.table_id, count(1) num_cols
from table_name t, column_name c
where t.id = c.table_id
group by 1, 2
order by 3 desc;
```
