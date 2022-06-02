# Deduplication

In an IOx table (aka measurement), data with the same primary key are considered duplicates. The primary key of a table is a set of all `tag` columns and the `time` column of that table. Duplicates can have different values of `field` columns and deduplication process is needed to only keep the most recently inserted field values.

Data **deduplication** happens in `Ingester`, `Compactor`, and `Querier` using the [same code base](https://github.com/influxdata/influxdb_iox/blob/fa2c1febf4b9dfb2d3049d9840ac087d9c4eb427/query/src/provider.rs#L410). In order to deduplicate chunks, their data must be sorted on `the same sort order`, and that sort order must include all columns of their primary key. We define this sort order to be the **sort key**. Since existing chunks can be deduplicated again and again with newly ingested chunks, maintaining a consistent sort order for chunks which may contain duplicated data helps avoid resorting them.

This document explains how data chunks are deduplicated and sorted consistently to avoid resort.

### Overlapped Chunks
Only chunks that might contain the same primary key might contain duplicates. This means that chunks with overlapped time ranges may potentially have duplicates. Since IOx partitions data by time (e.g. one day per partition), chunks in different partitions are guaranteed not to overlap. Figure 1 shows an example of two partitions. Each has four chunks respectively. Two of them overlap with each other, the other two do not overlap with any but one has duplicates within the chunk itself and the other has no duplicates at all.

```text
╔══════════════════════════════════════════════════════════════════╗
║                     PARTITION 1 (2022.04.25)                     ║
║ ┌──────────────────┐                                             ║
║ │       C1.1       │                                             ║
║ │Overlaps with C1.2│                                             ║
║ └──────────────────┘                                             ║
║     ┌──────────────────┐ ┌─────────────────┐ ┌─────────────────┐ ║
║     │       C1.2       │ │      C1.3       │ │      C1.4       │ ║
║     │Overlaps with C1.1│ │Duplicates within│ │  No Duplicates  │ ║
║     └──────────────────┘ └─────────────────┘ └─────────────────┘ ║
╚══════════════════════════════════════════════════════════════════╝
╔══════════════════════════════════════════════════════════════════╗
║                     PARTITION 2 (2022.04.24)                     ║
║ ┌──────────────────┐                                             ║
║ │       C2.1       │                                             ║
║ │Overlaps with C2.2│                                             ║
║ └──────────────────┘                                             ║
║     ┌──────────────────┐ ┌─────────────────┐ ┌─────────────────┐ ║
║     │       C2.2       │ │      C2.3       │ │      C2.4       │ ║
║     │Overlaps with C2.1│ │Duplicates within│ │  No Duplicates  │ ║
║     └──────────────────┘ └─────────────────┘ └─────────────────┘ ║
╚══════════════════════════════════════════════════════════════════╝
Figure 1: Two partitions each has four chunks
```

### Data Deduplication

Figure 2 shows a simplified query plan that IOx builds (in Ingester, Compactor and Querier using the same code based pointed out above) to read all eight chunks of these two partitions and deduplicate them to return only non-duplicated data. This diagram should be read bottom up as follows:
- First, IOx scans eight chunks of the two partitions.
- Chunks C1.1 and C1.2 overlap, they need to get deduplicated. Same to chunks C2.1 and C2.2. To do this, chunks C1.1 and C1.2 are sorted on the same sort key K1.1_2 (see below for how it is computed) and the Dedup (deduplication) is applied to eliminate duplicated values. Similarly, chunks C2.1 and C2.2 are sorted on the same K2.1_2 sort key and then deduplicated.
- Chunk C1.3 does not overlap with any chunks but potentially has duplicates in itself, hence it also needs to get sorted on its sort key K1.3 and then deduplicated. Same to chunk C2.3.
- Chunk C1.4 and C2.4 do not overlap with any other chunks and each contain no duplicates, no deduplication is needed for them hence they do not need to get sorted either. 
- All the data is then unioned and returned.

Here is [the code](https://github.com/influxdata/influxdb_iox/blob/fa2c1febf4b9dfb2d3049d9840ac087d9c4eb427/query/src/provider.rs#L419) to identify which chunks overlapped and which chunks do not overlap with any other chunks but have or have not duplicates within themselves. The document below will explain how that code works.

```text

 ┌────────────────────────────────────────────────────────────────────────────────┐ 
 │                                     Union                                      │ 
 └────────────────────────────────────────────────────────────────────────────────┘ 
         ▲                      ▲                  ▲         ▲         ▲        ▲   
         │                      │                  │         │         │        │   
┌─────────────────┐    ┌─────────────────┐     ┌──────┐  ┌──────┐      │        │   
│      Dedup      │    │      Dedup      │     │Dedup │  │Dedup │      │        │   
└─────────────────┘    └─────────────────┘     └──────┘  └──────┘      │        │   
    ▲          ▲           ▲          ▲            ▲         ▲         │        │                     
    │          │           │          │            │         │         │        │                      
........   ........    ........   ........     ........  ........      │        │                      
│ Sort │   │ Sort │    │ Sort │   │ Sort │     │ Sort │  │ Sort │      │        │                      
│K1.1_2│   │K1.1_2│    │K2.1_2│   │K2.1_2│     │ K1.3 │  │ K2.3 │      │        │                      
........   ........    ........   ........     ........  ........      │        │                      
    ▲          ▲           ▲          ▲            ▲         ▲         │        │                      
    │          │           │          │            │         │         │        │                      
┌──────┐   ┌──────┐    ┌──────┐   ┌──────┐     ┌──────┐  ┌──────┐   ┌──────┐ ┌──────┐               
│ C1.1 │   │ C1.2 │    │ C2.1 │   │ C2.1 │     │ C1.3 │  │ C2.3 │   │ C1.4 │ │ C2.4 │               
└──────┘   └──────┘    └──────┘   └──────┘     └──────┘  └──────┘   └──────┘ └──────┘                

Figure 2: A simplified query plan that scans all eight chunks of two partitions and deduplicate their data                 
```

If many queries read these two partitions, IOx has to build the same query plan above which we think suboptimal because of sort and deduplication. To avoid this suboptimal query plan, IOx is designed to:
1. Data in each partition is frequently compacted to only contain a few non-overlapped and non-duplicated chunks as shown in Figure 3 which will help scan fewer chunks and eliminate the `Sort` and `Dedup` operators as illustrated in Figure 4.
2. All **persisted chunks of the same partition are sorted on the same key** to avoid resorting before deduplication. So the plan in Figure 2 is only seen sometimes in IOx (mostly in Ingester and Querier with data sent back from Ingester), instead, you will mostly see the plan in Figure 4 (for data in Figure 3) or in Figure 6 (for data of Figure 5)

```text
╔═════════════════════════════════════════╗ ╔════════════════════════╗
║        PARTITION 1 (2022.04.25)         ║ ║PARTITION 2 (2022.04.24)║
║ ┌──────────────────┐┌──────────────────┐║ ║  ┌──────────────────┐  ║
║ │      C1.12       ││      C1.34       │║ ║  │     C2.1234      │  ║
║ │  No duplicates   ││  No duplicates   │║ ║  │  No duplicates   │  ║
║ └──────────────────┘└──────────────────┘║ ║  └──────────────────┘  ║
╚═════════════════════════════════════════╝ ╚════════════════════════╝
Figure 3: Compacted Partitions 1 and 2
```
```text                                                        
               ┌──────────────────────────────┐                       
               │            Union             │                       
               └──────────────────────────────┘                       
                    ▲          ▲           ▲                          
                    │          │           │                          
                ┌──────┐   ┌──────┐    ┌──────┐                       
                │C1.12 │   │C1.34 │    │C2.123│                       
                └──────┘   └──────┘    └──────┘     
Figure 4: Scan plan of the compacted Partitions 1 and 2 of Figure 3    

```
```text
╔════════════════════════════════════════╗  ╔═════════════════════════╗
║        PARTITION 1 (2022.04.25)        ║  ║PARTITION 2 (2022.04.24) ║
║ ┌────────────────────┐                 ║  ║  ┌──────────────────┐   ║
║ │        C1.1        │                 ║  ║  │     C2.1234      │   ║
║ │       Sorted       │                 ║  ║  │      Sorted      │   ║
║ │Overlap with C1.234 │                 ║  ║  │  No duplicates   │   ║
║ └────────────────────┘                 ║  ║  └──────────────────┘   ║
║             ┌────────────────────────┐ ║  ╚═════════════════════════╝
║             │         C1.234         │ ║                             
║             │         Sorted         │ ║                             
║             │   Overlap with C1.1    │ ║                             
║             └────────────────────────┘ ║                             
╚════════════════════════════════════════╝                             
Figure 5: Two partitions, one is fully compacted into one sorted chunk, the other is partially compacted into two sorted overlapped chunks
```
```text
┌───────────────────────────┐
│           Union           │
└───────────────────────────┘
         ▲              ▲    
         │              │    
┌─────────────────┐ ┌───────┐
│      Dedup      │ │C2.1234│
└─────────────────┘ └───────┘
    ▲          ▲             
    │          │             
┌──────┐   ┌──────┐          
│ C1.1 │   │C1.234│          
└──────┘   └──────┘          
Figure 6: Scan plan of data in Figure 5

```

### Sorted and Unsorted Chunks
- Persisted chunks are always sorted and contain no duplicates within themselves
- Non-persisted chunks are not sorted and
  - Every non-persisted chunk in the Ingester may contain duplicates
  - Every non-persisted chunk sent from Ingester to Querier contain no duplicates

# Sort Keys
Due to supporting schema-on-write, it may be the case that chunks of a table have different sets of columns  for that table.  To keep the sort key consistent for all chunks in the same partition, IOx stores two kinds of sort keys:
1. **Chunk Sort Key**: this is the sort key of the chunk and only includes tag and time columns of that chunk.
2. **Partition Sort Key**: this is the sort key of all chunks of the same partition. This sort key will include time and all tag columns of all chunks.

### Compute Partition Sort Key and Chunk Sort Key
- For the first chunk of the partition, its Chunk Sort Key will be computed based on the ascending of cardinality of the tag columns available in that chunk. Time column is always last. This Chunk Sort Key will be stored with the chunk. Since there is only one chunk at this time, the Partition Sort Key is the same as the Chunk Sort Key and stored in the Partition catalog object for that chunk.
- For the following chunks, its Chunk Sort Key will be the same as the Partition Sort Key but
  - If the chunk does not include certain tags in the Partition Sort Key, its Chunk Sort Key will be the Partition Sort Key minus the missing columns.
  - If the chunk include extra tags that do not exist in the Partition Sort Key, those tags will be added to the end but in front of the time column. In this case, the Partition Sort Key is changed and must be updated to the Partition catalog of the chunk.

**Example**

In the example below, chunks in lower numbers are persisted to parquet files before the ones with higher numbers.

```text

         | Tags of Chunk |   Chunk Sort Key | Partition Sort Key 
---------|---------------|------------------|---------------------------------------------------
Chunk 1  | tag1          | tag1, time       | tag1, time (need catalog insert)
Chunk 2  | tag2          | tag2, time       | tag1, tag2, time (need catalog update)
Chunk 3  | tag1, tag3    | tag1, tag3, time | tag1, tag2, tag3, time (need catalog update)
Chunk 4  | tag2, tag3    | tag2, tag3, time | tag1, tag2, tag3, time
Chunk 5  | tag1, tag4    | tag1, tag4, time | tag1, tag2, tag3, tag4, time (need catalog update)
```

Assuming Chunk 2 and Chunk 3 overlap. As explained above, a query that needs to read those two chunks have to deduplicate them. Their sort key will be read from the Partition Sort Key which is `tag1, tag2, tag3, tag4, time`. Since those chunks do not include `tag4`, it will be eliminated and the actually sort key of those 2 chunks is `tag1, tag2, tag3, time`. To ensure those two chunks have the same columns, IOx adds missing columns with all NULL values while scanning them(`tag1` and `tag3` to Chunk 2 and `tag2` to Chunk 3). Even though adding NULL columns `tag1` and `tag3` to Chunk 2 makes its primary key and sort key now `tag1, tag2, tag3, time`, it is still sorted as it was on its original `tag2, time` only. Hence no sorted needed for Chunk 2. Same to Chunk 3. Figure 7 shows the query plan that reads Chunk 2 and Chunk 3.

```text
                  ┌────────────────────────────────────────────────────────────────┐                 
                  │                             Dedup                              │                 
                  └────────────────────────────────────────────────────────────────┘                 
                        ▲                                                   ▲                        
                        │                                                   │                        
                        │                                                   │                        
┌───────────────────────────────────────────────┐   ┌───────────────────────────────────────────────┐
│                    Chunk 2                    │   │                    Chunk 3                    │
│                                               │   │                                               │
│         . Was sorted on: `tag2, time`         │   │      . Was sorted on: `tag1, tag3, time`      │
│                                               │   │                                               │
│. Is still sorted on: `tag1, tag2, tag3, time` │   │. Is still sorted on: `tag1, tag2, tag3, time` │
│    because tag1 and tag3 contain all NULLs    │   │        because tag2 contains all NULLs        │
└───────────────────────────────────────────────┘   └───────────────────────────────────────────────┘
Figure 7: Scan plan that reads Chunk 2 and Chunk 3
```