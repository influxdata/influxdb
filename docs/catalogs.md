# IOx Catalogs:  The Metadata for Operating a Database

To continue our architecture documentation series, this document explains how IOx uses **metadata** to organize its physical chunks and operate its Data Lifecycle described in previous document, [IOx Data Organization and LifeCycle](link). We also shows how metadata is incrementally saved and used to rebuild the database, as well as used to find the right physical chunks quickly to run queries.

Figure 1 copied from previous [document](link) illustrates IOx Data organization. Since actual data is only stored in the bottom layer, `Chunks`, the other information in other layers needed to access and manage the chunks are considered as `metadata` and, in IOx, managed by Catalogs.

```text
                                                                 ┌───────────┐                                    
                                                                 │IOx Server │                        IOx Server  
                                                                 └───────────┘                                    
                                                                       │                                          
                                                        ┌──────────────┼────────────────┐                         
                                                        ▼              ▼                ▼                         
                                              ┌───────────┐                     ┌────────────┐                    
                                              │Database 1 │           ...       │ Database p │        Databases   
                                              └───────────┘                     └────────────┘                    
                                                    │                                                             
                                     ┌──────────────┼─────────────┐                                               
                                     ▼              ▼             ▼                                               
                               ┌──────────┐                 ┌──────────┐                                          
                               │ Table 1  │       ...       │ Table n  │                                Tables    
                               └──────────┘                 └──────────┘                                          
                                     │                            │                                               
                      ┌──────────────┼──────────────┐             │                                               
                      ▼              ▼              ▼             ▼                                               
               ┌────────────┐                ┌────────────┐                                                       
               │Partition 1 │       ...      │Partition m │      ...                                 Partitions   
               │(2021-12-10)│                │(2021-12-20)│                                                       
               └────────────┘                └──────┬─────┘                                                       
                      │                             │                                                             
        ┌─────────────┼─────────────┐               │                                                             
        ▼             ▼             ▼               ▼                                                             
┌──────────────┐              ┌───────────┐                                                                       
│    Chunk 1   │     ...      │  Chunk 2  │        ...                                                 Chunks     
│              │              │           │                                                                       
│col1 col2 col3│              │ col1 col4 │                                                                       
│---- ---- ----│              │ ---- ---- │                                                                       
│---- ---- ----│              │ ---- ---- │                                                                       
└──────────────┘              │ ---- ---- │                                                                       
                              └───────────┘                                                                       
Figure 1: Data Organization in an IOx Server

```


Each database of IOx owns three catalogs demonstrated in Figure 2. The `In-Memory Catalog` (or simply the `Catalog`), contains metadata to access all chunk types in both memory and Object Store described in [IOx Data Organization and LifeCycle](link). The `Preserved Catalog` is used to rebuild the database in case it is shutdown or unexpected destroyed by accidents or disasters. The `Query Catalog` includes extra information to access the right chunks and their statistics quickly for running queries, recording necessary log info, and reporting the results back to users. The `Other Properties` are a set of properties created/rebuilt with the database based on some default configuration parameters of the IOx Server or specified by the user who creates it.

```text
                                     ┌───────────┐                   
                                     │IOx Server │                   
                                     └───────────┘                   
                                           │                         
                            ┌──────────────┼───────────────┐         
                            ▼              ▼               ▼         
                      ┌───────────┐                 ┌────────────┐   
                      │Database 1 │       ...       │ Database p │   
                      └───────────┘                 └────────────┘   
                            │                              │         
      ┌─────────────┬───────┴─────┬────────────┐           │         
      ▼             ▼             ▼            ▼           ▼         
┌──────────┐  ┌──────────┐  ┌──────────┐ ┌──────────┐                
│In-Memory │  │Preserved │  │  Query   │ │  Other   │      ...       
│ Catalog  │  │ Catalog  │  │ Catalog  │ │Properties│                
└──────────┘  └──────────┘  └──────────┘ └──────────┘                

Figure 2: Metadata of a Database
```
## In-memory Catalog

`In-Memory Catalog` implemented in the code as [Catalog](https://github.com/influxdata/influxdb_iox/blob/8a2410e161996603a4147e319c71e5bb38ca9cb7/server/src/db/catalog.rs#L88) and shown in Figure 3 represents the structure of its actual `Data Organization` and hence looks similar to Figure 1. Note that the bottom layer of Figure 3 shows physical chunks of data (O-MUB 1, RUB 2, O-MUB 3, F-MUB 4, RUB 5, and OS 5) that the catalog does not store but points to. In short, the `In-Memory Catalog` contains a set of [`Tables`](https://github.com/influxdata/influxdb_iox/blob/8a2410e161996603a4147e319c71e5bb38ca9cb7/server/src/db/catalog/table.rs#L20), each of which consists of a set of [`Partitions`](https://github.com/influxdata/influxdb_iox/blob/8a2410e161996603a4147e319c71e5bb38ca9cb7/server/src/db/catalog/partition.rs#L116). A `Partition` includes a set of [`CatalogChunks`](https://github.com/influxdata/influxdb_iox/blob/76befe94ad14cd121d6fc5c58aa112997d9e211a/server/src/db/catalog/chunk.rs#L195), each of which is represented by a [`ChunkStage`](https://github.com/influxdata/influxdb_iox/blob/76befe94ad14cd121d6fc5c58aa112997d9e211a/server/src/db/catalog/chunk.rs#L130) that points to the corresponding Data Chunk(s). The previous document, [IOx Data Organization and LifeCycle](link), has described the chunk stages and chunk types supported in IOx. 

Each object of the catalog contains information for us to operate the chunk lifecycle, run queries, as well as measure the health of the system [^prop]. For example, the `In-Memory Catalog` object itself includes [`CatalogMetrics`](https://github.com/influxdata/influxdb_iox/blob/873ce27b0c0c5e9da33e6f4fae8c5be7c163c16c/server/src/db/catalog/metrics.rs#L21) that measure how often its tables, partitions, and chunks are locked, what kinds of locks (e.g. shared or exclusive), and their lock wait time[^lock]. This information helps the IOx team to measure lock contention to adjust the database with a right setup. The `(L)` next to each object or property indicates they are modifiable but must be locked while doing so. Locking ensures the catalog consistency and integrity but will bring down the throughput and performance of the system, hence should be always measured for appropriate actions. 

Another example of information we handle is the `Partition`'s `PersistenceWindows` that  keep track of ingested data within a partition to determine when it can be persisted. This allows IOx to receive out of order writes in their timestamps while persisting mostly in non-time overlapping Object Store files. The `CatalogChunk`'s `LifecycleAction` is another example that keeps track of on-going action for each chunk (e.g. `compacting`, `persisting`) to avoid running the same job on the same chunk. 

[^prop] To make Figure 3 easy to read, the properties listed in each object are just some examples. Click on the corresponding link to see the full set of properties in the code.
[^lock] Transaction and locks will be described more in a separate document.

```text
                                                          ┌──────────────────┐                             
                                                          │In-Memory Catalog │                             
                                                          │----------------- │                             
                                                          │  CatalogMetrics  │                             
                                                          └──────────────────┘                             
                                                                    │                                      
                                                                    │                                      
                                            ┌───────────────────────┼──────────────────────────┐           
                                            ▼                       │                          ▼           
                                       ┌───────────┐                ▼                    ┌───────────┐     
                                       │Table 1 (L)│                                     │Table n (L)│     
                                       │-----------│               ...                   │-----------│     
                                       │Schema (L) │                                     │Schema (L) │     
                                       └───────────┘                                     └───────────┘     
                    ┌────────────────────────┬─────────────────────────┐                       │           
                    │                        │                         │                       │           
                    ▼                        │                         ▼                       ▼           
     ┌─────────────────────────────┐         ▼          ┌─────────────────────────────┐                    
     │       Partition 1 (L)       │                    │       Partition m (L)       │       ...          
     │ --------------------------- │        ...         │ --------------------------- │                    
     │      PersistenceWindow      │                    │      PersistenceWindow      │                    
     │ CreatedTime & LastWriteTime │                    │ CreatedTime & LastWriteTime │                    
     │       NextChunkOrder        │                    │       NextChunkOrder        │                    
     │      PartitionMetrics       │                    │      PartitionMetrics       │                    
     └─────────────────────────────┘                    └─────────────────────────────┘                    
                    │                                                  │                                   
          ┌─────────▼─────────┐                    ┌───────────────────┼──────────────────────┐            
          │                   │                    │                   │                      │            
          ▼                   ▼                    ▼                   ▼                      ▼            
┌──────────────────┐┌──────────────────┐  ┌──────────────────┐┌──────────────────┐  ┌──────────────────┐   
│CatalogChunk 1 (L)││CatalogChunk 2 (L)│  │CatalogChunk 3 (L)││CatalogChunk 4 (L)│  │CatalogChunk 5 (L)│   
│------------------││------------------│  │------------------││------------------│  │------------------│   
│  FirstWriteTime  ││  FirstWriteTime  │  │  FirstWriteTime  ││  FirstWriteTime  │  │  FirstWriteTime  │   
│  LastWriteTime   ││  LastWriteTime   │  │  LastWriteTime   ││  LastWriteTime   │  │  LastWriteTime   │   
│ LifecycleAction  ││ LifecycleAction  │  │ LifecycleAction  ││ LifecycleAction  │  │ LifecycleAction  │   
│   ChunkMetrics   ││   ChunkMetrics   │  │   ChunkMetrics   ││   ChunkMetrics   │  │   ChunkMetrics   │   
└──────────────────┘└──────────────────┘  └──────────────────┘└──────────────────┘  └──────────────────┘   
          │                   │                    │                   │                      │            
          ▼                   ▼                    ▼                   ▼                      ▼            
   ┌──────────────┐   ┌──────────────┐      ┌──────────────┐    ┌──────────────┐      ┌──────────────┐     
   │ ChunkStage 1 │   │ ChunkStage 2 │      │ ChunkStage 3 │    │ ChunkStage 4 │      │ ChunkStage 5 │     
   │    (Open)    │   │   (Frozen)   │      │    (Open)    │    │   (Frozen)   │      │ (Persisted)  │     
   └──────────────┘   └──────────────┘      └──────────────┘    └──────────────┘      └──────┬───────┘     
          │                   │                    │                   │               ┌─────▼──────┐      
          ▼                   ▼                    ▼                   ▼               ▼            ▼      
   ┌──────────────┐   ┌──────────────┐       ┌───────────┐     ┌────────────────┐┌───────────┐┌───────────┐
   │    O-MUB 1   │   │     RUB 2    │       │  O-MUB 3  │     │    F-MUB 4     ││    RUB 5  ││   OS 5    │
   │              │   │              │       │           │     │                ││           ││           │
   │col1 col2 col3│   │  col1 col4   │       │ col1 col2 │     │ col1 col2 col3 ││ col2 col3 ││ col2 col3 │
   │---- ---- ----│   │  ---- ----   │       │ ---- ---- │     │ ---- ---- ---- ││ ---- ---- ││ ---- ---- │
   │---- ---- ----│   │  ---- ----   │       │ ---- ---- │     │ ---- ---- ---- ││ ---- ---- ││ ---- ---- │
   └──────────────┘   │  ---- ----   │       └───────────┘     │ ---- ---- ---- ││ ---- ---- ││ ---- ---- │
                      └──────────────┘                         │ ---- ---- ---- │└───────────┘└───────────┘
                                                               └────────────────┘                          

Figure 3: In-Memory Catalog
```
## Preserved Catalog
Since the `In-Memory Catalog` is always in memory, we need a way to save it incrementally and rebuild it as quick as possible in case of accidents or disasters, or in case we need to restart an IOx Server, or when we want to move a database to different IOx Server. [`Preserved Catalog`](https://github.com/influxdata/influxdb_iox/blob/76befe94ad14cd121d6fc5c58aa112997d9e211a/parquet_catalog/src/core.rs#L212) is used for those purposes.  Like data, IOx's `Preserved Catalog` is saved as Parquet files in Object Store such as Amazon S3 and only the catalog information of persisted chunks are stored in the Preserved Catalog. This is the result of our data lifecycle design that all data must be saved in the durable object store and that we do not need to touch the `Preserved Catalog' if nothing is persisted.

The `Preserved Catalog` keeps [`DatabaseCheckpoints`](https://github.com/influxdata/influxdb_iox/blob/b39e01f7ba4f5d19f92862c5e87b90a40879a6c9/persistence_windows/src/checkpoint.rs#L554) which represent a sequence number of writes before which all the writes are persisted. To restart a database, the `In-Memory Catalog` will be rebuilt to the state of the latest `DatabaseCheckpoint` found in the `PreservedCatalog`, then all the writes after that will be (re)ingested [^write].

If a database is still running but in an inconsistent state, it will be rebuilt by momentarily stopping accepting writes and discarding all in-memory data of `O-MUBs`, `F-MUBs`, `RUBs`. Then the `In-Memory Catalog` will be brought back to the latest `DatabaseCheckpoint` found in the `PreservedCatalog` as above and then all the writes after that will be (re)ingested.

Since Preserved Catalog only saves data to a consistent state and includes the well-known slow IO reads and writes, their transactions and related locks on persisting objects will impact the data lifecycle workload, especially if a lot of data continues getting persisted. This is another reason for us to monitor transaction and locking contention to adjust our `Data LifeCycle Policy` to ensure ingesting data flow smoothly until persisted. 

[^write]: currently InfluxDB ingests data to IOx from Kafka so we know which writes to start (re)ingesting.

## Query Catalog
[`Query Catalog`](https://github.com/influxdata/influxdb_iox/blob/218042784fdb3bd0aa2c13dcaaf2f39190d42329/server/src/db/access.rs#L123) shown in Figure 4 is an `In-Memory Catalog` plus extra information to access the right chunks and their statistics quickly for running queries, logging necessary information or metrics, and reporting the results back to users. For instance, since IOx uses [DataFusion](https://github.com/apache/arrow-datafusion) to plan and run queries, `UserTables` provide the required interface to DataFusion for doing so. Similarly, since we want to query our own catalog data such as number of tables, partitions, chunks types and so on, we construct `SystemTables` interface to send to DataFusion for that purpose. `ChunkAccess` helps find and prune chunks of the query table that do not include data needed by the query[^query]. `QueryLog` logs necessary information such as times and types the queries are run.

[^query] Chunk pruning, query planning, optimization, and execution are beyond the scope of this document.

```text
┌────────────────┐
│ Query Catalog  │
│----------------│
│In-MemoryCatalog│
│   UserTables   │
│  SystemTables  │
│  ChunkAccess   │
│    QueryLog    │
└────────────────┘

Figure 4: Query Catalog
```

Now we know [IOx Data Organization and LifeCycle](link) and Catalogs, let us move to next topics: **IOx Transactions and Locks** (to be written and linked)