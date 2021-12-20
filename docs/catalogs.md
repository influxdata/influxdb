# IOx Catalog: The Metadata for Operating a Database

To continue our architecture documentation series, this document explains how IOx uses **metadata** to organize its physical chunks and operate its Data Lifecycle described in previous document, [IOx Data Organization and LifeCycle](data_organization_lifecycle.md). We also shows how metadata is incrementally saved and used to rebuild the database, as well as used to find the right physical chunks quickly to run queries.

Figure 1 of previous [document](data_organization_lifecycle.md) illustrates IOx Data organization. Since actual data is only stored in the bottom layer, `Chunks`, the other information in other layers needed to access and manage the chunks are considered as `metadata` and, in IOx, handled by a Catalog.

## Catalog
Figure 1 below shows the organization of both IOx Catalog and Data. The bottom layer `Data Chunks` are physical data described in previous [document](data_organization_lifecycle.md). All the layers from `Catalogs` to `Chunk Stages` are metadata managed by IOx `Catalog`. Each database has its own `Catalog`[^cat] that contains a set of `Tables`, each consists of a set of `Partitions`. A partition includes many `Chunks`, each is represented by a `ChunkStage` that points to their corresponding physical chunk data.

[^cat]: This design might be changed in the future to meet other use cases.

Since the catalog is the central information shared to all users of the database, it must be kept consistent. Each object of the catalog must be locked in either shared or exclusive mode when accessed or modified. Each catalog object also contains information to operate the chunk lifecycle, run queries, as well as measure the health of the system. 

**Examples of information for operating the chunk lifecycle**
* A chunk includes `LifecycleAction` that keeps track of on-going action of a chunk (e.g. `compacting`, `persisting`) to avoid running the same job on the chunk. 
* A partition contains `PersistenceWindows` that  keep track of ingested data within a partition to determine when it can be persisted. This allows IOx to receive out of order writes in their timestamps while persisting mostly in non-time overlapping Object Store files.

**Examples of information for measuring the health of the system**
* A catalog consists of metrics that measure how often its tables, partitions, and chunks are locked, what kinds of locks (e.g. shared or exclusive), and their lock wait time.

**Examples of information for running queries quickly**
* A catalog chunk column includes statistics of its data such as `min`, `max`, `row count`, `null count`. If a query does not need data between the chunk column's [min,max] range, that chunk data will be pruned from reading. If a query is to count the number of rows, the `row count` statistics of the chunk is good enough to answer the query without reading its chunk data.


```text
                                                             ┌───────────┐                                     
                                                             │IOx Server │                        IOx Server   
                                                             └───────────┘                                     
                                                                   │                                           
                                                    ┌──────────────┼───────────────┐                           
                                                    ▼              ▼               ▼                           
                                              ┌───────────┐                ┌────────────┐                      
                                              │Database 1 │       ...      │ Database p │         Databases    
                                              └───────────┘                └────────────┘                      
                                                    │                              │                           
                                                    ▼                              ▼                           
                                              ┌───────────┐                                                    
                                              │ Catalog 1 │                       ...             Catalogs     
                                              └───────────┘                                                    
                                                    │                                                          
                                    ┌───────────────┼───────────────────┐                                      
                                    ▼               ▼                   ▼                                      
                              ┌──────────┐                        ┌──────────┐                                 
                              │ Table 1  │         ...            │ Table n  │                       Tables    
                              └──────────┘                        └──────────┘                                 
                                    │                                   │                                      
                 ┌──────────────────┼────────────────────┐              │                                      
                 ▼                  ▼                    ▼              ▼                                      
          ┌────────────┐                          ┌────────────┐                                               
          │Partition 1 │           ...            │Partition m │       ...                        Partitions   
          │(2021-12-10)│                          │(2021-12-20)│                                               
          └────────────┘                          └────────────┘                                               
                 │                                       │                                                     
       ┌─────────▼───────┐               ┌───────────────┼─────────────────────┐                               
       ▼                 ▼               ▼               ▼                     ▼                               
   ┌───────┐         ┌───────┐       ┌───────┐       ┌───────┐             ┌───────┐                           
   │Chunk 1│         │Chunk 2│       │Chunk 3│       │Chunk 4│             │Chunk 5│               Chunks      
   └───────┘         └───────┘       └───────┘       └───────┘             └───────┘                           
       │                 │               │               │                     │                               
       ▼                 ▼               ▼               ▼                     ▼                               
┌──────────────┐ ┌──────────────┐ ┌──────────────┐┌──────────────┐     ┌──────────────┐                        
│ ChunkStage 1 │ │ ChunkStage 2 │ │ ChunkStage 3 ││ ChunkStage 4 │     │ ChunkStage 5 │        Chunk Stages    
│    (Open)    │ │   (Frozen)   │ │    (Open)    ││   (Frozen)   │     │ (Persisted)  │                        
└──────────────┘ └──────────────┘ └──────────────┘└──────────────┘     └───────┬──────┘                        
       │                 │               │               │              ┌──────▼──────┐                        
       ▼                 ▼               ▼               ▼              ▼             ▼                        
┌──────────────┐ ┌──────────────┐  ┌───────────┐ ┌────────────────┐┌───────────┐┌───────────┐                  
│    O-MUB 1   │ │     RUB 2    │  │  O-MUB 3  │ │    F-MUB 4     ││    RUB 5  ││   OS 5    │                  
│              │ │              │  │           │ │                ││           ││           │    Data Chunks   
│col1 col2 col3│ │  col1 col4   │  │ col1 col2 │ │ col1 col2 col3 ││ col2 col3 ││ col2 col3 │                  
│---- ---- ----│ │  ---- ----   │  │ ---- ---- │ │ ---- ---- ---- ││ ---- ---- ││ ---- ---- │                  
│---- ---- ----│ │  ---- ----   │  │ ---- ---- │ │ ---- ---- ---- ││ ---- ---- ││ ---- ---- │                  
└──────────────┘ │  ---- ----   │  └───────────┘ │ ---- ---- ---- ││ ---- ---- ││ ---- ---- │                  
                 └──────────────┘                │ ---- ---- ---- │└───────────┘└───────────┘                  
                                                 └────────────────┘                                            
Figure 1: Catalog and Data Organization
```


## Rebuild a Database from its Catalog
Since the `Catalog` is the core of the database, losing the catalog means losing the database; or rebuilding a database means rebuilding its catalog. Thus to rebuild a catalog,  its information needs to saved in a durable storage.

Basically, if an IOx server goes down unexpectedly, we will lose the in-memory catalog shown in Figure 1 and all of its in-memory data chunks `O-MUBs`, `F-MUBs`, and `RUBs`. Only data of `OS` chunks are not lost. As pointed out in [IOx Data Organization and LifeCycle](data_organization_lifecycle.md) that chunk data is persisted contiguously with their loading time, `OS` chunks only include data ingested before the one in `MUBs` and `RUBs`. Thus, in principal, rebuilding the catalog includes two major steps: first rebuild the catalog that links to the already persisted `OS` chunks, then rerun ingesting data of `O-MUBs`, `F-MUBs` and `RUBs`. To perform those 2 steps, IOx saves `catalog transactions` that include minimal information of the latest stage of all valid `OS` chunks (e.g, chunks are not deleted), and `checkpoints` of when in the past to re-ingest data. At the beginning of the `Catalog rebuild`, those transactions will be read from the Object Store, and their information is used to rebuild the Catalog and rerun the necessary ingestion. Due to IOx `DataLifeCyclePolicy` that is responsible for when to trigger compacting chunk data, the rebuilt catalog may look different from the one before but as long as all the data in previous `O-MUBs`, `F-MUBs`, and `RUBs` are reloaded/recovered and tracked in the catalog, it does not matter which chunk types the data belong to.  Refer to [Catalog Persistence](catalog_persistence.md) for the original design of Catalog Persistence and [CheckPoint](https://github.com/influxdata/influxdb_iox/blob/b39e01f7ba4f5d19f92862c5e87b90a40879a6c9/persistence_windows/src/checkpoint.rs) for the detailed implementation of IOx Checkpoints.

## Answer queries through the Catalog
Catalog is essential to not only lead IOx to the right chunk data for answering user queries but also critical to help read the minimal possible data. Besides the catalog structure shown in Figure 1, chunk statistics (e.g. chunk column's min, max, null count, row count) are calculated while building up the catalog and included in corresponding catalog objects. This information enables IOx to answer some queries right after reading its catalog without scanning physical chunk data as in some examples above [^query]. In addition, IOx provides system tables to let users query their catalog data such as number of tables, number of partitions per database or table, number of chunks per database or tables or partition, number of each chunk type and so on.

[^query]: Chunk pruning, query planning, optimization, and execution are beyond the scope of this document.

Now we know [IOx Data Organization and LifeCycle](data_organization_lifecycle.md) and Catalog, let us move to next topics: **IOx Transactions and Locks** (to be written and linked)