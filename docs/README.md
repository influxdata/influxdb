# InfluxDB IOx Documentation

Please see the main [README](../README.md) for user facing documentation.

This directory contains internal design documentation of potential
interest for those who wish to understand how the code works.

## IOx Tech Talks

We hold monthly Tech Talks that explain the project's technical underpinnings. You can register for the [InfluxDB IOx Tech Talks here](https://www.influxdata.com/community-showcase/influxdb-tech-talks/), or you can find links to previous sessions below or in the [YouTube playlist](https://www.youtube.com/playlist?list=PLYt2jfZorkDp-PKBS05kf2Yx2NrRyPAAz):

* December 2020: Rusty Introduction to Apache Arrow [recording](https://www.youtube.com/watch?v=dQFjKa9vKhM)
* Jan 2021: Data Lifecycle in InfluxDB IOx & How it Uses Object Storage for Persistence [recording](https://www.youtube.com/watch?v=KwdPifHC1Gc)
* February 2021: Intro to the InfluxDB IOx Read Buffer [recording](https://www.youtube.com/watch?v=KslD31VNqPU) [slides](https://www.slideshare.net/influxdata/influxdb-iox-tech-talks-intro-to-the-influxdb-iox-read-buffer-a-readoptimized-inmemory-query-execution-engine)
* March 2021: Query Engine Design and the Rust-Based DataFusion in Apache Arrow [recording](https://www.youtube.com/watch?v=K6eCAVEk4kU) [slides](https://www.slideshare.net/influxdata/influxdb-iox-tech-talks-query-engine-design-and-the-rustbased-datafusion-in-apache-arrow-244161934)
* April 2021: InfluxDB IOx Tech Talks: Replication, Durability and Subscriptions in InfluxDB IOx [recording](https://www.youtube.com/watch?v=UQj8ZaH5Yi4) [slides](https://www.slideshare.net/influxdata/influxdb-iox-tech-talks-replication-durability-and-subscriptions-in-influxdb-iox)
* May 2021: Catalogs - Turning a Set of Parquet Files into a Data Set [recording](https://www.youtube.com/watch?v=Zaei3l3qk0c), [slides](https://www.slideshare.net/influxdata/catalogs-turning-a-set-of-parquet-files-into-a-data-set)
* June 2021: Performance Profiling in Rust  [recording](https://www.youtube.com/watch?v=_ZNcg-nAVTM), [slides](https://www.slideshare.net/influxdata/performance-profiling-in-rust)
* July 2021: Impacts of Sharding, Partitioning, Encoding & Sorting on Distributed Query Performance [recording](https://www.youtube.com/watch?v=VHYMpItvBZQ), [slides](https://www.slideshare.net/influxdata/impacts-of-sharding-partitioning-encoding-and-sorting-on-distributed-query-performance)
* September 2021: Observability of InfluxDB IOx Tracing, Metrics and System Tables [recording](https://www.youtube.com/watch?v=tB-umdJCJQc), [slides](https://www.slideshare.net/influxdata/observability-of-influxdb-iox-tracing-metrics-and-system-tables)
* October 2021: Query Processing in InfluxDB IOx [recording](https://www.youtube.com/watch?v=9DYkWuM8xco), [slides](https://www.slideshare.net/influxdata/influxdb-iox-tech-talks-query-processing-in-influxdb-iox)
* November 2021: The Impossible Dream: Easy-to-Use, Super Fast Software and Simple Implementation [recording](https://www.youtube.com/watch?v=kK_7t24dQ-Q&list=PLYt2jfZorkDp-PKBS05kf2Yx2NrRyPAAz&index=2&t=122s), [slides](https://www.slideshare.net/influxdata/influxdb-iox-tech-talks-the-impossible-dream-easytouse-super-fast-software-and-simple-implementation)


## Table of Contents:

###  User Contents

* [SQL command line tips and tricks](sql.md)
* [CLI command line tips and tricks](cli.md)

###  Developer Content

* [Rust style and Idiom guide](style_guide.md)
* [Distributed Tracing Guide](tracing.md)
* [Logging Guide](logging.md)
* [Handling Observability Context](observability.md)
* [Metrics guide](metrics.md)
* [Profiling Guide](profiling.md)
* [Valgrind Guide](valgrind.md)
* [Protobuf tips and tricks](protobuf.md)
* [Testing documentation](testing.md) for developers of IOx
* IOx Architecture
    * [Data Deduplication & Sort Key](dedup_and_sort.md)
    * Transactions and locks (to be written & linked)
* [Thoughts on using multiple cores / thread pools](multi_core_tasks.md)
* [Compactor](compactor.md)
* [Data Rentention Policy](retention_policy.md)
* [Query Engine Docs](../iox_query/README.md)
* [Notes on the use of local filesystems](local_filesystems.md)
* [Querier <> Ingester Query Protocol](ingester_querier_protocol.md)
* [Underground Guide to Running IOx Locally](underground_guide.md)
