//! The compactor.
//!
//!
//! # Mission statement
//!
//! This processes parquet files produced by the ingesters so that queriers can read them more efficiently. More precisely it
//! tries to meet the following -- partially conflicting -- objectives:
//!
//! - **O1 -- duplicates:** Duplicate across files should be removed[^duplicates_within_files]. Removing duplicates
//!   during query is costly.
//! - **O2 -- overlaps:** The key-space (tags + timestamp) within parquet files should be strictly distinct. Without this
//!   attribute the querier has now way to detect the inter-file key uniqueness and hence must run costly
//!   de-duplications during query time.
//! - **O3 -- minimize number of  files:** There should be as few files as possible. Every file comes with
//!   overhead both within the catalog and on the querier side.
//! - **O4 -- max file size:** Parquet files must have a maximum (configurable) size (with a certain margin on top due
//!   for technical reasons[^max_parquet_size]). Files that are too large will cause memory issues with querier
//! - **O5 -- minimal changes:** The changes to the set of parquet files should be as minimal as possible to increase cache
//!   efficiency on the querier side,  reduce write amplification (rewriting the same row over and over again) and
//!   decrease the catalog load (each file creation / deletion requires catalog updates).
//! - **O6 -- catch-up:** The compactor should be able to not "fall behind" the ingest tier.
//! - **O7 -- costs:** The compactor shall be cost-efficient.
//!
//! From these objectives, we can derive the following additional requirements -- some more and some less obvious ones:
//!
//! - **R1 -- avoid re-compaction:** Avoid re-processing the same data multiple times. This can be derived from *O5*,
//!   *O7*, and potentially *O6*.
//! - **R2 -- parallelization:** Be able to run multiple compaction jobs at the same time. Derived from *O6* and *O7*.
//! - **R3 -- scale-out:** Be able to scale to multiple nodes. Derived from *O6* but may very much depend on *R2*.
//! - **R4 -- concurrent compute & IO:** Be able to decouple IO (and the involved latencies) from the actual
//!   computation. Derived from *O6* and *O7*.
//!
//!
//! # File Levels
//!
//! Each parquet file has a `compaction_level` that the compactor uses
//! to optimize its choice of what files to compact. There are three levels:
//! * `L0`: [`data_types::CompactionLevel::Initial`]
//! * `L1`: [`data_types::CompactionLevel::FileNonOverlapped`]
//! * `L2`: [`data_types::CompactionLevel::Final`].
//!
//! The compactor maintains the following invariants with levels:
//!
//! 1. The ingester writes all new data as `L0` files.
//! 2. The compactor creates `L1` and `L2` files.
//! 3. `L1` files never overlap with other `L1` files.
//! 4. `L2` files never overlap with other `L2` files.
//! 5. `L0` files can overlap with each other and any `L1` or `L2` files.
//! 6. `L1` files can overlap with `L2` files.
//! 7. If an `L0` overlaps with an `L1`, the L0's `max_l0_create_at` must  >= L1's `max_l0_create_at`.
//! 8. If an `L1` overlaps with an `L2`, the L1's `max_l0_create_at` must  >= L2's `max_l0_create_at`.
//!
//! To maintan those invariants, the compactor algorithms must follow these rules:
//!
//! i. Never compact more than 2 levels of files together. Every time, at most 2 consecutive levels must be compacted.
//! ii. Always start from the smallest level, compact L0 with L1. Then compact L1 and L2 together.
//! iii. If all L0 files cannot be compacted in one run, they must be picked by the order of their `max_l0_create_at`.
//! iv. If all L1 files cannot be compacted in one run, they must be picked by the order of their `min_time`.
//!    See funtion `order_files` where iii and iv are enforced
//!    Note that if we want to compact many L2s to fewer L2 files (or to L3 in the future), L2 files must be picked by
//!    the order of their `min_time` as well.
//! v. After the appropriate files are groupped to compact in one DF plan, higher level files are always ordered first.
//!    Function ir_planner::planner_v1::order enforces this rule.
//! vi. Split and Compact are exclusive operations. If the overlapped files are large and the split is needed,
//!    the split must be done before continuing compaction.
//! vii. Hot and Cold compaction on the same partition cannot be compacted concurrently.
//!
//! Over time the compactor aims to rearrange data in all partitions
//! into a small number of large `L2` files.
//!
//! # Crate Layout
//!
//! This crate tries to decouple "when to do what" from "how to do what". The "when" is described by the [driver] which
//! provides an abstract framework. The "how" part is described by [components] which act as puzzle pieces that are
//! plugged into the [driver].
//!
//! The concrete selection of components and their setup is currently [hardcoded](crate::components::hardcoded). This
//! setup process take a [config] and creates a [component setup](crate::components::Components).
//!
//! The final flow graph looks like this:
//!
//! ```text
//! (CLI/Env) --> (Config) --[hardcoded]--> (Components) --> (Driver)
//! ```
//!
//! ## Driver
//! The driver is the heart of the compactor and describes and executes the framework.
//!
//! This is the data and control flow of driver:
#![doc = include_str!("../img/driver.svg")]
//!
//! In general modification of the driver should be rare. Most changes should be implement by using components.
//!
//! ## Components
//! Components can be thought as puzzle pieces used by the driver. Technically there are two parts to it: the driver
//! interfaces (traits) and the implementations.
//!
//! **Note: The code samples used in the "components" section are simplified and may not reflect actual components. They are
//! mostly illustrations.**
//!
//! ### Interfaces
//! Component interfaces should describe one single aspect of the framework. For example:
//!
//! ```
//! # use std::fmt::{Debug, Display};
//! # use data_types::ParquetFile;
//! /// Calculate importance of a file.
//! pub trait FileImportants: Debug + Display + Send + Sync {
//!     /// Rates file by importance.
//!     ///
//!     /// Higher ratings mean "more important".
//!     fn rate(&self, file: &ParquetFile) -> u64;
//! }
//! ```
//!
//! Looking this interface you will note the following aspects:
//!
//! - **`Debug`:** Allows to use the component in our structs that implement [`Debug`].
//! - **`Display`:** Allows to produce easy human (and machine) readable dumps of the current component setup. Also
//!   allows other components to implement [`Display`] more easily.
//! - **immutable:** All methods take `&self`. Interior mutability (e.g. for caching) is allowed. This allows easy parallelization.
//! - **not-consuming:** No method takes `self`. This ensures that the trait is object-safe and can be put into an [`Arc`].
//! - **`Send + Sync`:** Together with the the two previous points this allows the usage within an [`Arc`].
//! - **single method:** Most interfaces will only have a single method. This is because they have one dedicated job.
//!
//! The above interface will not allow any IO because it is not async. It is important to keep the interfaces that
//! perform IO and the ones that don't clearly apart, so it is visible to the user and allows a proper driver design. An
//! interface that performs IO looks like this:
//!
//! ```
//! # use std::fmt::{Debug, Display};
//! # use async_trait::async_trait;
//! # use data_types::{Partition, PartitionId};
//! /// Fetches partition info.
//! #[async_trait]
//! pub trait PartitionSource: Debug + Display + Send + Sync {
//!     /// Fetches partition info.
//!     ///
//!     /// This method retries internally.
//!     async fn fetch(&self, id: PartitionId) -> Partition;
//! }
//! ```
//!
//! For IO interfaces, all the points of the normal interface apply. In addition, the following points are important:
//!
//! - **error & retries:** Most IO interfaces are NOT allowed to return an error. Instead they shall implement internal
//!   retries (e.g. using [`backoff`]).
//!
//! ### Implementations
//! The implementations of the aforementioned interfaces roughly fall into the following categories:
//!
//! - **true sources/sinks:** These are the actual (mostly object-store and catalog backed) implementations used in
//!   production. There may be multiple variants that are used depending on the config. NO metrics or logging is
//!   implemented directly within these (see "wrappers"  below).
//! - **assemblies:** Assembles components of other types into this type. E.g. a filter for a set of files
//!   (`[file] -> [file]`) can be assembled by a filter for a single file (`file -> bool`).
//! - **mocks:** Mocks can be controlled during initialization or later and may record the interfaction with them. They
//!   are mostly used for testing or development purposes.
//! - **wrappers:** Wrap a component of the same type but add functionality on top. This can be logging, metrics,
//!   assertions, data dumping, filtering, randomness, etc.
//!
//! This modular approach allows easy testing of components as well consistency. E.g. swapping out a data source for
//! another one will preserve the same logs and metrics (given that the same wrappers are used).
//!
//! ## Configuration
//! The [config] allows the compactor to set up a wide range of component setups. The config options fall roughly into
//! the following categories:
//!
//! - **parameters:** Information required to run the compactor, e.g. the object store connection or sharding information.
//! - **tuning knobs:** Options like "maximum desired file size", or "number of concurrent jobs" allow deployments to
//!   fine-tune the compactor behavior. They do NOT fundamentally change the behavior though.
//! - **behavior switches:** Switches like "ignore that partitions where flagged", "run once and exit", or "use this
//!   pre-defined set of partitions" change the behavior of the system. This may be used for emergency situations or to
//!   debug / develop the compactor.
//!
//! [^duplicates_within_files]: The compactor design would not fundamentally change if the ingester would produce output
//!     that contains duplicates within in single file. The current implementation however assumes that this is NOT the case.
//!
//! [^max_parquet_size]: It is technically hard to determine the size of a parquet size before writing it. It may be
//!     estimated by assuming that "sum size of input files >= sum size of output files". However there are certain
//!     cases where this does NOT hold due to the way the data within the output parquet file is split into data pages,
//!     is then potentially dictionary- and RLE-encoded and [ZSTD] compressed.
//!
//!
//! [`AlgoVersion`]: crate::config::AlgoVersion
//! [`Arc`]: std::sync::Arc
//! [components]: crate::components
//! [config]: crate::config
//! [`Debug`]: std::fmt::Debug
//! [`Display`]: std::fmt::Display
//! [driver]: crate::driver
//! [ZSTD]: https://github.com/facebook/zstd
#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]
#![allow(rustdoc::private_intra_doc_links)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

pub mod compactor;
mod components;
pub mod config;
mod driver;
mod error;
mod file_classification;
pub mod object_store;
mod partition_info;
mod plan_ir;
mod round_info;

// publically expose items needed for testing
pub use components::{
    df_planner::panic::PanicDataFusionPlanner, hardcoded::hardcoded_components,
    namespaces_source::mock::NamespaceWrapper, parquet_files_sink::ParquetFilesSink, Components,
};
pub use driver::compact;
pub use error::DynError;
pub use partition_info::PartitionInfo;
pub use plan_ir::PlanIR;
pub use round_info::RoundInfo;

#[cfg(test)]
mod test_utils;

pub mod file_group;
