# Introduction / Purpose

The purpose of this document is to explicitly state IOx's assumptions about the local filesystem where it is operating.

Note the distinction between the “local filesystem” and the ObjectStore (which may itself be configured to use a local filesystem)

# Assumptions

## When configured with a cloud service provider backed ObjectStore (e.g. S3, GCP, Azure)
IOx assumes the contents of the local filesystem can potentially change after a process restarts. IOx may use files it finds on the local filesystem after a restart, (e.g. to avoid re-fetching some data from object storage) but their existence is NOT required for correctness, and their contents must be validated (e.g. with a checksum, etc) prior to use. IOx may delete files in its assigned area whose name or contents are invalid.

In other words, IOx does not require the retention of any contents of the local filesystem across process restarts for *durability*, though it may use the filesystem contents for better performance.


The rationale for this assumption is to enable IOx to be operated in a containerized environment. At the time of this writing, it is challenging to ensure consistent and reliable preservation of locally attached volumes across such environments.

## When configured with a local filesystem backed ObjectStore

This configuration is used when running IOx on a "plain old server" operating at the edge as well as for local testing.

In this configuration, IOx assumes the contents of the local file system are preserved at least as long as the life of the IOx instance and that external measures are taken to backup or otherwise manage this.

In other words, unsurprisingly, when using the local filesystem as object storage, the durability of the data in IOx is tied to the durability of the filesystem.
