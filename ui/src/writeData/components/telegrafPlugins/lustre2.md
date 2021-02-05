# Lustre Input Plugin

The [Lustre][]® file system is an open-source, parallel file system that supports
many requirements of leadership class HPC simulation environments.

This plugin monitors the Lustre file system using its entries in the proc filesystem.

### Configuration

```toml
# Read metrics from local Lustre service on OST, MDS
[[inputs.lustre2]]
  ## An array of /proc globs to search for Lustre stats
  ## If not specified, the default will work on Lustre 2.5.x
  ##
  # ost_procfiles = [
  #   "/proc/fs/lustre/obdfilter/*/stats",
  #   "/proc/fs/lustre/osd-ldiskfs/*/stats",
  #   "/proc/fs/lustre/obdfilter/*/job_stats",
  # ]
  # mds_procfiles = [
  #   "/proc/fs/lustre/mdt/*/md_stats",
  #   "/proc/fs/lustre/mdt/*/job_stats",
  # ]
```

### Metrics

From `/proc/fs/lustre/obdfilter/*/stats` and `/proc/fs/lustre/osd-ldiskfs/*/stats`:

- lustre2
  - tags:
    - name
  - fields:
    - write_bytes
    - write_calls
    - read_bytes
    - read_calls
    - cache_hit
    - cache_miss
    - cache_access

From `/proc/fs/lustre/obdfilter/*/job_stats`:

- lustre2
  - tags:
    - name
    - jobid
  - fields:
    - jobstats_ost_getattr
    - jobstats_ost_setattr
    - jobstats_ost_sync
    - jobstats_punch
    - jobstats_destroy
    - jobstats_create
    - jobstats_ost_statfs
    - jobstats_get_info
    - jobstats_set_info
    - jobstats_quotactl
    - jobstats_read_bytes
    - jobstats_read_calls
    - jobstats_read_max_size
    - jobstats_read_min_size
    - jobstats_write_bytes
    - jobstats_write_calls
    - jobstats_write_max_size
    - jobstats_write_min_size

From `/proc/fs/lustre/mdt/*/md_stats`:

- lustre2
  - tags:
    - name
  - fields:
    - open
    - close
    - mknod
    - link
    - unlink
    - mkdir
    - rmdir
    - rename
    - getattr
    - setattr
    - getxattr
    - setxattr
    - statfs
    - sync
    - samedir_rename
    - crossdir_rename

From `/proc/fs/lustre/mdt/*/job_stats`:

- lustre2
  - tags:
    - name
    - jobid
  - fields:
    - jobstats_close
    - jobstats_crossdir_rename
    - jobstats_getattr
    - jobstats_getxattr
    - jobstats_link
    - jobstats_mkdir
    - jobstats_mknod
    - jobstats_open
    - jobstats_rename
    - jobstats_rmdir
    - jobstats_samedir_rename
    - jobstats_setattr
    - jobstats_setxattr
    - jobstats_statfs
    - jobstats_sync
    - jobstats_unlink


### Troubleshooting

Check for the default or custom procfiles in the proc filesystem, and reference
the [Lustre Monitoring and Statistics Guide][guide].  This plugin does not
report all information from these files, only a limited set of items
corresponding to the above metric fields.

### Example Output

```
lustre2,host=oss2,jobid=42990218,name=wrk-OST0041 jobstats_ost_setattr=0i,jobstats_ost_sync=0i,jobstats_punch=0i,jobstats_read_bytes=4096i,jobstats_read_calls=1i,jobstats_read_max_size=4096i,jobstats_read_min_size=4096i,jobstats_write_bytes=310206488i,jobstats_write_calls=7423i,jobstats_write_max_size=53048i,jobstats_write_min_size=8820i 1556525847000000000
lustre2,host=mds1,jobid=42992017,name=wrk-MDT0000 jobstats_close=31798i,jobstats_crossdir_rename=0i,jobstats_getattr=34146i,jobstats_getxattr=15i,jobstats_link=0i,jobstats_mkdir=658i,jobstats_mknod=0i,jobstats_open=31797i,jobstats_rename=0i,jobstats_rmdir=0i,jobstats_samedir_rename=0i,jobstats_setattr=1788i,jobstats_setxattr=0i,jobstats_statfs=0i,jobstats_sync=0i,jobstats_unlink=0i 1556525828000000000

```

[lustre]: http://lustre.org/
[guide]: http://wiki.lustre.org/Lustre_Monitoring_and_Statistics_Guide
