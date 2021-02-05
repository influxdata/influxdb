# Beanstalkd Input Plugin

The `beanstalkd` plugin collects server stats as well as tube stats (reported by `stats` and `stats-tube` commands respectively).

### Configuration:

```toml
[[inputs.beanstalkd]]
  ## Server to collect data from
  server = "localhost:11300"

  ## List of tubes to gather stats about.
  ## If no tubes specified then data gathered for each tube on server reported by list-tubes command
  tubes = ["notifications"]
```

### Metrics:

Please see the [Beanstalk Protocol doc](https://raw.githubusercontent.com/kr/beanstalkd/master/doc/protocol.txt) for detailed explanation of `stats` and `stats-tube` commands output.

`beanstalkd_overview` – statistical information about the system as a whole
- fields
  - cmd_delete
  - cmd_pause_tube
  - current_jobs_buried
  - current_jobs_delayed
  - current_jobs_ready
  - current_jobs_reserved
  - current_jobs_urgent
  - current_using
  - current_waiting
  - current_watching
  - pause
  - pause_time_left
  - total_jobs
- tags
  - name
  - server (address taken from config)

`beanstalkd_tube` – statistical information about the specified tube
- fields
  - binlog_current_index
  - binlog_max_size
  - binlog_oldest_index
  - binlog_records_migrated
  - binlog_records_written
  - cmd_bury
  - cmd_delete
  - cmd_ignore
  - cmd_kick
  - cmd_list_tube_used
  - cmd_list_tubes
  - cmd_list_tubes_watched
  - cmd_pause_tube
  - cmd_peek
  - cmd_peek_buried
  - cmd_peek_delayed
  - cmd_peek_ready
  - cmd_put
  - cmd_release
  - cmd_reserve
  - cmd_reserve_with_timeout
  - cmd_stats
  - cmd_stats_job
  - cmd_stats_tube
  - cmd_touch
  - cmd_use
  - cmd_watch
  - current_connections
  - current_jobs_buried
  - current_jobs_delayed
  - current_jobs_ready
  - current_jobs_reserved
  - current_jobs_urgent
  - current_producers
  - current_tubes
  - current_waiting
  - current_workers
  - job_timeouts
  - max_job_size
  - pid
  - rusage_stime
  - rusage_utime
  - total_connections
  - total_jobs
  - uptime
- tags
  - hostname
  - id
  - server (address taken from config)
  - version

### Example Output:
```
beanstalkd_overview,host=server.local,hostname=a2ab22ed12e0,id=232485800aa11b24,server=localhost:11300,version=1.10 cmd_stats_tube=29482i,current_jobs_delayed=0i,current_jobs_urgent=6i,cmd_kick=0i,cmd_stats=7378i,cmd_stats_job=0i,current_waiting=0i,max_job_size=65535i,pid=6i,cmd_bury=0i,cmd_reserve_with_timeout=0i,cmd_touch=0i,current_connections=1i,current_jobs_ready=6i,current_producers=0i,cmd_delete=0i,cmd_list_tubes=7369i,cmd_peek_ready=0i,cmd_put=6i,cmd_use=3i,cmd_watch=0i,current_jobs_reserved=0i,rusage_stime=6.07,cmd_list_tubes_watched=0i,cmd_pause_tube=0i,total_jobs=6i,binlog_records_migrated=0i,cmd_list_tube_used=0i,cmd_peek_delayed=0i,cmd_release=0i,current_jobs_buried=0i,job_timeouts=0i,binlog_current_index=0i,binlog_max_size=10485760i,total_connections=7378i,cmd_peek_buried=0i,cmd_reserve=0i,current_tubes=4i,binlog_records_written=0i,cmd_peek=0i,rusage_utime=1.13,uptime=7099i,binlog_oldest_index=0i,current_workers=0i,cmd_ignore=0i 1528801650000000000

beanstalkd_tube,host=server.local,name=notifications,server=localhost:11300 pause_time_left=0i,current_jobs_buried=0i,current_jobs_delayed=0i,current_jobs_reserved=0i,current_using=0i,current_waiting=0i,pause=0i,total_jobs=3i,cmd_delete=0i,cmd_pause_tube=0i,current_jobs_ready=3i,current_jobs_urgent=3i,current_watching=0i 1528801650000000000
```
