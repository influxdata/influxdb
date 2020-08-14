# Amazon ECS Input Plugin

Amazon ECS, Fargate compatible, input plugin which uses the Amazon ECS metadata and
stats [v2][task-metadata-endpoint-v2] or [v3][task-metadata-endpoint-v3] API endpoints
to gather stats on running containers in a Task.

The telegraf container must be run in the same Task as the workload it is
inspecting.

This is similar to (and reuses a few pieces of) the [Docker][docker-input]
input plugin, with some ECS specific modifications for AWS metadata and stats
formats.

The amazon-ecs-agent (though it _is_ a container running on the host) is not
present in the metadata/stats endpoints.

### Configuration

```toml
# Read metrics about ECS containers
[[inputs.ecs]]
  ## ECS metadata url.
  ## Metadata v2 API is used if set explicitly. Otherwise,
  ## v3 metadata endpoint API is used if available.
  # endpoint_url = ""

  ## Containers to include and exclude. Globs accepted.
  ## Note that an empty array for both will include all containers
  # container_name_include = []
  # container_name_exclude = []

  ## Container states to include and exclude. Globs accepted.
  ## When empty only containers in the "RUNNING" state will be captured.
  ## Possible values are "NONE", "PULLED", "CREATED", "RUNNING",
  ## "RESOURCES_PROVISIONED", "STOPPED".
  # container_status_include = []
  # container_status_exclude = []

  ## ecs labels to include and exclude as tags.  Globs accepted.
  ## Note that an empty array for both will include all labels as tags
  ecs_label_include = [ "com.amazonaws.ecs.*" ]
  ecs_label_exclude = []

  ## Timeout for queries.
  # timeout = "5s"
```

### Configuration (enforce v2 metadata)

```toml
# Read metrics about ECS containers
[[inputs.ecs]]
  ## ECS metadata url.
  ## Metadata v2 API is used if set explicitly. Otherwise,
  ## v3 metadata endpoint API is used if available.
  endpoint_url = "http://169.254.170.2"

  ## Containers to include and exclude. Globs accepted.
  ## Note that an empty array for both will include all containers
  # container_name_include = []
  # container_name_exclude = []

  ## Container states to include and exclude. Globs accepted.
  ## When empty only containers in the "RUNNING" state will be captured.
  ## Possible values are "NONE", "PULLED", "CREATED", "RUNNING",
  ## "RESOURCES_PROVISIONED", "STOPPED".
  # container_status_include = []
  # container_status_exclude = []

  ## ecs labels to include and exclude as tags.  Globs accepted.
  ## Note that an empty array for both will include all labels as tags
  ecs_label_include = [ "com.amazonaws.ecs.*" ]
  ecs_label_exclude = []

  ## Timeout for queries.
  # timeout = "5s"
```

### Metrics

- ecs_task
  - tags:
    - cluster
    - task_arn
    - family
    - revision
    - id
    - name
  - fields:
    - revision (string)
    - desired_status (string)
    - known_status (string)
    - limit_cpu (float)
    - limit_mem (float)

+ ecs_container_mem
  - tags:
    - cluster
    - task_arn
    - family
    - revision
    - id
    - name
  - fields:
    - container_id
    - active_anon
    - active_file
    - cache
    - hierarchical_memory_limit
    - inactive_anon
    - inactive_file
    - mapped_file
    - pgfault
    - pgmajfault
    - pgpgin
    - pgpgout
    - rss
    - rss_huge
    - total_active_anon
    - total_active_file
    - total_cache
    - total_inactive_anon
    - total_inactive_file
    - total_mapped_file
    - total_pgfault
    - total_pgmajfault
    - total_pgpgin
    - total_pgpgout
    - total_rss
    - total_rss_huge
    - total_unevictable
    - total_writeback
    - unevictable
    - writeback
    - fail_count
    - limit
    - max_usage
    - usage
    - usage_percent

- ecs_container_cpu
  - tags:
    - cluster
    - task_arn
    - family
    - revision
    - id
    - name
    - cpu
  - fields:
    - container_id
    - usage_total
    - usage_in_usermode
    - usage_in_kernelmode
    - usage_system
    - throttling_periods
    - throttling_throttled_periods
    - throttling_throttled_time
    - usage_percent
    - usage_total

+ ecs_container_net
  - tags:
    - cluster
    - task_arn
    - family
    - revision
    - id
    - name
    - network
  - fields:
    - container_id
    - rx_packets
    - rx_dropped
    - rx_bytes
    - rx_errors
    - tx_packets
    - tx_dropped
    - tx_bytes
    - tx_errors

- ecs_container_blkio
  - tags:
    - cluster
    - task_arn
    - family
    - revision
    - id
    - name
    - device
  - fields:
    - container_id
    - io_service_bytes_recursive_async
    - io_service_bytes_recursive_read
    - io_service_bytes_recursive_sync
    - io_service_bytes_recursive_total
    - io_service_bytes_recursive_write
    - io_serviced_recursive_async
    - io_serviced_recursive_read
    - io_serviced_recursive_sync
    - io_serviced_recursive_total
    - io_serviced_recursive_write

+ ecs_container_meta
  - tags:
    - cluster
    - task_arn
    - family
    - revision
    - id
    - name
  - fields:
    - container_id
    - docker_name
    - image
    - image_id
    - desired_status
    - known_status
    - limit_cpu
    - limit_mem
    - created_at
    - started_at
    - type


### Example Output

```
ecs_task,cluster=test,family=nginx,host=c4b301d4a123,revision=2,task_arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a revision="2",desired_status="RUNNING",known_status="RUNNING",limit_cpu=0.5,limit_mem=512 1542641488000000000
ecs_container_mem,cluster=test,com.amazonaws.ecs.cluster=test,com.amazonaws.ecs.container-name=~internal~ecs~pause,com.amazonaws.ecs.task-arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a,com.amazonaws.ecs.task-definition-family=nginx,com.amazonaws.ecs.task-definition-version=2,family=nginx,host=c4b301d4a123,id=e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba,name=~internal~ecs~pause,revision=2,task_arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a active_anon=40960i,active_file=8192i,cache=790528i,pgpgin=1243i,total_pgfault=1298i,total_rss=40960i,limit=1033658368i,max_usage=4825088i,hierarchical_memory_limit=536870912i,rss=40960i,total_active_file=8192i,total_mapped_file=618496i,usage_percent=0.05349543109392212,container_id="e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba",pgfault=1298i,pgmajfault=6i,pgpgout=1040i,total_active_anon=40960i,total_inactive_file=782336i,total_pgpgin=1243i,usage=552960i,inactive_file=782336i,mapped_file=618496i,total_cache=790528i,total_pgpgout=1040i 1542642001000000000
ecs_container_cpu,cluster=test,com.amazonaws.ecs.cluster=test,com.amazonaws.ecs.container-name=~internal~ecs~pause,com.amazonaws.ecs.task-arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a,com.amazonaws.ecs.task-definition-family=nginx,com.amazonaws.ecs.task-definition-version=2,cpu=cpu-total,family=nginx,host=c4b301d4a123,id=e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba,name=~internal~ecs~pause,revision=2,task_arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a usage_in_kernelmode=0i,throttling_throttled_periods=0i,throttling_periods=0i,throttling_throttled_time=0i,container_id="e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba",usage_percent=0,usage_total=26426156i,usage_in_usermode=20000000i,usage_system=2336100000000i 1542642001000000000
ecs_container_cpu,cluster=test,com.amazonaws.ecs.cluster=test,com.amazonaws.ecs.container-name=~internal~ecs~pause,com.amazonaws.ecs.task-arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a,com.amazonaws.ecs.task-definition-family=nginx,com.amazonaws.ecs.task-definition-version=2,cpu=cpu0,family=nginx,host=c4b301d4a123,id=e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba,name=~internal~ecs~pause,revision=2,task_arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a container_id="e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba",usage_total=26426156i 1542642001000000000
ecs_container_net,cluster=test,com.amazonaws.ecs.cluster=test,com.amazonaws.ecs.container-name=~internal~ecs~pause,com.amazonaws.ecs.task-arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a,com.amazonaws.ecs.task-definition-family=nginx,com.amazonaws.ecs.task-definition-version=2,family=nginx,host=c4b301d4a123,id=e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba,name=~internal~ecs~pause,network=eth0,revision=2,task_arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a rx_errors=0i,rx_packets=36i,tx_errors=0i,tx_bytes=648i,container_id="e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba",rx_dropped=0i,rx_bytes=5338i,tx_packets=8i,tx_dropped=0i 1542642001000000000
ecs_container_net,cluster=test,com.amazonaws.ecs.cluster=test,com.amazonaws.ecs.container-name=~internal~ecs~pause,com.amazonaws.ecs.task-arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a,com.amazonaws.ecs.task-definition-family=nginx,com.amazonaws.ecs.task-definition-version=2,family=nginx,host=c4b301d4a123,id=e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba,name=~internal~ecs~pause,network=eth5,revision=2,task_arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a rx_errors=0i,tx_packets=9i,rx_packets=26i,tx_errors=0i,rx_bytes=4641i,tx_dropped=0i,tx_bytes=690i,container_id="e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba",rx_dropped=0i 1542642001000000000
ecs_container_net,cluster=test,com.amazonaws.ecs.cluster=test,com.amazonaws.ecs.container-name=~internal~ecs~pause,com.amazonaws.ecs.task-arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a,com.amazonaws.ecs.task-definition-family=nginx,com.amazonaws.ecs.task-definition-version=2,family=nginx,host=c4b301d4a123,id=e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba,name=~internal~ecs~pause,network=total,revision=2,task_arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a rx_dropped=0i,rx_bytes=9979i,rx_errors=0i,rx_packets=62i,tx_bytes=1338i,container_id="e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba",tx_packets=17i,tx_dropped=0i,tx_errors=0i 1542642001000000000
ecs_container_blkio,cluster=test,com.amazonaws.ecs.cluster=test,com.amazonaws.ecs.container-name=~internal~ecs~pause,com.amazonaws.ecs.task-arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a,com.amazonaws.ecs.task-definition-family=nginx,com.amazonaws.ecs.task-definition-version=2,device=253:1,family=nginx,host=c4b301d4a123,id=e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba,name=~internal~ecs~pause,revision=2,task_arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a io_service_bytes_recursive_sync=790528i,io_service_bytes_recursive_total=790528i,io_serviced_recursive_sync=10i,io_serviced_recursive_write=0i,io_serviced_recursive_async=0i,io_serviced_recursive_total=10i,container_id="e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba",io_service_bytes_recursive_read=790528i,io_service_bytes_recursive_write=0i,io_service_bytes_recursive_async=0i,io_serviced_recursive_read=10i 1542642001000000000
ecs_container_blkio,cluster=test,com.amazonaws.ecs.cluster=test,com.amazonaws.ecs.container-name=~internal~ecs~pause,com.amazonaws.ecs.task-arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a,com.amazonaws.ecs.task-definition-family=nginx,com.amazonaws.ecs.task-definition-version=2,device=253:2,family=nginx,host=c4b301d4a123,id=e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba,name=~internal~ecs~pause,revision=2,task_arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a io_service_bytes_recursive_sync=790528i,io_service_bytes_recursive_total=790528i,io_serviced_recursive_async=0i,io_serviced_recursive_total=10i,container_id="e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba",io_service_bytes_recursive_read=790528i,io_service_bytes_recursive_write=0i,io_service_bytes_recursive_async=0i,io_serviced_recursive_read=10i,io_serviced_recursive_write=0i,io_serviced_recursive_sync=10i 1542642001000000000
ecs_container_blkio,cluster=test,com.amazonaws.ecs.cluster=test,com.amazonaws.ecs.container-name=~internal~ecs~pause,com.amazonaws.ecs.task-arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a,com.amazonaws.ecs.task-definition-family=nginx,com.amazonaws.ecs.task-definition-version=2,device=253:4,family=nginx,host=c4b301d4a123,id=e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba,name=~internal~ecs~pause,revision=2,task_arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a io_service_bytes_recursive_write=0i,io_service_bytes_recursive_sync=790528i,io_service_bytes_recursive_async=0i,io_service_bytes_recursive_total=790528i,io_serviced_recursive_async=0i,container_id="e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba",io_service_bytes_recursive_read=790528i,io_serviced_recursive_read=10i,io_serviced_recursive_write=0i,io_serviced_recursive_sync=10i,io_serviced_recursive_total=10i 1542642001000000000
ecs_container_blkio,cluster=test,com.amazonaws.ecs.cluster=test,com.amazonaws.ecs.container-name=~internal~ecs~pause,com.amazonaws.ecs.task-arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a,com.amazonaws.ecs.task-definition-family=nginx,com.amazonaws.ecs.task-definition-version=2,device=202:26368,family=nginx,host=c4b301d4a123,id=e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba,name=~internal~ecs~pause,revision=2,task_arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a io_serviced_recursive_read=10i,io_serviced_recursive_write=0i,io_serviced_recursive_sync=10i,io_serviced_recursive_async=0i,io_serviced_recursive_total=10i,container_id="e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba",io_service_bytes_recursive_sync=790528i,io_service_bytes_recursive_total=790528i,io_service_bytes_recursive_async=0i,io_service_bytes_recursive_read=790528i,io_service_bytes_recursive_write=0i 1542642001000000000
ecs_container_blkio,cluster=test,com.amazonaws.ecs.cluster=test,com.amazonaws.ecs.container-name=~internal~ecs~pause,com.amazonaws.ecs.task-arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a,com.amazonaws.ecs.task-definition-family=nginx,com.amazonaws.ecs.task-definition-version=2,device=total,family=nginx,host=c4b301d4a123,id=e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba,name=~internal~ecs~pause,revision=2,task_arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a io_serviced_recursive_async=0i,io_serviced_recursive_read=40i,io_serviced_recursive_sync=40i,io_serviced_recursive_write=0i,io_serviced_recursive_total=40i,io_service_bytes_recursive_read=3162112i,io_service_bytes_recursive_write=0i,io_service_bytes_recursive_async=0i,container_id="e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba",io_service_bytes_recursive_sync=3162112i,io_service_bytes_recursive_total=3162112i 1542642001000000000
ecs_container_meta,cluster=test,com.amazonaws.ecs.cluster=test,com.amazonaws.ecs.container-name=~internal~ecs~pause,com.amazonaws.ecs.task-arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a,com.amazonaws.ecs.task-definition-family=nginx,com.amazonaws.ecs.task-definition-version=2,family=nginx,host=c4b301d4a123,id=e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba,name=~internal~ecs~pause,revision=2,task_arn=arn:aws:ecs:aws-region-1:012345678901:task/a1234abc-a0a0-0a01-ab01-0abc012a0a0a limit_mem=0,type="CNI_PAUSE",container_id="e6af031b91deb3136a2b7c42f262ed2ab554e2fe2736998c7d8edf4afe708dba",docker_name="ecs-nginx-2-internalecspause",limit_cpu=0,known_status="RESOURCES_PROVISIONED",image="amazon/amazon-ecs-pause:0.1.0",image_id="",desired_status="RESOURCES_PROVISIONED" 1542642001000000000
```

[docker-input]: /plugins/inputs/docker/README.md
[task-metadata-endpoint-v2]: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-metadata-endpoint-v2.html
[task-metadata-endpoint-v3] https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-metadata-endpoint-v3.html
