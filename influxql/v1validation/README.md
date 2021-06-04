This package asserts the fully compatibility between the InfluxDB v1 Influxql engine
and IDPE influxql.

There is only one file that runs as subtest all the goldenfiles located in
`./goldenfiles`

This is an example of goldenfile:

```yaml
query: "select host, inactive from mem where time >=30000000000 AND time < 50000000000"
dataset: |
  mem,host=gianarb page_tables=39534592i,vmalloc_chunk=0i,write_back_tmp=0i,dirty=884736i,high_total=0i,available=11992494080i,used=3284553728i,active=7172775936i,huge_pages_free=0i,swap_total=8589930496i,vmalloc_used=38604800i,free=4928421888i,commit_limit=16853958656i,committed_as=12584218624i,mapped=939278336i,vmalloc_total=35184372087808i,write_back=0i,buffered=989163520i,wired=0i,low_free=0i,huge_page_size=2097152i,swap_cached=120016896i,swap_free=8445227008i,inactive=3461185536i,slab=542363648i,high_free=0i,shared=903233536i,sreclaimable=449650688i,total=16528056320i,cached=7325917184i,available_percent=72.55840522208482,sunreclaim=92712960i,used_percent=19.87259520664557,huge_pages_total=0i,low_total=0i 0
  mem,host=gianarb slab=542392320i,shared=931356672i,vmalloc_used=38506496i,sunreclaim=92692480i,write_back=0i,total=16528056320i,commit_limit=16853958656i,huge_pages_free=0i,vmalloc_chunk=0i,active=7159382016i,huge_page_size=2097152i,swap_total=8589930496i,used=3266457600i,buffered=989843456i,wired=0i,high_free=0i,page_tables=38965248i,swap_cached=120016896i,write_back_tmp=0i,inactive=3454332928i,used_percent=19.7631078740177,committed_as=12570415104i,dirty=339968i,high_total=0i,huge_pages_total=0i,sreclaimable=449699840i,vmalloc_total=35184372087808i,free=4914728960i,cached=7357026304i,available_percent=72.49944859820032,low_free=0i,low_total=0i,swap_free=8445227008i,available=11982749696i,mapped=938016768i 20000000000
  cpu,cpu=cpu-total,host=gianarb usage_user=6.102117061021656,usage_system=2.938978829389874,usage_nice=0,usage_irq=1.0709838107097638,usage_guest=0,usage_idle=89.41469489414925,usage_iowait=0.024906600249037934,usage_softirq=0.4483188044832491,usage_steal=0,usage_guest_nice=0 30000000000
  cpu,cpu=cpu3,host=gianarb usage_user=5.711422845699912,usage_nice=0,usage_iowait=0,usage_irq=1.302605210421328,usage_steal=0,usage_guest=0,usage_guest_nice=0,usage_system=2.905811623247315,usage_idle=89.67935871744233,usage_softirq=0.4008016032064967 30000000000
  cpu,cpu=cpu2,host=gianarb usage_user=5.982053838488868,usage_idle=89.73080757727861,usage_irq=0.8973080757725367,usage_softirq=0.4985044865405501,usage_steal=0,usage_guest=0,usage_system=2.8913260219364374,usage_nice=0,usage_iowait=0,usage_guest_nice=0 30000000000
  cpu,cpu=cpu1,host=gianarb usage_iowait=0,usage_irq=1.0912698412698993,usage_softirq=0.4960317460319519,usage_guest_nice=0,usage_steal=0,usage_guest=0,usage_user=6.051587301585189,usage_system=3.075396825396861,usage_idle=89.28571428573105,usage_nice=0 30000000000
  cpu,cpu=cpu0,host=gianarb usage_user=6.560636182902656,usage_system=2.9821073558650495,usage_idle=88.86679920477891,usage_irq=1.0934393638174862,usage_steal=0,usage_nice=0,usage_iowait=0,usage_softirq=0.4970178926437982,usage_guest=0,usage_guest_nice=0 30000000000
  mem,host=gianarb used=3279171584i,wired=0i,committed_as=12718530560i,huge_pages_total=0i,active=7167565824i,swap_total=8589930496i,vmalloc_total=35184372087808i,available=11979239424i,cached=7348174848i,buffered=989859840i,commit_limit=16853958656i,low_free=0i,vmalloc_used=38834176i,total=16528056320i,inactive=3460194304i,available_percent=72.47821033562403,high_free=0i,huge_pages_free=0i,slab=542396416i,dirty=487424i,page_tables=39669760i,swap_free=8445227008i,vmalloc_chunk=0i,write_back=0i,free=4910850048i,huge_page_size=2097152i,low_total=0i,sunreclaim=92696576i,write_back_tmp=0i,used_percent=19.84003152283547,high_total=0i,mapped=938799104i,shared=922521600i,sreclaimable=449699840i,swap_cached=120016896i 30000000000
  cpu,cpu=cpu-total,host=gianarb usage_guest_nice=0,usage_user=5.682102628286863,usage_idle=89.88735919901404,usage_nice=0,usage_iowait=0.10012515644554432,usage_guest=0,usage_system=2.828535669587463,usage_irq=1.076345431789406,usage_softirq=0.4255319148935456,usage_steal=0 40000000000
  cpu,cpu=cpu3,host=gianarb usage_user=5.599999999992869,usage_steal=0,usage_guest_nice=0,usage_softirq=0.4999999999999318,usage_guest=0,usage_system=2.9999999999984537,usage_idle=89.39999999999075,usage_nice=0,usage_iowait=0.19999999999995852,usage_irq=1.2999999999994816 40000000000
result: |
  name: mem
  time        host    inactive
  ----        ----    --------
  30000000000 gianarb 3460194304
  40000000000 gianarb 3454791680
```

There are only 3 required fields:

* `query`: to specify the INFLUXQL query to run against the dataset.
* `dataset` are the data used for the query.
* `result` is the expected result returned from the query against the dataset.

## Add a new testcase

In order to `add a new test` you can create a new file with your dataset, your
query and your expected result.

## Example of dataset creation

You can use telegraf to create your own dataset. The one you see above comes
from this configuration:

```toml
[agent]
  interval = "10s"
  round_interval = true
  metric_batch_size = 1000
  metric_buffer_limit = 10000
  collection_jitter = "0s"
  flush_interval = "10s"
  flush_jitter = "0s"
  precision = ""
  hostname = ""
  omit_hostname = false

[[outputs.file]]
   files = ["stdout", "./metrics.out"]
   data_format = "influx"

[[inputs.mem]]
[[inputs.cpu]]
```

Configure Telegraf to collect metrics using the `mem` and `cpu` input plugins.
Additionally, configure telegraf to use the `file` plugin for output, which will
write data as InfluxDB line protocol to `metrics.out`. `metrics.out` will become
the golden file used for the unit test.

Timestamps within `metrics.out` may be normalized using the
`./cmd/goldenfilenormalizer` tool, which adjusts the first timestamp to a
specified base time and all later times relative to that. This is not mandatory,
but may simplify certain test cases, if start and end times are well defined.
