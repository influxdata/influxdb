# Jolokia2 Input Plugins

The [Jolokia](http://jolokia.org) _agent_ and _proxy_ input plugins collect JMX metrics from an HTTP endpoint using Jolokia's [JSON-over-HTTP protocol](https://jolokia.org/reference/html/protocol.html).

### Configuration:

#### Jolokia Agent Configuration

The `jolokia2_agent` input plugin reads JMX metrics from one or more [Jolokia agent](https://jolokia.org/agent/jvm.html) REST endpoints.

```toml
[[inputs.jolokia2_agent]]
  urls = ["http://agent:8080/jolokia"]

  [[inputs.jolokia2_agent.metric]]
    name  = "jvm_runtime"
    mbean = "java.lang:type=Runtime"
    paths = ["Uptime"]
```

Optionally, specify TLS options for communicating with agents:

```toml
[[inputs.jolokia2_agent]]
  urls = ["https://agent:8080/jolokia"]
  tls_ca   = "/var/private/ca.pem"
  tls_cert = "/var/private/client.pem"
  tls_key  = "/var/private/client-key.pem"
  #insecure_skip_verify = false

  [[inputs.jolokia2_agent.metric]]
    name  = "jvm_runtime"
    mbean = "java.lang:type=Runtime"
    paths = ["Uptime"]
```

#### Jolokia Proxy Configuration

The `jolokia2_proxy` input plugin reads JMX metrics from one or more _targets_ by interacting with a [Jolokia proxy](https://jolokia.org/features/proxy.html) REST endpoint.

```toml
[[inputs.jolokia2_proxy]]
  url = "http://proxy:8080/jolokia"

  #default_target_username = ""
  #default_target_password = ""
  [[inputs.jolokia2_proxy.target]]
    url = "service:jmx:rmi:///jndi/rmi://targethost:9999/jmxrmi"
    # username = ""
    # password = ""

  [[inputs.jolokia2_proxy.metric]]
    name  = "jvm_runtime"
    mbean = "java.lang:type=Runtime"
    paths = ["Uptime"]
```

Optionally, specify TLS options for communicating with proxies:

```toml
[[inputs.jolokia2_proxy]]
  url = "https://proxy:8080/jolokia"

  tls_ca   = "/var/private/ca.pem"
  tls_cert = "/var/private/client.pem"
  tls_key  = "/var/private/client-key.pem"
  #insecure_skip_verify = false

  #default_target_username = ""
  #default_target_password = ""
  [[inputs.jolokia2_proxy.target]]
    url = "service:jmx:rmi:///jndi/rmi://targethost:9999/jmxrmi"
    # username = ""
    # password = ""

  [[inputs.jolokia2_agent.metric]]
    name  = "jvm_runtime"
    mbean = "java.lang:type=Runtime"
    paths = ["Uptime"]
```

#### Jolokia Metric Configuration

Each `metric` declaration generates a Jolokia request to fetch telemetry from a JMX MBean.

| Key            | Required | Description |
|----------------|----------|-------------|
| `mbean`        | yes      | The object name of a JMX MBean. MBean property-key values can contain a wildcard `*`, allowing you to fetch multiple MBeans with one declaration. |
| `paths`        | no       | A list of MBean attributes to read. |
| `tag_keys`     | no       | A list of MBean property-key names to convert into tags. The property-key name becomes the tag name, while the property-key value becomes the tag value. |
| `tag_prefix`   | no       | A string to prepend to the tag names produced by this `metric` declaration. |
| `field_name`   | no       | A string to set as the name of the field produced by this metric; can contain substitutions. |
| `field_prefix` | no       | A string to prepend to the field names produced by this `metric` declaration; can contain substitutions. |

Use `paths` to refine which fields to collect.

```toml
[[inputs.jolokia2_agent.metric]]
  name  = "jvm_memory"
  mbean = "java.lang:type=Memory"
  paths = ["HeapMemoryUsage", "NonHeapMemoryUsage", "ObjectPendingFinalizationCount"]
```

The preceeding `jvm_memory` `metric` declaration produces the following output:

```
jvm_memory HeapMemoryUsage.committed=4294967296,HeapMemoryUsage.init=4294967296,HeapMemoryUsage.max=4294967296,HeapMemoryUsage.used=1750658992,NonHeapMemoryUsage.committed=67350528,NonHeapMemoryUsage.init=2555904,NonHeapMemoryUsage.max=-1,NonHeapMemoryUsage.used=65821352,ObjectPendingFinalizationCount=0 1503762436000000000
```

Use `*` wildcards against `mbean` property-key values to create distinct series by capturing values into `tag_keys`.

```toml
[[inputs.jolokia2_agent.metric]]
  name     = "jvm_garbage_collector"
  mbean    = "java.lang:name=*,type=GarbageCollector"
  paths    = ["CollectionTime", "CollectionCount"]
  tag_keys = ["name"]
```

Since `name=*` matches both `G1 Old Generation` and `G1 Young Generation`, and `name` is used as a tag, the preceeding `jvm_garbage_collector` `metric` declaration produces two metrics.

```
jvm_garbage_collector,name=G1\ Old\ Generation CollectionCount=0,CollectionTime=0 1503762520000000000
jvm_garbage_collector,name=G1\ Young\ Generation CollectionTime=32,CollectionCount=2 1503762520000000000
```

Use `tag_prefix` along with `tag_keys` to add detail to tag names.

```toml
[[inputs.jolokia2_agent.metric]]
  name       = "jvm_memory_pool"
  mbean      = "java.lang:name=*,type=MemoryPool"
  paths      = ["Usage", "PeakUsage", "CollectionUsage"]
  tag_keys   = ["name"]
  tag_prefix = "pool_"
```

The preceeding `jvm_memory_pool` `metric` declaration produces six metrics, each with a distinct `pool_name` tag.

```
jvm_memory_pool,pool_name=Compressed\ Class\ Space PeakUsage.max=1073741824,PeakUsage.committed=3145728,PeakUsage.init=0,Usage.committed=3145728,Usage.init=0,PeakUsage.used=3017976,Usage.max=1073741824,Usage.used=3017976 1503764025000000000
jvm_memory_pool,pool_name=Code\ Cache PeakUsage.init=2555904,PeakUsage.committed=6291456,Usage.committed=6291456,PeakUsage.used=6202752,PeakUsage.max=251658240,Usage.used=6210368,Usage.max=251658240,Usage.init=2555904 1503764025000000000
jvm_memory_pool,pool_name=G1\ Eden\ Space CollectionUsage.max=-1,PeakUsage.committed=56623104,PeakUsage.init=56623104,PeakUsage.used=53477376,Usage.max=-1,Usage.committed=49283072,Usage.used=19922944,CollectionUsage.committed=49283072,CollectionUsage.init=56623104,CollectionUsage.used=0,PeakUsage.max=-1,Usage.init=56623104 1503764025000000000
jvm_memory_pool,pool_name=G1\ Old\ Gen CollectionUsage.max=1073741824,CollectionUsage.committed=0,PeakUsage.max=1073741824,PeakUsage.committed=1017118720,PeakUsage.init=1017118720,PeakUsage.used=137032208,Usage.max=1073741824,CollectionUsage.init=1017118720,Usage.committed=1017118720,Usage.init=1017118720,Usage.used=134708752,CollectionUsage.used=0 1503764025000000000
jvm_memory_pool,pool_name=G1\ Survivor\ Space Usage.max=-1,Usage.init=0,CollectionUsage.max=-1,CollectionUsage.committed=7340032,CollectionUsage.used=7340032,PeakUsage.committed=7340032,Usage.committed=7340032,Usage.used=7340032,CollectionUsage.init=0,PeakUsage.max=-1,PeakUsage.init=0,PeakUsage.used=7340032 1503764025000000000
jvm_memory_pool,pool_name=Metaspace PeakUsage.init=0,PeakUsage.used=21852224,PeakUsage.max=-1,Usage.max=-1,Usage.committed=22282240,Usage.init=0,Usage.used=21852224,PeakUsage.committed=22282240 1503764025000000000
```

Use substitutions to create fields and field prefixes with MBean property-keys captured by wildcards. In the following example, `$1` represents the value of the property-key `name`, and `$2` represents the value of the property-key `topic`.

```toml
[[inputs.jolokia2_agent.metric]]
  name         = "kafka_topic"
  mbean        = "kafka.server:name=*,topic=*,type=BrokerTopicMetrics"
  field_prefix = "$1"
  tag_keys     = ["topic"]
```

The preceeding `kafka_topic` `metric` declaration produces a metric per Kafka topic. The `name` Mbean property-key is used as a field prefix to aid in gathering fields together into the single metric.

```
kafka_topic,topic=my-topic BytesOutPerSec.MeanRate=0,FailedProduceRequestsPerSec.MeanRate=0,BytesOutPerSec.EventType="bytes",BytesRejectedPerSec.Count=0,FailedProduceRequestsPerSec.RateUnit="SECONDS",FailedProduceRequestsPerSec.EventType="requests",MessagesInPerSec.RateUnit="SECONDS",BytesInPerSec.EventType="bytes",BytesOutPerSec.RateUnit="SECONDS",BytesInPerSec.OneMinuteRate=0,FailedFetchRequestsPerSec.EventType="requests",TotalFetchRequestsPerSec.MeanRate=146.301533938701,BytesOutPerSec.FifteenMinuteRate=0,TotalProduceRequestsPerSec.MeanRate=0,BytesRejectedPerSec.FifteenMinuteRate=0,MessagesInPerSec.FiveMinuteRate=0,BytesInPerSec.Count=0,BytesRejectedPerSec.MeanRate=0,FailedFetchRequestsPerSec.MeanRate=0,FailedFetchRequestsPerSec.FiveMinuteRate=0,FailedFetchRequestsPerSec.FifteenMinuteRate=0,FailedProduceRequestsPerSec.Count=0,TotalFetchRequestsPerSec.FifteenMinuteRate=128.59314292334466,TotalFetchRequestsPerSec.OneMinuteRate=126.71551273850747,TotalFetchRequestsPerSec.Count=1353483,TotalProduceRequestsPerSec.FifteenMinuteRate=0,FailedFetchRequestsPerSec.OneMinuteRate=0,FailedFetchRequestsPerSec.Count=0,FailedProduceRequestsPerSec.FifteenMinuteRate=0,TotalFetchRequestsPerSec.FiveMinuteRate=130.8516148751592,TotalFetchRequestsPerSec.RateUnit="SECONDS",BytesRejectedPerSec.RateUnit="SECONDS",BytesInPerSec.MeanRate=0,FailedFetchRequestsPerSec.RateUnit="SECONDS",BytesRejectedPerSec.OneMinuteRate=0,BytesOutPerSec.Count=0,BytesOutPerSec.OneMinuteRate=0,MessagesInPerSec.FifteenMinuteRate=0,MessagesInPerSec.MeanRate=0,BytesInPerSec.FiveMinuteRate=0,TotalProduceRequestsPerSec.RateUnit="SECONDS",FailedProduceRequestsPerSec.OneMinuteRate=0,TotalProduceRequestsPerSec.EventType="requests",BytesRejectedPerSec.FiveMinuteRate=0,BytesRejectedPerSec.EventType="bytes",BytesOutPerSec.FiveMinuteRate=0,FailedProduceRequestsPerSec.FiveMinuteRate=0,MessagesInPerSec.Count=0,TotalProduceRequestsPerSec.FiveMinuteRate=0,TotalProduceRequestsPerSec.OneMinuteRate=0,MessagesInPerSec.EventType="messages",MessagesInPerSec.OneMinuteRate=0,TotalFetchRequestsPerSec.EventType="requests",BytesInPerSec.RateUnit="SECONDS",BytesInPerSec.FifteenMinuteRate=0,TotalProduceRequestsPerSec.Count=0 1503767532000000000
```

Both `jolokia2_agent` and `jolokia2_proxy` plugins support default configurations that apply to every `metric` declaration.

| Key                       | Default Value | Description |
|---------------------------|---------------|-------------|
| `default_field_separator` | `.`           | A character to use to join Mbean attributes when creating fields. |
| `default_field_prefix`    | _None_        | A string to prepend to the field names produced by all `metric` declarations. |
| `default_tag_prefix`      | _None_        | A string to prepend to the tag names produced by all `metric` declarations. |

### Example Configurations:

- [ActiveMQ](/plugins/inputs/jolokia2/examples/activemq.conf)
- [BitBucket](/plugins/inputs/jolokia2/examples/bitbucket.conf)
- [Cassandra](/plugins/inputs/jolokia2/examples/cassandra.conf)
- [Hadoop-HDFS](/plugins/inputs/jolokia2/examples/hadoop-hdfs.conf)
- [Java JVM](/plugins/inputs/jolokia2/examples/java.conf)
- [JBoss](/plugins/inputs/jolokia2/examples/jboss.conf)
- [Kafka](/plugins/inputs/jolokia2/examples/kafka.conf)
- [Tomcat](/plugins/inputs/jolokia2/examples/tomcat.conf)
- [Weblogic](/plugins/inputs/jolokia2/examples/weblogic.conf)
- [ZooKeeper](/plugins/inputs/jolokia2/examples/zookeeper.conf)

Please help improve this list and contribute new configuration files by opening an issue or pull request.
