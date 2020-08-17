# NSQ Consumer Input Plugin

The [NSQ][nsq] consumer plugin reads from NSQD and creates metrics using one
of the supported [input data formats][].

### Configuration:

```toml
# Read metrics from NSQD topic(s)
[[inputs.nsq_consumer]]
  ## Server option still works but is deprecated, we just prepend it to the nsqd array.
  # server = "localhost:4150"

  ## An array representing the NSQD TCP HTTP Endpoints
  nsqd = ["localhost:4150"]

  ## An array representing the NSQLookupd HTTP Endpoints
  nsqlookupd = ["localhost:4161"]
  topic = "telegraf"
  channel = "consumer"
  max_in_flight = 100

  ## Maximum messages to read from the broker that have not been written by an
  ## output.  For best throughput set based on the number of metrics within
  ## each message and the size of the output's metric_batch_size.
  ##
  ## For example, if each message from the queue contains 10 metrics and the
  ## output metric_batch_size is 1000, setting this to 100 will ensure that a
  ## full batch is collected and the write is triggered immediately without
  ## waiting until the next flush_interval.
  # max_undelivered_messages = 1000

  ## Data format to consume.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"
```

[nsq]: https://nsq.io
[input data formats]: /docs/DATA_FORMATS_INPUT.md
