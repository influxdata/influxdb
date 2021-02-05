# Kinesis Consumer Input Plugin

The [Kinesis][kinesis] consumer plugin reads from a Kinesis data stream
and creates metrics using one of the supported [input data formats][].


### Configuration

```toml
[[inputs.kinesis_consumer]]
  ## Amazon REGION of kinesis endpoint.
  region = "ap-southeast-2"

  ## Amazon Credentials
  ## Credentials are loaded in the following order
  ## 1) Assumed credentials via STS if role_arn is specified
  ## 2) explicit credentials from 'access_key' and 'secret_key'
  ## 3) shared profile from 'profile'
  ## 4) environment variables
  ## 5) shared credentials file
  ## 6) EC2 Instance Profile
  # access_key = ""
  # secret_key = ""
  # token = ""
  # role_arn = ""
  # profile = ""
  # shared_credential_file = ""

  ## Endpoint to make request against, the correct endpoint is automatically
  ## determined and this option should only be set if you wish to override the
  ## default.
  ##   ex: endpoint_url = "http://localhost:8000"
  # endpoint_url = ""

  ## Kinesis StreamName must exist prior to starting telegraf.
  streamname = "StreamName"

  ## Shard iterator type (only 'TRIM_HORIZON' and 'LATEST' currently supported)
  # shard_iterator_type = "TRIM_HORIZON"

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

  ## Optional
  ## Configuration for a dynamodb checkpoint
  [inputs.kinesis_consumer.checkpoint_dynamodb]
    ## unique name for this consumer
    app_name = "default"
    table_name = "default"
```


#### Required AWS IAM permissions

Kinesis:
 - DescribeStream
 - GetRecords
 - GetShardIterator

DynamoDB:
 - GetItem
 - PutItem


#### DynamoDB Checkpoint

The DynamoDB checkpoint stores the last processed record in a DynamoDB. To leverage
this functionality, create a table with the following string type keys:

```
Partition key: namespace
Sort key: shard_id
```


[kinesis]: https://aws.amazon.com/kinesis/
[input data formats]: /docs/DATA_FORMATS_INPUT.md
