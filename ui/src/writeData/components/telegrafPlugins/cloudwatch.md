# Amazon CloudWatch Statistics Input Plugin

This plugin will pull Metric Statistics from Amazon CloudWatch.

### Amazon Authentication

This plugin uses a credential chain for Authentication with the CloudWatch
API endpoint. In the following order the plugin will attempt to authenticate.
1. Assumed credentials via STS if `role_arn` attribute is specified (source credentials are evaluated from subsequent rules)
2. Explicit credentials from `access_key`, `secret_key`, and `token` attributes
3. Shared profile from `profile` attribute
4. [Environment Variables](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html#environment-variables)
5. [Shared Credentials](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html#shared-credentials-file)
6. [EC2 Instance Profile](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html)

### Configuration:

```toml
[[inputs.cloudwatch]]
  ## Amazon Region
  region = "us-east-1"

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

  # The minimum period for Cloudwatch metrics is 1 minute (60s). However not all
  # metrics are made available to the 1 minute period. Some are collected at
  # 3 minute, 5 minute, or larger intervals. See https://aws.amazon.com/cloudwatch/faqs/#monitoring.
  # Note that if a period is configured that is smaller than the minimum for a
  # particular metric, that metric will not be returned by the Cloudwatch API
  # and will not be collected by Telegraf.
  #
  ## Requested CloudWatch aggregation Period (required - must be a multiple of 60s)
  period = "5m"

  ## Collection Delay (required - must account for metrics availability via CloudWatch API)
  delay = "5m"

  ## Recommended: use metric 'interval' that is a multiple of 'period' to avoid
  ## gaps or overlap in pulled data
  interval = "5m"

  ## Recommended if "delay" and "period" are both within 3 hours of request time. Invalid values will be ignored.
  ## Recently Active feature will only poll for CloudWatch ListMetrics values that occurred within the last 3 Hours.
  ## If enabled, it will reduce total API usage of the CloudWatch ListMetrics API and require less memory to retain.
  ## Do not enable if "period" or "delay" is longer than 3 hours, as it will not return data more than 3 hours old.
  ## See https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_ListMetrics.html
  #recently_active = "PT3H"

  ## Configure the TTL for the internal cache of metrics.
  # cache_ttl = "1h"

  ## Metric Statistic Namespace (required)
  namespace = "AWS/ELB"

  ## Maximum requests per second. Note that the global default AWS rate limit is
  ## 50 reqs/sec, so if you define multiple namespaces, these should add up to a
  ## maximum of 50.
  ## See http://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_limits.html
  # ratelimit = 25

  ## Timeout for http requests made by the cloudwatch client.
  # timeout = "5s"

  ## Namespace-wide statistic filters. These allow fewer queries to be made to
  ## cloudwatch.
  # statistic_include = [ "average", "sum", "minimum", "maximum", sample_count" ]
  # statistic_exclude = []

  ## Metrics to Pull
  ## Defaults to all Metrics in Namespace if nothing is provided
  ## Refreshes Namespace available metrics every 1h
  #[[inputs.cloudwatch.metrics]]
  #  names = ["Latency", "RequestCount"]
  #
  #  ## Statistic filters for Metric.  These allow for retrieving specific
  #  ## statistics for an individual metric.
  #  # statistic_include = [ "average", "sum", "minimum", "maximum", sample_count" ]
  #  # statistic_exclude = []
  #
  #  ## Dimension filters for Metric.  All dimensions defined for the metric names
  #  ## must be specified in order to retrieve the metric statistics.
  #  [[inputs.cloudwatch.metrics.dimensions]]
  #    name = "LoadBalancerName"
  #    value = "p-example"
```
#### Requirements and Terminology

Plugin Configuration utilizes [CloudWatch concepts](http://docs.aws.amazon.com/AmazonCloudWatch/latest/DeveloperGuide/cloudwatch_concepts.html) and access pattern to allow monitoring of any CloudWatch Metric.

- `region` must be a valid AWS [Region](http://docs.aws.amazon.com/AmazonCloudWatch/latest/DeveloperGuide/cloudwatch_concepts.html#CloudWatchRegions) value
- `period` must be a valid CloudWatch [Period](http://docs.aws.amazon.com/AmazonCloudWatch/latest/DeveloperGuide/cloudwatch_concepts.html#CloudWatchPeriods) value
- `namespace` must be a valid CloudWatch [Namespace](http://docs.aws.amazon.com/AmazonCloudWatch/latest/DeveloperGuide/cloudwatch_concepts.html#Namespace) value
- `names` must be valid CloudWatch [Metric](http://docs.aws.amazon.com/AmazonCloudWatch/latest/DeveloperGuide/cloudwatch_concepts.html#Metric) names
- `dimensions` must be valid CloudWatch [Dimension](http://docs.aws.amazon.com/AmazonCloudWatch/latest/DeveloperGuide/cloudwatch_concepts.html#Dimension) name/value pairs

Omitting or specifying a value of `'*'` for a dimension value configures all available metrics that contain a dimension with the specified name
to be retrieved. If specifying >1 dimension, then the metric must contain *all* the configured dimensions where the the value of the
wildcard dimension is ignored.

Example:
```
[[inputs.cloudwatch]]
  period = "1m"
  interval = "5m"

  [[inputs.cloudwatch.metrics]]
    names = ["Latency"]

    ## Dimension filters for Metric (optional)
    [[inputs.cloudwatch.metrics.dimensions]]
      name = "LoadBalancerName"
      value = "p-example"

    [[inputs.cloudwatch.metrics.dimensions]]
      name = "AvailabilityZone"
      value = "*"
```

If the following ELBs are available:
- name: `p-example`, availabilityZone: `us-east-1a`
- name: `p-example`, availabilityZone: `us-east-1b`
- name: `q-example`, availabilityZone: `us-east-1a`
- name: `q-example`, availabilityZone: `us-east-1b`


Then 2 metrics will be output:
- name: `p-example`, availabilityZone: `us-east-1a`
- name: `p-example`, availabilityZone: `us-east-1b`

If the `AvailabilityZone` wildcard dimension was omitted, then a single metric (name: `p-example`)
would be exported containing the aggregate values of the ELB across availability zones.

To maximize efficiency and savings, consider making fewer requests by increasing `interval` but keeping `period` at the duration you would like metrics to be reported. The above example will request metrics from Cloudwatch every 5 minutes but will output five metrics timestamped one minute apart.

#### Restrictions and Limitations
- CloudWatch metrics are not available instantly via the CloudWatch API. You should adjust your collection `delay` to account for this lag in metrics availability based on your [monitoring subscription level](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-cloudwatch-new.html)
- CloudWatch API usage incurs cost - see [GetMetricData Pricing](https://aws.amazon.com/cloudwatch/pricing/)

### Measurements & Fields:

Each CloudWatch Namespace monitored records a measurement with fields for each available Metric Statistic.
Namespace and Metrics are represented in [snake case](https://en.wikipedia.org/wiki/Snake_case)

- cloudwatch_{namespace}
  - {metric}_sum         (metric Sum value)
  - {metric}_average     (metric Average value)
  - {metric}_minimum     (metric Minimum value)
  - {metric}_maximum     (metric Maximum value)
  - {metric}_sample_count (metric SampleCount value)


### Tags:
Each measurement is tagged with the following identifiers to uniquely identify the associated metric
Tag Dimension names are represented in [snake case](https://en.wikipedia.org/wiki/Snake_case)

- All measurements have the following tags:
  - region           (CloudWatch Region)
  - {dimension-name} (Cloudwatch Dimension value - one for each metric dimension)

### Troubleshooting:

You can use the aws cli to get a list of available metrics and dimensions:
```
aws cloudwatch list-metrics --namespace AWS/EC2 --region us-east-1
aws cloudwatch list-metrics --namespace AWS/EC2 --region us-east-1 --metric-name CPUCreditBalance
```

If the expected metrics are not returned, you can try getting them manually
for a short period of time:
```
aws cloudwatch get-metric-data \
  --start-time 2018-07-01T00:00:00Z \
  --end-time 2018-07-01T00:15:00Z \
  --metric-data-queries '[
  {
    "Id": "avgCPUCreditBalance",
    "MetricStat": {
      "Metric": {
        "Namespace": "AWS/EC2",
        "MetricName": "CPUCreditBalance",
        "Dimensions": [
          {
            "Name": "InstanceId",
            "Value": "i-deadbeef"
          }
        ]
      },
      "Period": 300,
      "Stat": "Average"
    },
    "Label": "avgCPUCreditBalance"
  }
]'
```

### Example Output:

```
$ ./telegraf --config telegraf.conf --input-filter cloudwatch --test
> cloudwatch_aws_elb,load_balancer_name=p-example,region=us-east-1 latency_average=0.004810798017284538,latency_maximum=0.1100282669067383,latency_minimum=0.0006084442138671875,latency_sample_count=4029,latency_sum=19.382705211639404 1459542420000000000
```
