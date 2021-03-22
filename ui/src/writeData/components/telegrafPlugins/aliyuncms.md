# Alibaba (aka Aliyun) CloudMonitor Service Statistics Input
Here and after we use `Aliyun` instead `Alibaba` as it is default naming across web console and docs.

This plugin will pull Metric Statistics from Aliyun CMS.

### Aliyun Authentication

This plugin uses an [AccessKey](https://www.alibabacloud.com/help/doc-detail/53045.htm?spm=a2c63.p38356.b99.127.5cba21fdt5MJKr&parentId=28572) credential for Authentication with the Aliyun OpenAPI endpoint.
In the following order the plugin will attempt to authenticate.
1. Ram RoleARN credential if `access_key_id`, `access_key_secret`, `role_arn`, `role_session_name` is specified
2. AccessKey STS token credential if `access_key_id`, `access_key_secret`, `access_key_sts_token` is specified
3. AccessKey credential if `access_key_id`, `access_key_secret` is specified
4. Ecs Ram Role Credential if `role_name` is specified
5. RSA keypair credential if `private_key`, `public_key_id` is specified
6. Environment variables credential
7. Instance metadata credential

### Configuration:

```toml
  ## Aliyun Credentials
  ## Credentials are loaded in the following order
  ## 1) Ram RoleArn credential
  ## 2) AccessKey STS token credential
  ## 3) AccessKey credential
  ## 4) Ecs Ram Role credential
  ## 5) RSA keypair credential
  ## 6) Environment variables credential
  ## 7) Instance metadata credential
  
  # access_key_id = ""
  # access_key_secret = ""
  # access_key_sts_token = ""
  # role_arn = ""
  # role_session_name = ""
  # private_key = ""
  # public_key_id = ""
  # role_name = ""
  
  # The minimum period for AliyunCMS metrics is 1 minute (60s). However not all
  # metrics are made available to the 1 minute period. Some are collected at
  # 3 minute, 5 minute, or larger intervals.
  # See: https://help.aliyun.com/document_detail/51936.html?spm=a2c4g.11186623.2.18.2bc1750eeOw1Pv
  # Note that if a period is configured that is smaller than the minimum for a
  # particular metric, that metric will not be returned by the Aliyun OpenAPI
  # and will not be collected by Telegraf.
  #
  ## Requested AliyunCMS aggregation Period (required - must be a multiple of 60s)
  period = "5m"
  
  ## Collection Delay (required - must account for metrics availability via AliyunCMS API)
  delay = "1m"
  
  ## Recommended: use metric 'interval' that is a multiple of 'period' to avoid
  ## gaps or overlap in pulled data
  interval = "5m"
  
  ## Metric Statistic Project (required)
  project = "acs_slb_dashboard"
  
  ## Maximum requests per second, default value is 200
  ratelimit = 200
  
  ## Discovery regions set the scope for object discovery, the discovered info can be used to enrich
  ## the metrics with objects attributes/tags. Discovery is supported not for all projects (if not supported, then 
  ## it will be reported on the start - foo example for 'acs_cdn' project:
  ## 'E! [inputs.aliyuncms] Discovery tool is not activated: no discovery support for project "acs_cdn"' )
  ## Currently, discovery supported for the following projects:
  ## - acs_ecs_dashboard
  ## - acs_rds_dashboard
  ## - acs_slb_dashboard
  ## - acs_vpc_eip
  ##
  ## If not set, all regions would be covered, it can provide a significant load on API, so the recommendation here
  ## is to limit the list as much as possible. Allowed values: https://www.alibabacloud.com/help/zh/doc-detail/40654.htm
  discovery_regions = ["cn-hongkong"]
  
  ## how often the discovery API call executed (default 1m)
  #discovery_interval = "1m"
  
  ## Metrics to Pull (Required)
  [[inputs.aliyuncms.metrics]]
  ## Metrics names to be requested, 
  ## described here (per project): https://help.aliyun.com/document_detail/28619.html?spm=a2c4g.11186623.6.690.1938ad41wg8QSq
  names = ["InstanceActiveConnection", "InstanceNewConnection"]
  
  ## Dimension filters for Metric (these are optional).
  ## This allows to get additional metric dimension. If dimension is not specified it can be returned or
  ## the data can be aggregated - it depends on particular metric, you can find details here: https://help.aliyun.com/document_detail/28619.html?spm=a2c4g.11186623.6.690.1938ad41wg8QSq
  ##
  ## Note, that by default dimension filter includes the list of discovered objects in scope (if discovery is enabled)
  ## Values specified here would be added into the list of discovered objects.
  ## You can specify either single dimension:      
  #dimensions = '{"instanceId": "p-example"}'
  
  ## Or you can specify several dimensions at once:
  #dimensions = '[{"instanceId": "p-example"},{"instanceId": "q-example"}]'
  
  ## Enrichment tags, can be added from discovery (if supported)
  ## Notation is <measurement_tag_name>:<JMES query path (https://jmespath.org/tutorial.html)>
  ## To figure out which fields are available, consult the Describe<ObjectType> API per project.
  ## For example, for SLB: https://api.aliyun.com/#/?product=Slb&version=2014-05-15&api=DescribeLoadBalancers&params={}&tab=MOCK&lang=GO
  #tag_query_path = [
  #    "address:Address",
  #    "name:LoadBalancerName",
  #    "cluster_owner:Tags.Tag[?TagKey=='cs.cluster.name'].TagValue | [0]"
  #    ]
  ## The following tags added by default: regionId (if discovery enabled), userId, instanceId.
  
  ## Allow metrics without discovery data, if discovery is enabled. If set to true, then metric without discovery
  ## data would be emitted, otherwise dropped. This cane be of help, in case debugging dimension filters, or partial coverage 
  ## of discovery scope vs monitoring scope 
  #allow_dps_without_discovery = false
```

#### Requirements and Terminology

Plugin Configuration utilizes [preset metric items references](https://www.alibabacloud.com/help/doc-detail/28619.htm?spm=a2c63.p38356.a3.2.389f233d0kPJn0)

- `discovery_region` must be a valid Aliyun [Region](https://www.alibabacloud.com/help/doc-detail/40654.htm) value
- `period` must be a valid duration value
- `project` must be a preset project value
- `names` must be preset metric names
- `dimensions` must be preset dimension values

### Measurements & Fields:

Each Aliyun CMS Project monitored records a measurement with fields for each available Metric Statistic
Project and Metrics are represented in [snake case](https://en.wikipedia.org/wiki/Snake_case)

- aliyuncms_{project}
  - {metric}_average     (metric Average value)
  - {metric}_minimum     (metric Minimum value)
  - {metric}_maximum     (metric Maximum value)
  - {metric}_value       (metric Value value)

### Example Output:

```
$ ./telegraf --config telegraf.conf --input-filter aliyuncms --test
> aliyuncms_acs_slb_dashboard,instanceId=p-example,regionId=cn-hangzhou,userId=1234567890 latency_average=0.004810798017284538,latency_maximum=0.1100282669067383,latency_minimum=0.0006084442138671875
```