# Event Hub Consumer Input Plugin

This plugin provides a consumer for use with Azure Event Hubs and Azure IoT Hub.

### IoT Hub Setup

The main focus for development of this plugin is Azure IoT hub:

1. Create an Azure IoT Hub by following any of the guides provided here: https://docs.microsoft.com/en-us/azure/iot-hub/
2. Create a device, for example a [simulated Raspberry Pi](https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-raspberry-pi-web-simulator-get-started)
3. The connection string needed for the plugin is located under *Shared access policies*, both the *iothubowner* and *service* policies should work

### Configuration

```toml
[[inputs.eventhub_consumer]]
  ## The default behavior is to create a new Event Hub client from environment variables.
  ## This requires one of the following sets of environment variables to be set:
  ##
  ## 1) Expected Environment Variables:
  ##    - "EVENTHUB_NAMESPACE"
  ##    - "EVENTHUB_NAME"
  ##    - "EVENTHUB_CONNECTION_STRING"
  ##
  ## 2) Expected Environment Variables:
  ##    - "EVENTHUB_NAMESPACE"
  ##    - "EVENTHUB_NAME"
  ##    - "EVENTHUB_KEY_NAME"
  ##    - "EVENTHUB_KEY_VALUE"

  ## Uncommenting the option below will create an Event Hub client based solely on the connection string.
  ## This can either be the associated environment variable or hard coded directly.
  # connection_string = ""

  ## Set persistence directory to a valid folder to use a file persister instead of an in-memory persister
  # persistence_dir = ""

  ## Change the default consumer group
  # consumer_group = ""

  ## By default the event hub receives all messages present on the broker, alternative modes can be set below.
  ## The timestamp should be in https://github.com/toml-lang/toml#offset-date-time format (RFC 3339).
  ## The 3 options below only apply if no valid persister is read from memory or file (e.g. first run).
  # from_timestamp =
  # latest = true

  ## Set a custom prefetch count for the receiver(s)
  # prefetch_count = 1000

  ## Add an epoch to the receiver(s)
  # epoch = 0

  ## Change to set a custom user agent, "telegraf" is used by default
  # user_agent = "telegraf"

  ## To consume from a specific partition, set the partition_ids option.
  ## An empty array will result in receiving from all partitions.
  # partition_ids = ["0","1"]

  ## Max undelivered messages
  # max_undelivered_messages = 1000

  ## Set either option below to true to use a system property as timestamp.
  ## You have the choice between EnqueuedTime and IoTHubEnqueuedTime.
  ## It is recommended to use this setting when the data itself has no timestamp.
  # enqueued_time_as_ts = true
  # iot_hub_enqueued_time_as_ts = true

  ## Tags or fields to create from keys present in the application property bag.
  ## These could for example be set by message enrichments in Azure IoT Hub.
  # application_property_tags = []
  # application_property_fields = []

  ## Tag or field name to use for metadata
  ## By default all metadata is disabled
  # sequence_number_field = "SequenceNumber"
  # enqueued_time_field = "EnqueuedTime"
  # offset_field = "Offset"
  # partition_id_tag = "PartitionID"
  # partition_key_tag = "PartitionKey"
  # iot_hub_device_connection_id_tag = "IoTHubDeviceConnectionID"
  # iot_hub_auth_generation_id_tag = "IoTHubAuthGenerationID"
  # iot_hub_connection_auth_method_tag = "IoTHubConnectionAuthMethod"
  # iot_hub_connection_module_id_tag = "IoTHubConnectionModuleID"
  # iot_hub_enqueued_time_field = "IoTHubEnqueuedTime"

  ## Data format to consume.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"
```

#### Environment Variables

[Full documentation of the available environment variables][envvar].

[envvar]: https://github.com/Azure/azure-event-hubs-go#environment-variables
