# Salesforce Input Plugin

The Salesforce plugin gathers metrics about the limits in your Salesforce organization and the remaining usage.
It fetches its data from the [limits endpoint](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_limits.htm) of Salesforce's REST API.

### Configuration:

```toml
# Gather Metrics about Salesforce limits and remaining usage
[[inputs.salesforce]]
  username = "your_username"
  password = "your_password"
  ## (Optional) security token
  security_token = "your_security_token"
  ## (Optional) environment type (sandbox or production)
  ## default is: production
  # environment = "production"
  ## (Optional) API version (default: "39.0")
  # version = "39.0"
```

### Measurements & Fields:

Salesforce provide one measurement named "salesforce".
Each entry is converted to snake\_case and 2 fields are created.

- \<key\>_max represents the limit threshold
- \<key\>_remaining represents the usage remaining before hitting the limit threshold

- salesforce
    - \<key\>_max (int)
    - \<key\>_remaining (int)
    - (...)

### Tags:

- All measurements have the following tags:
    - host
    - organization_id (t18 char organisation ID)


### Example Output:

```
$./telegraf --config telegraf.conf --input-filter salesforce --test

salesforce,organization_id=XXXXXXXXXXXXXXXXXX,host=xxxxx.salesforce.com daily_workflow_emails_max=546000i,hourly_time_based_workflow_max=50i,daily_async_apex_executions_remaining=250000i,daily_durable_streaming_api_events_remaining=1000000i,streaming_api_concurrent_clients_remaining=2000i,daily_bulk_api_requests_remaining=10000i,hourly_sync_report_runs_remaining=500i,daily_api_requests_max=5000000i,data_storage_mb_remaining=1073i,file_storage_mb_remaining=1069i,daily_generic_streaming_api_events_remaining=10000i,hourly_async_report_runs_remaining=1200i,hourly_time_based_workflow_remaining=50i,daily_streaming_api_events_remaining=1000000i,single_email_max=5000i,hourly_dashboard_refreshes_remaining=200i,streaming_api_concurrent_clients_max=2000i,daily_durable_generic_streaming_api_events_remaining=1000000i,daily_api_requests_remaining=4999998i,hourly_dashboard_results_max=5000i,hourly_async_report_runs_max=1200i,daily_durable_generic_streaming_api_events_max=1000000i,hourly_dashboard_results_remaining=5000i,concurrent_sync_report_runs_max=20i,durable_streaming_api_concurrent_clients_remaining=2000i,daily_workflow_emails_remaining=546000i,hourly_dashboard_refreshes_max=200i,daily_streaming_api_events_max=1000000i,hourly_sync_report_runs_max=500i,hourly_o_data_callout_max=10000i,mass_email_max=5000i,mass_email_remaining=5000i,single_email_remaining=5000i,hourly_dashboard_statuses_max=999999999i,concurrent_async_get_report_instances_max=200i,daily_durable_streaming_api_events_max=1000000i,daily_generic_streaming_api_events_max=10000i,hourly_o_data_callout_remaining=10000i,concurrent_sync_report_runs_remaining=20i,daily_bulk_api_requests_max=10000i,data_storage_mb_max=1073i,hourly_dashboard_statuses_remaining=999999999i,concurrent_async_get_report_instances_remaining=200i,daily_async_apex_executions_max=250000i,durable_streaming_api_concurrent_clients_max=2000i,file_storage_mb_max=1073i 1501565661000000000
```
