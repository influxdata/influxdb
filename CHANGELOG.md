## v2.0.0-alpha.3 [unreleased]

### Features
1. [11809](https://github.com/influxdata/influxdb/pull/11809): Add the ability to name a scraper target
1. [11821](https://github.com/influxdata/influxdb/pull/11821): Display scraper name as the first and only updatable column in scrapers list
1. [11804](https://github.com/influxdata/influxdb/pull/11804): Add the ability to view runs for a task
1. [11824](https://github.com/influxdata/influxdb/pull/11824): Display last completed run for tasks list
1. [11836](https://github.com/influxdata/influxdb/pull/11836): Add the ability to view the logs for a specific task run

### Bug Fixes
1. [11819](https://github.com/influxdata/influxdb/pull/11819): Update the inline edit for resource names to guard for empty strings
1. [11852](https://github.com/influxdata/influxdb/pull/11852): Prevent a new template dashboard from being created on every telegraf config update
1. [11848](https://github.com/influxdata/influxdb/pull/11848): Fix overlapping buttons in the telegrafs verify data step

### UI Improvements
1. [11764](https://github.com/influxdata/influxdb/pull/11764): Move the download telegraf config button to view config overlay
1. [11879](https://github.com/influxdata/influxdb/pull/11879): Combine permissions for user by type

## v2.0.0-alpha.2 [2019-02-07]

### Features
1. [11677](https://github.com/influxdata/influxdb/pull/11677): Add instructions button to view `$INFLUX_TOKEN` setup for telegraf configs
1. [11693](https://github.com/influxdata/influxdb/pull/11693): Save the $INFLUX_TOKEN environmental variable in telegraf configs
1. [11700](https://github.com/influxdata/influxdb/pull/11700): Update Tasks tab on Org page to look like Tasks Page
1. [11740](https://github.com/influxdata/influxdb/pull/11740): Add view button to view the telegraf config toml
1. [11522](https://github.com/influxdata/influxdb/pull/11522): Add plugin information step to allow for config naming and configure one plugin at a time
1. [11758](https://github.com/influxdata/influxdb/pull/11758): Update Dashboards tab on Org page to look like Dashboards Page
1. [11810](https://github.com/influxdata/influxdb/pull/11810): Add tab for template variables under organizations page

## Bug Fixes
1. [11678](https://github.com/influxdata/influxdb/pull/11678): Update the System Telegraf Plugin bundle to include the swap plugin
1. [11722](https://github.com/influxdata/influxdb/pull/11722): Revert behavior allowing users to create authorizations on behalf of another user

### UI Improvements
1. [11683](https://github.com/influxdata/influxdb/pull/11683): Change the wording for the plugin config form button to Done
1. [11689](https://github.com/influxdata/influxdb/pull/11689): Change the wording for the Collectors configure step button to Create and Verify
1. [11697](https://github.com/influxdata/influxdb/pull/11697): Standardize page loading spinner styles
1. [11711](https://github.com/influxdata/influxdb/pull/11711): Show checkbox on Save As button in data explorer
1. [11705](https://github.com/influxdata/influxdb/pull/11705): Make collectors plugins side bar visible in only the configure step
1. [11745](https://github.com/influxdata/influxdb/pull/11745): Swap retention policies on Create bucket page

## v2.0.0-alpha.1 [2019-01-23]

### Release Notes

This is the initial alpha release of InfluxDB 2.0.
