# Getting Started with Chronograf

Let's get familiar with some of Chronograf's main features.
In the next sections, we'll show you how Chronograf makes the monitoring and alerting for your infrastructure easy to configure and maintain.

If you haven't installed Chronograf check out the [Installation Guide](https://github.com/influxdata/chronograf/blob/master/docs/INSTALLATION.md).

## Host List

The `HOST LIST` page is essentially Chronograf's home page.
It lists every host that is sending [Telegraf](https://github.com/influxdata/telegraf) data to your [InfluxDB](https://github.com/influxdata/influxdb) instance as well a some information about each host's CPU usage, load, and configured apps.

![Host List](https://github.com/influxdata/chronograf/blob/master/docs/images/host-list-gs.png)

The Chronograf instance shown above is connected to two hosts (`telegraf-narnia` and `telegraf-neverland`).
The first host is using 0.35%	of its total CPU and has a load of 0.00.
It has one configured app: `system`.
Apps are Telegraf [input plugins](https://github.com/influxdata/telegraf#input-plugins) that have dashboard templates in Chronograf.

Click on the app on the `HOST LIST` page to access its dashboard template.
The dashboard offers [pre-created](https://github.com/influxdata/chronograf/tree/master/canned) graphs of the input's data that are currently in InfluxDB.
Here's the dashboard template for Telegraf's [system stats](https://github.com/influxdata/telegraf/tree/master/plugins/inputs/system) input plugin:

![System Graph Layout](https://github.com/influxdata/chronograf/blob/master/docs/images/system-layout-gs.gif)

Hover over the graphs to get additional information about the data.
In addition, select alternative refresh intervals, alternative time ranges, and enter presentation mode with the icons in the top right corner.

See the [README](https://github.com/influxdata/chronograf#dashboard-templates) for a complete list of the apps supported by Chronograf.

## Data Explorer

Chronograf's Data Explorer gives you the tools to dig in and create personalized visualizations of your data.

### Generate Visualizations with the Query Builder

Use the query builder to easily generate [InfluxQL](https://docs.influxdata.com/influxdb/latest/query_language/) queries and create beautiful visualizations:

![Data Exploration](https://github.com/influxdata/chronograf/blob/master/docs/images/data-exploration-gs.gif)

### Generate Visualizations with the Raw Query Editor

Paste an existing [InfluxQL](https://docs.influxdata.com/influxdb/latest/query_language/) query or write a query from scratch with the `InfluxQL` editor:

![Raw Editor](https://github.com/influxdata/chronograf/blob/master/docs/images/raw-editor-gs.gif)

### Other Features
Select an alternative refresh interval (1), an alternative time range (2), and view query results in tabular format (3):

![Data Exploration Extras](https://github.com/influxdata/chronograf/blob/master/docs/images/data-exploration-extras-gs.png)

## Create and View Alerts

Chronograf also offers a UI for generating [Kapacitor](https://github.com/influxdata/kapacitor) alerting rules and viewing those alerts as they occur.

### Create an Alert Rule
Easily create a Kapacitor alert rule on the `KAPACITOR RULES` page.
Access the `KAPACITOR RULES` page by hovering over the third item in the left navigation menu and selecting `Kapacitor Rules`.
Then, click on the `Create New Rule` button to create a new alert rule.

The example rule shown below operates on data from Telegraf's [system stats](https://github.com/influxdata/telegraf/tree/master/plugins/inputs/system) input plugin and sends a simple threshold alert to Slack:

![Example Rule](https://github.com/influxdata/chronograf/blob/master/docs/images/example-rule-gs.png)

The `Select a Time Series` section includes an [InfluxQL](https://docs.influxdata.com/influxdb/latest/query_language/) query builder which allows you to specify the target data for the alert rule.
The example shown above is working with the system stat's `usage_idle` [field](https://docs.influxdata.com/influxdb/v1.1/concepts/glossary/#field) in the `cpu` [measurement](https://docs.influxdata.com/influxdb/v1.1/concepts/glossary/#measurement).

The `Values` section defines the alert rule.
It supports three rule types:

* Threshold Rule - alert if the data cross a boundary
* Relative Rule - alert if the data change relative to the data in a different time range
* Deadman Rule - alert if no data are received for the specified time range

The example above creates a simple threshold rule that sends an alert when `usage_idle` values are less than 96%.
Notice that the graph provides a preview of the target data and the configured rule boundary.

Lastly, the `Alert Message` section allows you to personalize the alert message and select an alert endpoint.
The rule shown above sends alert messages to a Slack channel.
Here's an example of the alert messages in Slack:

![Slack Alert](https://github.com/influxdata/chronograf/blob/master/docs/images/slack-alert-gs.png)

Currently, Chronograf supports the following alert endpoints: HipChat, OpsGenie, PagerDuty, Sensu, Slack, SMTP, Talk, Telegram, and VictorOps.
You can configure your alert endpoints on the `CONFIGURE KAPACITOR` page.

### View all Active Alerts

See all active alerts on the `ALERTING` page, and filter them by `Name`,
`Level`, and `Host`:

![Alert View](https://github.com/influxdata/chronograf/blob/master/docs/images/alert-view-gs.png)

### Alerta TICKscript Parser

Chronograf offers a parser for TICKscripts that use the [Alerta](https://docs.influxdata.com/kapacitor/latest/nodes/alert_node/#alerta) output.
This is a new feature in version 1.2.0-beta2.

To use the TICKscript parser:

* Select Alerta as the output when creating or editing an alert rule
* Paste your existing TICKscript in the text input (spacing doesn't matter!)
* Save your rule

You're good to go! The system automatically parses your TICKscript and creates a
Chronograf-friendly alert rule.

> **Notes:**
>
* Currently, the Alerta TICKscript parser requires users to **paste** their existing TICKscript in the text input. The parser does not support manually entering or editing a TICKscript.
* The parser requires users to whitespace delimit any services listed in the TICKscript's [`.services()` attribute](https://docs.influxdata.com/kapacitor/latest/nodes/alert_node/#alerta-services).

## Manage Users and Queries

The `ADMIN` section of Chronograf supports managing InfluxDB users and queries.

### User Management

Create, assign permissions to, and delete [InfluxDB users](https://docs.influxdata.com/influxdb/latest/query_language/authentication_and_authorization/#user-types-and-privileges).
In version 1.2.0-beta5, Chronograf only supports assigning `ALL` permissions to users; that is, read and write permissions to every database in the InfluxDB instance.

### Query Management

View currently-running queries and stop expensive queries from running on the InfluxDB instance:

![Alert View](https://github.com/influxdata/chronograf/blob/master/docs/images/admin-gs.png)