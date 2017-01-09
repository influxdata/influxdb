## v1.1.0 [unreleased]

### Upcoming Bug Fixes
### Upcoming Features
  1. [#660](https://github.com/influxdata/chronograf/issues/660): Add option to accept any certificate from InfluxDB.
  1. [#733](https://github.com/influxdata/chronograf/pull/733): Add optional Github organization membership checks to authentication
  1. [#564](https://github.com/influxdata/chronograf/issues/564): Add RabbitMQ pre-canned layout.
 
## v1.1.0-beta5 [2017-01-05]

### Bug Fixes
  1. [#693](https://github.com/influxdata/chronograf/issues/693): Fix corrupted MongoDB pre-canned layout.
  1. [#714](https://github.com/influxdata/chronograf/issues/714): Relative rules check data in the wrong direction
  1. [#718](https://github.com/influxdata/chronograf/issues/718): Fix bug that stopped apps from displaying

## v1.1.0-beta4 [2016-12-30]

### Features
  1. [#691](https://github.com/influxdata/chronograf/issues/691): Add server-side dashboard API
  2. [#709](https://github.com/influxdata/chronograf/pull/709): Add kapacitor range alerting to API
  3. [#672](https://github.com/influxdata/chronograf/pull/672): Added visual indicator for down hosts
  4. [#612](https://github.com/influxdata/chronograf/issues/612): Add dashboard menu

### Bug Fixes
  1. [679](https://github.com/influxdata/chronograf/issues/679): Fix version display

## v1.1.0-beta3 [2016-12-16]

### Features
  1. [#610](https://github.com/influxdata/chronograf/issues/610): Add ability to edit raw text queries in the Data Explorer

### UI Improvements
  1. [#688](https://github.com/influxdata/chronograf/issues/688): Add ability to visually distinguish queries in the Data Explorer
  1. [#618](https://github.com/influxdata/chronograf/issues/618): Add measurement name and field key to the query tab in the Data Explorer
  1. [#698](https://github.com/influxdata/chronograf/issues/698): Add color differentiation for Kapacitor alert levels
  1. [#698](https://github.com/influxdata/chronograf/issues/698): Clarify an empty Kapacitor configuration on the InfluxDB Sources page
  1. [#676](https://github.com/influxdata/chronograf/issues/676): Streamline the function selector in the Data Explorer

### Bug Fixes
  1. [#652](https://github.com/influxdata/chronograf/issues/652),[#670](https://github.com/influxdata/chronograf/issues/670): Allow text selecting in text box inputs
  2. [#679](https://github.com/influxdata/chronograf/issues/679): Add version information to the nightly builds
  3. [#675](https://github.com/influxdata/chronograf/issues/675): Fix user flow for Kapacitor connect

## v1.1.0-beta2 [2016-12-09]

### Features
  1. [#624](https://github.com/influxdata/chronograf/issues/624): Add time range selection to kapacitor alert rules
  1. Update Go to 1.7.4

### Bug Fixes
  1. [#664](https://github.com/influxdata/chronograf/issues/664): Fix Content-Type of single-page app to always be text/html
  1. [#671](https://github.com/influxdata/chronograf/issues/671): Fix multiple influxdb source freezing page

## v1.1.0-beta1 [2016-12-06]
### Layouts
  1. [#575](https://github.com/influxdata/chronograf/issues/556): Varnish Layout
  2. [#535](https://github.com/influxdata/chronograf/issues/535): Elasticsearch Layout

### Features
  1. [#565](https://github.com/influxdata/chronograf/issues/565) [#246](https://github.com/influxdata/chronograf/issues/246) [#234](https://github.com/influxdata/chronograf/issues/234) [#311](https://github.com/influxdata/chronograf/issues/311) Github Oauth login
  2. [#487](https://github.com/influxdata/chronograf/issues/487): Warn users if they are using a kapacitor instance that is configured to use an influxdb instance that does not match the current source
  3. [#597](https://github.com/influxdata/chronograf/issues/597): Filter host by series tags
  4. [#568](https://github.com/influxdata/chronograf/issues/568): [#569](https://github.com/influxdata/chronograf/issues/569): Add support for multiple y-axis, labels, and ranges
  5. [#605](https://github.com/influxdata/chronograf/issues/605): Singlestat visualization type in host view
  5. [#607](https://github.com/influxdata/chronograf/issues/607): Singlestat and line graph visualization type in host view

### Bug Fixes
  1. [#536](https://github.com/influxdata/chronograf/issues/536) Redirect the user to the kapacitor config screen if they are attempting to view or edit alerts without a configured kapacitor
  2. [#539](https://github.com/influxdata/chronograf/issues/539) Zoom works only on the first graph of a layout
  3. [#494](https://github.com/influxdata/chronograf/issues/494) Layouts should only be displayed when the measurement is present
  4. [#588](https://github.com/influxdata/chronograf/issues/588) Unable to connect to source
  5. [#586](https://github.com/influxdata/chronograf/issues/586) Allow telegraf database in non-default locations
  6. [#542](https://github.com/influxdata/chronograf/issues/542) Graphs in layouts do not show up in the order of the layout definition
  7. [#574](https://github.com/influxdata/chronograf/issues/574): Fix broken graphs on Postgres Layouts by adding aggregates
  8. [#644](https://github.com/influxdata/chronograf/pull/644): Fix bug that stopped apps from displaying
  9. [#510](https://github.com/influxdata/chronograf/issues/510): Fix connect button

## v1.1-alpha [2016-11-14]

### Release Notes

This is the initial alpha release of Chronograf 1.1.
