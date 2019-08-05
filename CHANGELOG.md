## v2.0.0-alpha.17 [unreleased]

### Features
1. [14495](https://github.com/influxdata/influxdb/pull/14495): optional gzip compression of the query CSV response.
1. [14567](https://github.com/influxdata/influxdb/pull/14567): Add task types.

### UI Improvements

### Bug Fixes

1. [14480](https://github.com/influxdata/influxdb/pull/14480): Fix authentication when updating a task with invalid org or bucket.
1. [14497](https://github.com/influxdata/influxdb/pull/14497): Update the documentation link for Telegraf.
1. [14492](https://github.com/influxdata/influxdb/pull/14492): Fix to surface errors properly as task notifications on create.

## v2.0.0-alpha.16 [2019-07-25]

### Bug Fixes

1. [14385](https://github.com/influxdata/influxdb/pull/14385): Add link to Documentation text in line protocol upload overlay
1. [14344](https://github.com/influxdata/influxdb/pull/14344): Fix issue in Authorization API, can't create auth for another user.
1. [14352](https://github.com/influxdata/influxdb/pull/14352): Fix Influx CLI ignored user flag for auth creation.
1. [14379](https://github.com/influxdata/influxdb/pull/14379): Fix the map example in the documentation
1. [14423](https://github.com/influxdata/influxdb/pull/14423): Ignore null/empty Flux rows which prevents a single stat/gauge crash.
1. [14434](https://github.com/influxdata/influxdb/pull/14434): Fixes an issue where clicking on a dashboard name caused an incorrect redirect.
1. [14441](https://github.com/influxdata/influxdb/pull/14441): Upgrade templates lib to 0.5.0
1. [14453](https://github.com/influxdata/influxdb/pull/14453): Upgrade giraffe lib to 0.16.1
1. [14412](https://github.com/influxdata/influxdb/pull/14412): Fix incorrect notification type for manually running a Task
1. [14356](https://github.com/influxdata/influxdb/pull/14356): Fix an issue where canceled tasks did not resume.

## v2.0.0-alpha.15 [2019-07-11]

### Features

1. [14256](https://github.com/influxdata/influxdb/pull/14256): Add time zone support to UI
2. [14243](https://github.com/influxdata/influxdb/pull/14243): Addded new storage inspection tool to verify tsm files
3. [14353](https://github.com/influxdata/influxdb/pull/14353): Require a token to be supplied for all task creation

### Bug Fixes

1. [14287](https://github.com/influxdata/influxdb/pull/14287): Fix incorrect reporting of task as successful when error occurs during result iteration
1. [14412](https://github.com/influxdata/influxdb/pull/14412): Fix incorrect notification type for manually running a Task

### Known Issues

1. [influxdata/flux#1492](https://github.com/influxdata/flux/issues/1492): Null support in Flux was introduced in Alhpa 14. Several null issues were fixed in this release, but one known issue remains - Users may hit a panic if the first record processed by a map function has a null value.

## v2.0.0-alpha.14 [2019-06-28]

### Features

1. [14221](https://github.com/influxdata/influxdb/pull/14221): Add influxd inspect verify-wal tool
1. [14218](https://github.com/influxdata/influxdb/commit/4faf2a24def4f351aef5b3c0f2907c385f82fdb9): Move to Flux .34.2 - which includes new string functions and initial multi-datasource support with Sql.from()
1. [14164](https://github.com/influxdata/influxdb/pull/14164): Only click save once to save cell
1. [14188](https://github.com/influxdata/influxdb/pull/14188): Enable selecting more columns for line visualizations

### UI Improvements

1. [14194](https://github.com/influxdata/influxdb/pull/14194): Draw gauges correctly on HiDPI displays
1. [14194](https://github.com/influxdata/influxdb/pull/14194): Clamp gauge position to gauge domain
1. [14168](https://github.com/influxdata/influxdb/pull/14168): Improve display of error messages
1. [14157](https://github.com/influxdata/influxdb/pull/14157): Remove rendering bottleneck when streaming Flux responses
1. [14165](https://github.com/influxdata/influxdb/pull/14165): Prevent variable dropdown from clipping

## v2.0.0-alpha.13 [2019-06-13]

### Features

1. [14130](https://github.com/influxdata/influxdb/pull/14130): Add static templates for system, docker, redis, kubernetes
1. [14189](https://github.com/influxdata/influxdb/pull/14189): Add option to select a token when creating a task
1. [14200](https://github.com/influxdata/influxdb/pull/14200): Add the ability to update a token when updating a task

## v2.0.0-alpha.12 [2019-06-13]

### Features

1. [14059](https://github.com/influxdata/influxdb/pull/14059): Enable formatting line graph y ticks with binary prefix
1. [14052](https://github.com/influxdata/influxdb/pull/14052): Add x and y column pickers to graph types
1. [14128](https://github.com/influxdata/influxdb/pull/14128): Add option to shade area below line graphs

### Bug Fixes

1. [14085](https://github.com/influxdata/influxdb/pull/14085): Fix performance regression in graph tooltips

### UI Improvements

## v2.0.0-alpha.11 [2019-05-31]

1. [14031](https://github.com/influxdata/influxdb/pull/14031): Correctly check if columnKeys include xColumn in heatmap

## v2.0.0-alpha.10 [2019-05-30]

### Features

1. [13945](https://github.com/influxdata/influxdb/pull/13945): Add heatmap visualization type
1. [13961](https://github.com/influxdata/influxdb/pull/13961): Add scatter graph visualization type
1. [13850](https://github.com/influxdata/influxdb/pull/13850): Add description field to Tasks
1. [13924](https://github.com/influxdata/influxdb/pull/13924): Add CLI arguments for configuring session length and renewal
1. [13961](https://github.com/influxdata/influxdb/pull/13961): Add smooth interpolation option to line graphs

### Bug Fixes

1. [13753](https://github.com/influxdata/influxdb/pull/13753): Removed hardcoded bucket for Getting Started with Flux dashboard
1. [13783](https://github.com/influxdata/influxdb/pull/13783): Ensure map type variables allow for selecting values
1. [13800](https://github.com/influxdata/influxdb/pull/13800): Generate more idiomatic Flux in query builder
1. [13797](https://github.com/influxdata/influxdb/pull/13797): Expand tab key presses to 2 spaces in the Flux editor
1. [13823](https://github.com/influxdata/influxdb/pull/13823): Prevent dragging of Variable Dropdowns when dragging a scrollbar inside the dropdown
1. [13853](https://github.com/influxdata/influxdb/pull/13853): Improve single stat computation
1. [13945](https://github.com/influxdata/influxdb/pull/13945): Fix crash when opening histogram settings with no data

### UI Improvements

1. [#13835](https://github.com/influxdata/influxdb/pull/13835): Render checkboxes in query builder tag selection lists
1. [#13856](https://github.com/influxdata/influxdb/pull/13856): Fix jumbled card text in Telegraf configuration wizard
1. [#13888](https://github.com/influxdata/influxdb/pull/13888): Change scrapers in scrapers list to be resource cards
1. [#13925](https://github.com/influxdata/influxdb/pull/13925): Export and download resource with formatted resource name with no spaces

## v2.0.0-alpha.9 [2019-05-01]

**NOTE: This will remove all tasks from your InfluxDB v2.0 instance.**

### Features

1. [13423](https://github.com/influxdata/influxdb/pull/13423): Set autorefresh of dashboard to pause if absolute time range is selected
1. [13473](https://github.com/influxdata/influxdb/pull/13473): Switch task back end to a more modular and flexible system
1. [13493](https://github.com/influxdata/influxdb/pull/13493): Add org profile tab with ability to edit organization name
1. [13510](https://github.com/influxdata/influxdb/pull/13510): Add org name to dahboard page title
1. [13520](https://github.com/influxdata/influxdb/pull/13520): Add cautioning to bucket renaming
1. [13560](https://github.com/influxdata/influxdb/pull/13560): Add option to generate all access token in tokens tab
1. [13601](https://github.com/influxdata/influxdb/pull/13601): Add option to generate read/write token in tokens tab
1. [13715](https://github.com/influxdata/influxdb/pull/13715): Added a new Local Metrics Dashboard template that is created during Quick Start

### Bug Fixes

1. [13584](https://github.com/influxdata/influxdb/pull/13584): Fixed scroll clipping found in label editing flow
1. [13585](https://github.com/influxdata/influxdb/pull/13585): Prevent overlapping text and dot in time range dropdown
1. [13602](https://github.com/influxdata/influxdb/pull/13602): Updated link in notes cell to a more useful site
1. [13618](https://github.com/influxdata/influxdb/pull/13618): Show error message when adding line protocol
1. [13657](https://github.com/influxdata/influxdb/pull/13657): Update UI Flux function documentation
1. [13718](https://github.com/influxdata/influxdb/pull/13718): Updated System template to support math with floats
1. [13732](https://github.com/influxdata/influxdb/pull/13732): Fixed the window function documentation
1. [13738](https://github.com/influxdata/influxdb/pull/13738): Fixed typo in the `range` Flux function example
1. [13742](https://github.com/influxdata/influxdb/pull/13742): Updated the `systemTime` function to use `system.time`

### UI Improvements

1. [13424](https://github.com/influxdata/influxdb/pull/13424): Add general polish and empty states to Create Dashboard from Template overlay

## v2.0.0-alpha.8 [2019-04-12]

### Features

1. [13024](https://github.com/influxdata/influxdb/pull/13024): Add the ability to edit token's description
1. [13078](https://github.com/influxdata/influxdb/pull/13078): Add the option to create a Dashboard from a Template.
1. [13161](https://github.com/influxdata/influxdb/pull/13161): Add the ability to add labels on variables
1. [13171](https://github.com/influxdata/influxdb/pull/13171): Add switch organizations dropdown to home navigation menu item.
1. [13173](https://github.com/influxdata/influxdb/pull/13173): Add create org to side nav
1. [13345](https://github.com/influxdata/influxdb/pull/13345): Added a new Getting Started with Flux Template

### Bug Fixes

1. [13284](https://github.com/influxdata/influxdb/pull/13284): Update shift to timeShift in the flux functions side bar

### UI Improvements

1. [13287](https://github.com/influxdata/influxdb/pull/13287): Update cursor to grab when hovering draggable areas
1. [13311](https://github.com/influxdata/influxdb/pull/13311): Sync note editor text and preview scrolling
1. [13249](https://github.com/influxdata/influxdb/pull/13249): Add the ability to create a bucket when creating an organization

## v2.0.0-alpha.7 [2019-03-28]

### Features

1. [12663](https://github.com/influxdata/influxdb/pull/12663): Insert flux function near cursor in flux editor
1. [12678](https://github.com/influxdata/influxdb/pull/12678): Enable the use of variables in the Data Explorer and Cell Editor Overlay
1. [12655](https://github.com/influxdata/influxdb/pull/12655): Add a variable control bar to dashboards to select values for variables.
1. [12706](https://github.com/influxdata/influxdb/pull/12706): Add ability to add variable to script from the side menu.
1. [12791](https://github.com/influxdata/influxdb/pull/12791): Use time range for metaqueries in Data Explorer and Cell Editor Overlay
1. [12827](https://github.com/influxdata/influxdb/pull/12827): Fix screen tearing bug in Raw Data View
1. [12843](https://github.com/influxdata/influxdb/pull/12843): Add copy to clipboard button to export overlays
1. [12826](https://github.com/influxdata/influxdb/pull/12826): Enable copying error messages to the clipboard from dashboard cells
1. [12876](https://github.com/influxdata/influxdb/pull/12876): Add the ability to update token's status in Token list
1. [12821](https://github.com/influxdata/influxdb/pull/12821): Allow variables to be re-ordered within control bar on a dashboard.
1. [12888](https://github.com/influxdata/influxdb/pull/12888): Add the ability to delete a template
1. [12901](https://github.com/influxdata/influxdb/pull/12901): Save user preference for variable control bar visibility and default to visible
1. [12910](https://github.com/influxdata/influxdb/pull/12910): Add the ability to clone a template
1. [12958](https://github.com/influxdata/influxdb/pull/12958): Add the ability to import a variable

### Bug Fixes

1. [12684](https://github.com/influxdata/influxdb/pull/12684): Fix mismatch in bucket row and header
1. [12703](https://github.com/influxdata/influxdb/pull/12703): Allows user to edit note on cell
1. [12764](https://github.com/influxdata/influxdb/pull/12764): Fix empty state styles in scrapers in org view
1. [12790](https://github.com/influxdata/influxdb/pull/12790): Fix bucket creation error when changing rentention rules types.
1. [12793](https://github.com/influxdata/influxdb/pull/12793): Fix task creation error when switching schedule types.
1. [12805](https://github.com/influxdata/influxdb/pull/12805): Fix hidden horizonal scrollbars in flux raw data view
1. [12827](https://github.com/influxdata/influxdb/pull/12827): Fix screen tearing bug in Raw Data View
1. [12961](https://github.com/influxdata/influxdb/pull/12961): Fix scroll clipping in graph legends & dropdown menus
1. [12959](https://github.com/influxdata/influxdb/pull/12959): Fix routing loop

### UI Improvements

1. [12782](https://github.com/influxdata/influxdb/pull/12782): Move bucket selection in the query builder to the first card in the list
1. [12850](https://github.com/influxdata/influxdb/pull/12850): Ensure editor is automatically focused in note editor
1. [12915](https://github.com/influxdata/influxdb/pull/12915): Add ability to edit a template's name.

## v2.0.0-alpha.6 [2019-03-15]

### Release Notes

We have updated the way we do predefined dashboards to [include Templates](https://github.com/influxdata/influxdb/pull/12532) in this release which will cause existing Organizations to not have a System dashboard created when they build a new Telegraf configuration. In order to get this functionality, remove your existing data and start from scratch.

**NOTE: This will remove all data from your InfluxDB v2.0 instance including timeseries data.**

On most `linux` systems including `macOS`:

```sh
$ rm -r ~/.influxdbv2
```

Once completed, `v2.0.0-alpha.6` can be started.

### Features

1. [12496](https://github.com/influxdata/influxdb/pull/12496): Add ability to import a dashboard
1. [12524](https://github.com/influxdata/influxdb/pull/12524): Add ability to import a dashboard from org view
1. [12531](https://github.com/influxdata/influxdb/pull/12531): Add ability to export a dashboard and a task
1. [12615](https://github.com/influxdata/influxdb/pull/12615): Add `run` subcommand to influxd binary. This is also the default when no subcommand is specified.
1. [12523](https://github.com/influxdata/influxdb/pull/12523): Add ability to save a query as a variable from the Data Explorer.
1. [12532](https://github.com/influxdata/influxdb/pull/12532): Add System template on onboarding

### Bug Fixes

1. [12641](https://github.com/influxdata/influxdb/pull/12641): Stop scrollbars from covering text in flux editor

### UI Improvements

1. [12610](https://github.com/influxdata/influxdb/pull/12610): Fine tune keyboard interactions for managing labels from a resource card

## v2.0.0-alpha.5 [2019-03-08]

### Release Notes

This release includes a [breaking change](https://github.com/influxdata/influxdb/pull/12391) to the format that TSM and index data are stored on disk.
Any existing local data will not be queryable once InfluxDB is upgraded to this release.
Prior to installing this release we recommend all storage-engine data is removed from your local InfluxDB `2.x` installation; this can be achieved without losing any of your other InfluxDB `2.x` data (settings etc).
To remove only local storage data, run the following in a terminal.

On most `linux` systems:

```sh

# Replace <username> with your actual username.

$ rm -r /home/<username>/.influxdbv2/engine
```

On `macOS`:

```sh
# Replace <username> with your actual username.

$ rm -r /Users/<username>/.influxdbv2/engine
```

Once completed, `v2.0.0-alpha.5` can be started.

### Features

1. [12096](https://github.com/influxdata/influxdb/pull/12096): Add labels to cloned tasks
1. [12111](https://github.com/influxdata/influxdb/pull/12111): Add ability to filter resources by clicking a label
1. [12401](https://github.com/influxdata/influxdb/pull/12401): Add ability to add a member to org
1. [12391](https://github.com/influxdata/influxdb/pull/12391): Improve representation of TSM tagsets on disk
1. [12437](https://github.com/influxdata/influxdb/pull/12437): Add ability to remove a member from org

### Bug Fixes

1. [12302](https://github.com/influxdata/influxdb/pull/12302): Prevent clipping of code snippets in Firefox
1. [12379](https://github.com/influxdata/influxdb/pull/12379): Prevent clipping of cell edit menus in dashboards

### UI Improvements

1. [12302](https://github.com/influxdata/influxdb/pull/12302): Make code snippet copy functionality easier to use
1. [12304](https://github.com/influxdata/influxdb/pull/12304): Always show live preview in Note Cell editor
1. [12317](https://github.com/influxdata/influxdb/pull/12317): Redesign Create Scraper workflow
1. [12317](https://github.com/influxdata/influxdb/pull/12317): Show warning in Telegrafs and Scrapers lists when user has no buckets
1. [12384](https://github.com/influxdata/influxdb/pull/12384): Streamline label addition, removal, and creation from the dashboards list
1. [12464](https://github.com/influxdata/influxdb/pull/12464): Improve label color selection

## v2.0.0-alpha.4 [2019-02-21]

### Features

1. [11954](https://github.com/influxdata/influxdb/pull/11954): Add the ability to run a task manually from tasks page
1. [11990](https://github.com/influxdata/influxdb/pull/11990): Add the ability to select a custom time range in explorer and dashboard
1. [12009](https://github.com/influxdata/influxdb/pull/12009): Display the version information on the login page
1. [12011](https://github.com/influxdata/influxdb/pull/12011): Add the ability to update a Variable's name and query.
1. [12026](https://github.com/influxdata/influxdb/pull/12026): Add labels to cloned dashboard
1. [12018](https://github.com/influxdata/influxdb/pull/12057): Add ability filter resources by label name
1. [11973](https://github.com/influxdata/influxdb/pull/11973): Add ability to create or add labels to a resource from labels editor

### Bug Fixes

1. [11997](https://github.com/influxdata/influxdb/pull/11997): Update the bucket retention policy to update the time in seconds

### UI Improvements

1. [12016](https://github.com/influxdata/influxdb/pull/12016): Update the preview in the label overlays to be shorter
1. [12012](https://github.com/influxdata/influxdb/pull/12012): Add notifications to scrapers page for created/deleted/updated scrapers
1. [12023](https://github.com/influxdata/influxdb/pull/12023): Add notifications to buckets page for created/deleted/updated buckets
1. [12072](https://github.com/influxdata/influxdb/pull/12072): Update the admin page to display error for password length

## v2.0.0-alpha.3 [2019-02-15]

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
1. [11938](https://github.com/influxdata/influxdb/pull/11938): Add ordering to UI list items

## v2.0.0-alpha.2 [2019-02-07]

### Features

1. [11677](https://github.com/influxdata/influxdb/pull/11677): Add instructions button to view `$INFLUX_TOKEN` setup for telegraf configs
1. [11693](https://github.com/influxdata/influxdb/pull/11693): Save the \$INFLUX_TOKEN environmental variable in telegraf configs
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
