## v2.0.0-beta.11 [unreleased]

### Features

1. [18011](https://github.com/influxdata/influxdb/pull/18011): Integrate UTC dropdown when making custom time range query

### Bug Fixes

### UI Improvements

## v2.0.0-beta.10 [2020-05-07]

### Features

1. [17934](https://github.com/influxdata/influxdb/pull/17934): Add ability to delete a stack and all the resources associated with it
1. [17941](https://github.com/influxdata/influxdb/pull/17941): Enforce DNS name compliance on all pkger resources' metadata.name field
1. [17989](https://github.com/influxdata/influxdb/pull/17989): Add stateful pkg management with stacks
1. [18007](https://github.com/influxdata/influxdb/pull/18007): Add remove and list pkger stack commands to influx CLI
1. [18017](https://github.com/influxdata/influxdb/pull/18017): Fixup display message for interactive influx setup cmd

### Bug Fixes

1. [17906](https://github.com/influxdata/influxdb/pull/17906): Ensure UpdateUser cleans up the index when updating names
1. [17933](https://github.com/influxdata/influxdb/pull/17933): Ensure Checks can be set for zero values

### UI Improvements

1. [17860](https://github.com/influxdata/influxdb/pull/17860): Allow bucket creation from the Data Explorer and Cell Editor

## v2.0.0-beta.9 [2020-04-23]

### Features

1. [17851](https://github.com/influxdata/influxdb/pull/17851): Add feature flag package capability and flags endpoint

### Bug Fixes

1. [17618](https://github.com/influxdata/influxdb/pull/17618): Add index for URM by user ID to improve lookup performance
1. [17751](https://github.com/influxdata/influxdb/pull/17751): Existing session expiration time is respected on session renewal
1. [17817](https://github.com/influxdata/influxdb/pull/17817): Make CLI respect env vars and flags in addition to the configs and extend support for config orgs to all commands

### UI Improvements

1. [17714](https://github.com/influxdata/influxdb/pull/17714): Cloud environments no longer render markdown images, for security reasons.
1. [17321](https://github.com/influxdata/influxdb/pull/17321): Improve UI for sorting resources
1. [17740](https://github.com/influxdata/influxdb/pull/17740): Add single-color color schemes for visualizations
1. [17849](https://github.com/influxdata/influxdb/pull/17849): Move Organization navigation items to user menu.

## v2.0.0-beta.8 [2020-04-10]

### Features

1. [17490](https://github.com/influxdata/influxdb/pull/17490): `influx config -`, to switch back to previous activated configuration
1. [17581](https://github.com/influxdata/influxdb/pull/17581): Introduce new navigation menu
1. [17595](https://github.com/influxdata/influxdb/pull/17595): Add -f (--file) option to `influx query` and `influx task` commands
1. [17498](https://github.com/influxdata/influxdb/pull/17498): Added support for command line options to limit memory for queries

### Bug Fixes

1. [17257](https://github.com/influxdata/influxdb/pull/17769): Fix retention policy after bucket is migrated
1. [17612](https://github.com/influxdata/influxdb/pull/17612): Fix card size and layout jank in dashboards index view
1. [17651](https://github.com/influxdata/influxdb/pull/17651): Fix check graph font and lines defaulting to black causing graph to be unreadable
1. [17660](https://github.com/influxdata/influxdb/pull/17660): Fix text wrapping display issue and popover sizing bug when adding labels to a resource
1. [17670](https://github.com/influxdata/influxdb/pull/17670): Respect the now-time of the compiled query if it's provided
1. [17692](https://github.com/influxdata/influxdb/pull/17692): Update giraffe to fix spacing between ticks
1. [17694](https://github.com/influxdata/influxdb/pull/17694): Fixed typos in the Flux functions list
1. [17701](https://github.com/influxdata/influxdb/pull/17701): Allow mouse cursor inside Script Editor for Safari
1. [17609](https://github.com/influxdata/influxdb/pull/17609): Fixed an issue where Variables could not use other Variables
1. [17754](https://github.com/influxdata/influxdb/pull/17754): Adds error messaging for Cells in Dashboard View

### UI Improvements

1. [17583](https://github.com/influxdata/influxdb/pull/17583): Update layout of Alerts page to work on all screen sizes
1. [17657](https://github.com/influxdata/influxdb/pull/17657): Sort dashboards on Getting Started page by recently modified

## v2.0.0-beta.7 [2020-03-27]

### Features

1. [17232](https://github.com/influxdata/influxdb/pull/17232): Allow dashboards to optionally be displayed in light mode
1. [17273](https://github.com/influxdata/influxdb/pull/17273): Add shell completions command for the influx cli
1. [17353](https://github.com/influxdata/influxdb/pull/17353): Make all pkg resources unique by metadata.name field
1. [17363](https://github.com/influxdata/influxdb/pull/17363): Telegraf config tokens can no longer be retrieved after creation, but new tokens can be created after a telegraf has been setup
1. [17400](https://github.com/influxdata/influxdb/pull/17400): Be able to delete bucket by name via cli
1. [17396](https://github.com/influxdata/influxdb/pull/17396): Add module to write line data to specified url, org, and bucket
1. [17398](https://github.com/influxdata/influxdb/pull/17398): Extend influx cli write command with ability to process CSV data
1. [17448](https://github.com/influxdata/influxdb/pull/17448): Add foundation for pkger stacks, stateful package management
1. [17462](https://github.com/influxdata/influxdb/pull/17462): Flag to disable scheduling of tasks
1. [17470](https://github.com/influxdata/influxdb/pull/17470): Add ability to output cli output as json and hide table headers
1. [17472](https://github.com/influxdata/influxdb/pull/17472): Add an easy way to switch config via cli

### Bug Fixes

1. [17240](https://github.com/influxdata/influxdb/pull/17240): NodeJS logo displays properly in Firefox
1. [17363](https://github.com/influxdata/influxdb/pull/17363): Fixed telegraf configuration bugs where system buckets were appearing in the buckets dropdown
1. [17391](https://github.com/influxdata/influxdb/pull/17391): Fixed threshold check bug where checks could not be created when a field had a space in the name
1. [17384](https://github.com/influxdata/influxdb/pull/17384): Reuse slices built by iterator to reduce allocations
1. [17404](https://github.com/influxdata/influxdb/pull/17404): Updated duplicate check error message to be more explicit and actionable
1. [17515](https://github.com/influxdata/influxdb/pull/17515): Editing a table cell shows the proper values and respects changes
1. [17521](https://github.com/influxdata/influxdb/pull/17521): Table view scrolling should be slightly smoother
1. [17601](https://github.com/influxdata/influxdb/pull/17601): URL table values on single columns are being correctly parsed
1. [17552](https://github.com/influxdata/influxdb/pull/17552): Fixed a regression bug that insert aggregate functions where the cursor is rather than a new line

### UI Improvements

1. [17291](https://github.com/influxdata/influxdb/pull/17291): Redesign OSS Login page
1. [17297](https://github.com/influxdata/influxdb/pull/17297): Display graphic when a dashboard has no cells

## v2.0.0-beta.6 [2020-03-12]

### Features

1. [17085](https://github.com/influxdata/influxdb/pull/17085): Clicking on bucket name takes user to Data Explorer with bucket selected
1. [17095](https://github.com/influxdata/influxdb/pull/17095): Extend pkger dashboards with table view support
1. [17114](https://github.com/influxdata/influxdb/pull/17114): Allow for retention to be provided to influx setup command as a duration
1. [17138](https://github.com/influxdata/influxdb/pull/17138): Extend pkger export all capabilities to support filtering by lable name and resource type
1. [17049](https://github.com/influxdata/influxdb/pull/17049): Added new login and sign-up screen that for cloud users that allows direct login from their region
1. [17170](https://github.com/influxdata/influxdb/pull/17170): Added new cli multiple profiles management tool
1. [17145](https://github.com/influxdata/influxdb/pull/17145): Update kv.Store to define schema changes via new kv.Migrator types

### Bug Fixes

1. [17039](https://github.com/influxdata/influxdb/pull/17039): Fixed issue where tasks are exported for notification rules
1. [17042](https://github.com/influxdata/influxdb/pull/17042): Fixed issue where tasks are not exported when exporting by org id
1. [17070](https://github.com/influxdata/influxdb/pull/17070): Fixed issue where tasks with imports in query break in pkger
1. [17028](https://github.com/influxdata/influxdb/pull/17028): Fixed issue where selecting an aggregate function in the script editor was not adding the function to a new line
1. [17072](https://github.com/influxdata/influxdb/pull/17072): Fixed issue where creating a variable of type map was piping the incorrect value when map variables were used in queries
1. [17050](https://github.com/influxdata/influxdb/pull/17050): Added missing user names to auth CLI commands
1. [17113](https://github.com/influxdata/influxdb/pull/17113): Disabled group functionality for check query builder
1. [17120](https://github.com/influxdata/influxdb/pull/17120): Fixed cell configuration error that was popping up when users create a dashboard and accessed the disk usage cell for the first time
1. [17097](https://github.com/influxdata/influxdb/pull/17097): Listing all the default variables in the VariableTab of the script editor
1. [17049](https://github.com/influxdata/influxdb/pull/17049): Fixed bug that was preventing the interval status on the dashboard header from refreshing on selections
1. [17161](https://github.com/influxdata/influxdb/pull/17161): Update table custom decimal feature for tables to update table onFocus
1. [17168](https://github.com/influxdata/influxdb/pull/17168): Fixed UI bug that was setting Telegraf config buttons off-center and was resizing config selections when filtering through the data
1. [17208](https://github.com/influxdata/influxdb/pull/17208): Fixed UI bug that was setting causing dashboard cells to error when the a v.bucket was being used and was being configured for the first time
1. [17214](https://github.com/influxdata/influxdb/pull/17214): Fix appearance of client library logos in Safari
1. [17202](https://github.com/influxdata/influxdb/pull/17202): Fixed UI bug that was preventing checks created with the query builder from updating. Also fixed a bug that was preventing dashboard cell queries from working properly when creating group queries using the query builder

## v2.0.0-beta.5 [2020-02-27]

### Features

1. [16991](https://github.com/influxdata/influxdb/pull/16991): Update Flux functions list for v0.61
1. [16574](https://github.com/influxdata/influxdb/pull/16574): Add secure flag to session cookie

### Bug Fixes

1. [16919](https://github.com/influxdata/influxdb/pull/16919): Sort dashboards on homepage alphabetically
1. [16934](https://github.com/influxdata/influxdb/pull/16934): Tokens page now sorts by status
1. [16931](https://github.com/influxdata/influxdb/pull/16931): Set the default value of tags in a Check
1. [16935](https://github.com/influxdata/influxdb/pull/16935): Fix sort by variable type
1. [16973](https://github.com/influxdata/influxdb/pull/16973): Calculate correct stacked line cumulative when lines are different lengths
1. [17010](https://github.com/influxdata/influxdb/pull/17010): Fixed scrollbar issue where resource cards would overflow the parent container rather than be hidden and scrollable
1. [16992](https://github.com/influxdata/influxdb/pull/16992): Query Builder now groups on column values, not tag values
1. [17013](https://github.com/influxdata/influxdb/pull/17013): Scatterplots can once again render the tooltip correctly
1. [17027](https://github.com/influxdata/influxdb/pull/17027): Drop pkger gauge chart requirement for color threshold type
1. [17040](https://github.com/influxdata/influxdb/pull/17040): Fixed bug that was preventing the interval status on the dashboard header from refreshing on selections
1. [16961](https://github.com/influxdata/influxdb/pull/16961): Remove cli confirmation of secret, add an optional parameter of secret value

## v2.0.0-beta.4 [2020-02-14]

### Features

1. [16855](https://github.com/influxdata/influxdb/pull/16855): Added labels to buckets in UI
1. [16842](https://github.com/influxdata/influxdb/pull/16842): Connect monaco editor to Flux LSP server
1. [16856](https://github.com/influxdata/influxdb/pull/16856): Update Flux to v0.59.6

### Bug Fixes

1. [16852](https://github.com/influxdata/influxdb/pull/16852): Revert for bad indexing of UserResourceMappings and Authorizations
1. [15911](https://github.com/influxdata/influxdb/pull/15911): Gauge no longer allowed to become too small
1. [16878](https://github.com/influxdata/influxdb/pull/16878): Fix issue with INFLUX_TOKEN env vars being overridden by default token

## v2.0.0-beta.3 [2020-02-11]

### Features

1. [16765](https://github.com/influxdata/influxdb/pull/16765): Extend influx cli pkg command with ability to take multiple files and directories
1. [16767](https://github.com/influxdata/influxdb/pull/16767): Extend influx cli pkg command with ability to take multiple urls, files, directories, and stdin at the same time
1. [16786](https://github.com/influxdata/influxdb/pull/16786): influx cli can manage secrets.

### Bug Fixes

1. [16733](https://github.com/influxdata/influxdb/pull/16733): Fix notification rule renaming panics from UI
1. [16769](https://github.com/influxdata/influxdb/pull/16769): Fix the tooltip for stacked line graphs
1. [16825](https://github.com/influxdata/influxdb/pull/16825): Fixed false success notification for read-only users creating dashboards
1. [16822](https://github.com/influxdata/influxdb/pull/16822): Fix issue with pkger/http stack crashing on dupe content type

## v2.0.0-beta.2 [2020-01-24]

### Features

1. [16711](https://github.com/influxdata/influxdb/pull/16711): Query Builder supports group() function (change the dropdown from filter to group)
1. [16523](https://github.com/influxdata/influxdb/pull/16523): Change influx packages to be CRD compliant
1. [16547](https://github.com/influxdata/influxdb/pull/16547): Allow trailing newline in credentials file and CLI integration
1. [16545](https://github.com/influxdata/influxdb/pull/16545): Add support for prefixed cursor search to ForwardCursor types
1. [16504](https://github.com/influxdata/influxdb/pull/16504): Add backup and restore
1. [16522](https://github.com/influxdata/influxdb/pull/16522): Introduce resource logger to tasks, buckets and organizations

### Bug Fixes

1. [16656](https://github.com/influxdata/influxdb/pull/16656): Check engine closed before collecting index metrics
1. [16412](https://github.com/influxdata/influxdb/pull/16412): Reject writes which use any of the reserved tag keys
1. [16715](https://github.com/influxdata/influxdb/pull/16715): Fixed dashboard mapping for getDashboards to map correct prop
1. [16716](https://github.com/influxdata/influxdb/pull/16716): Improve the lacking error responses for unmarshal errors in org service

### Bug Fixes

1. [16527](https://github.com/influxdata/influxdb/pull/16527): fix /telegrafs panics when using org=org_name parameter

### UI Improvements

1. [16575](https://github.com/influxdata/influxdb/pull/16575): Swap billingURL with checkoutURL
1. [16203](https://github.com/influxdata/influxdb/pull/16203): Move cloud navigation to top of page instead of within left side navigation
1. [16536](https://github.com/influxdata/influxdb/pull/16536): Adjust aggregate window periods to be more "reasonable". Use duration input with validation.

## v2.0.0-beta.1 [2020-01-08]

### Features

1. [16234](https://github.com/influxdata/influxdb/pull/16234): Add support for notification endpoints to influx templates/pkgs.
1. [16242](https://github.com/influxdata/influxdb/pull/16242): Drop id prefix for secret key requirement for notification endpoints
1. [16259](https://github.com/influxdata/influxdb/pull/16259): Add support for check resource to pkger parser
1. [16262](https://github.com/influxdata/influxdb/pull/16262): Add support for check resource pkger dry run functionality
1. [16275](https://github.com/influxdata/influxdb/pull/16275): Add support for check resource pkger apply functionality
1. [16283](https://github.com/influxdata/influxdb/pull/16283): Add support for check resource pkger export functionality
1. [16212](https://github.com/influxdata/influxdb/pull/16212): Add new kv.ForwardCursor interface
1. [16297](https://github.com/influxdata/influxdb/pull/16297): Add support for notification rule to pkger parser
1. [16298](https://github.com/influxdata/influxdb/pull/16298): Add support for notification rule pkger dry run functionality
1. [16305](https://github.com/influxdata/influxdb/pull/16305): Add support for notification rule pkger apply functionality
1. [16312](https://github.com/influxdata/influxdb/pull/16312): Add support for notification rule pkger export functionality
1. [16320](https://github.com/influxdata/influxdb/pull/16320): Add support for tasks to pkger parser
1. [16322](https://github.com/influxdata/influxdb/pull/16322): Add support for tasks to pkger dry run functionality
1. [16323](https://github.com/influxdata/influxdb/pull/16323): Add support for tasks to pkger apply functionality
1. [16324](https://github.com/influxdata/influxdb/pull/16324): Add support for tasks to pkger export functionality
1. [16226](https://github.com/influxdata/influxdb/pull/16226): Add group() to Query Builder
1. [16338](https://github.com/influxdata/influxdb/pull/16338): Add last run status to check and notification rules
1. [16340](https://github.com/influxdata/influxdb/pull/16340): Add last run status to tasks
1. [16341](https://github.com/influxdata/influxdb/pull/16341): Extend pkger apply functionality with ability to provide secrets outside of pkg
1. [16345](https://github.com/influxdata/influxdb/pull/16345): Add hide headers flag to influx cli task find cmd
1. [16336](https://github.com/influxdata/influxdb/pull/16336): Manual Overrides for Readiness Endpoint
1. [16347](https://github.com/influxdata/influxdb/pull/16347): Drop legacy inmem service implementation in favor of kv service with inmem dependency
1. [16348](https://github.com/influxdata/influxdb/pull/16348): Drop legacy bolt service implementation in favor of kv service with bolt dependency
1. [16014](https://github.com/influxdata/influxdb/pull/16014): While creating check, also display notification rules that would match check based on tag rules
1. [16389](https://github.com/influxdata/influxdb/pull/16389): Increase default bucket retention period to 30 days
1. [16430](https://github.com/influxdata/influxdb/pull/16430): Added toggle to table thresholds to allow users to choose between setting threshold colors to text or background
1. [16418](https://github.com/influxdata/influxdb/pull/16418): Add Developer Documentation
1. [16260](https://github.com/influxdata/influxdb/pull/16260): Capture User-Agent header as query source for logging purposes
1. [16469](https://github.com/influxdata/influxdb/pull/16469): Add support for configurable max batch size in points write handler
1. [16509](https://github.com/influxdata/influxdb/pull/16509): Add support for applying an influx package via a public facing URL
1. [16511](https://github.com/influxdata/influxdb/pull/16511): Add jsonnet support for influx packages
1. [14782](https://github.com/influxdata/influxdb/pull/16336): Add view page for Check
1. [16537](https://github.com/influxdata/influxdb/pull/16537): Add update password for CLI

### Bug Fixes

1. [16225](https://github.com/influxdata/influxdb/pull/16225): Ensures env vars are applied consistently across cmd, and fixes issue where INFLUX\_ env var prefix was not set globally.
1. [16235](https://github.com/influxdata/influxdb/pull/16235): Removed default frontend sorting when flux queries specify sorting
1. [16238](https://github.com/influxdata/influxdb/pull/16238): Store canceled task runs in the correct bucket
1. [16237](https://github.com/influxdata/influxdb/pull/16237): Updated Sortby functionality for table frontend sorts to sort numbers correctly
1. [16249](https://github.com/influxdata/influxdb/pull/16249): Prevent potential infinite loop when finding tasks by organization.
1. [16255](https://github.com/influxdata/influxdb/pull/16255): Retain user input when parsing invalid JSON during import
1. [16268](https://github.com/influxdata/influxdb/pull/16268): Fixed test flakiness that stemmed from multiple flush/signins being called in the same test suite
1. [16346](https://github.com/influxdata/influxdb/pull/16346): Update pkger task export to only trim out option task and not all vars provided
1. [16374](https://github.com/influxdata/influxdb/pull/16374): Update influx CLI, only show "see help" message, instead of the whole usage.
1. [16380](https://github.com/influxdata/influxdb/pull/16380): Fix notification tag matching rules and enable tests to verify
1. [16376](https://github.com/influxdata/influxdb/pull/16376): Extend the y-axis when stacked graph is selected
1. [16404](https://github.com/influxdata/influxdb/pull/16404): Fixed query reset bug that was resetting query in script editor whenever dates were changed
1. [16430](https://github.com/influxdata/influxdb/pull/16430): Fixed table threshold bug that was defaulting set colors to the background.
1. [16435](https://github.com/influxdata/influxdb/pull/16435): Time labels are no longer squished to the left
1. [16427](https://github.com/influxdata/influxdb/pull/16427): Fixed underlying issue with disappearing queries made in Advanced Mode
1. [16439](https://github.com/influxdata/influxdb/pull/16439): Prevent negative zero and allow zero to have decimal places
1. [16376](https://github.com/influxdata/influxdb/pull/16413): Limit data loader bucket selection to non system buckets
1. [16458](https://github.com/influxdata/influxdb/pull/16458): Fix EOF error when manually running tasks from the Task Page.
1. [16491](https://github.com/influxdata/influxdb/pull/16491): Add missing env vals to influx cli usage and fixes precedence of flag/env var priority

### UI Improvements

1. [16444](https://github.com/influxdata/influxdb/pull/16444): Add honeybadger reporting to create checks

## v2.0.0-alpha.21 [2019-12-13]

### Features

1. [15836](https://github.com/influxdata/influxdb/pull/16077): Add stacked line layer option to graphs
1. [16094](https://github.com/influxdata/influxdb/pull/16094): Annotate log messages with trace ID, if available
1. [16187](https://github.com/influxdata/influxdb/pull/16187): Bucket create to accept an org name flag
1. [16158](https://github.com/influxdata/influxdb/pull/16158): Add trace ID response header to query endpoint

### Bug Fixes

1. [15655](https://github.com/influxdata/influxdb/pull/15655): Allow table columns to be draggable in table settings
1. [15757](https://github.com/influxdata/influxdb/pull/15757): Light up the home page icon when active
1. [15797](https://github.com/influxdata/influxdb/pull/15797): Make numeric inputs first class citizens
1. [15853](https://github.com/influxdata/influxdb/pull/15853): Prompt users to make a dashboard when dashboards are empty
1. [15884](https://github.com/influxdata/influxdb/pull/15884): Remove name editing from query definition during threshold check creation
1. [15975](https://github.com/influxdata/influxdb/pull/15975): Wait until user stops dragging and releases marker before zooming in after threshold changes
1. [16057](https://github.com/influxdata/influxdb/pull/16057): Adds `properties` to each cell on GET /dashboards/{dashboardID}
1. [16101](https://github.com/influxdata/influxdb/pull/16101): Gracefully handle invalid user-supplied JSON
1. [16105](https://github.com/influxdata/influxdb/pull/16105): Fix crash when loading queries built using Query Builder
1. [16112](https://github.com/influxdata/influxdb/pull/16112): Create cell view properties on dashboard creation
1. [16144](https://github.com/influxdata/influxdb/pull/16144): Scrollbars are dapper and proper
1. [16172](https://github.com/influxdata/influxdb/pull/16172): Fixed table ui threshold colorization issue where setting thresholds would not change table UI
1. [16194](https://github.com/influxdata/influxdb/pull/16194): Fixed windowPeriod issue that stemmed from webpack rules
1. [16175](https://github.com/influxdata/influxdb/pull/16175): Added delete functionality to note cells so that they can be deleted
1. [16204](https://github.com/influxdata/influxdb/pull/16204): Fix failure to create labels when creating telegraf configs
1. [16207](https://github.com/influxdata/influxdb/pull/16207): Fix crash when editing a Telegraf config
1. [16201](https://github.com/influxdata/influxdb/pull/16201): Updated start/endtime functionality so that custom script timeranges overwrite dropdown selections
1. [16217](https://github.com/influxdata/influxdb/pull/16217): Fix 12-hour time format to use consistent formatting and number of time ticks

### UI Improvements

## v2.0.0-alpha.20 [2019-11-20]

### Features

1. [15805](https://github.com/influxdata/influxdb/pull/15924): Add tls insecure skip verify to influx CLI.
1. [15981](https://github.com/influxdata/influxdb/pull/15981): Extend influx cli user create to allow for organization ID and user passwords to be set on user.
1. [15983](https://github.com/influxdata/influxdb/pull/15983): Autopopulate organization ids in the code samples
1. [15749](https://github.com/influxdata/influxdb/pull/15749): Expose bundle analysis tools for frontend resources
1. [15674](https://github.com/influxdata/influxdb/pull/15674): Allow users to view just the output section of a telegraf config
1. [15923](https://github.com/influxdata/influxdb/pull/15923): Allow the users to see string data in the single stat graph type

### Bug Fixes

1. [15777](https://github.com/influxdata/influxdb/pull/15777): Fix long startup when running 'influx help'
1. [15713](https://github.com/influxdata/influxdb/pull/15713): Mock missing Flux dependencies when creating tasks
1. [15731](https://github.com/influxdata/influxdb/pull/15731): Ensure array cursor iterator stats accumulate all cursor stats
1. [15866](https://github.com/influxdata/influxdb/pull/15866): Do not show Members section in Cloud environments
1. [15801](https://github.com/influxdata/influxdb/pull/15801): Change how cloud mode is enabled
1. [15820](https://github.com/influxdata/influxdb/pull/15820): Merge frontend development environments
1. [15944](https://github.com/influxdata/influxdb/pull/15944): Refactor table state logic on the frontend
1. [15920](https://github.com/influxdata/influxdb/pull/15920): Arrows in tables now show data in ascending and descening order
1. [15728](https://github.com/influxdata/influxdb/pull/15728): Sort by retention rules now sorts by seconds
1. [15628](https://github.com/influxdata/influxdb/pull/15628): Horizontal scrollbar no longer covering data

### UI Improvements

1. [15809](https://github.com/influxdata/influxdb/pull/15809): Redesign cards and animations on getting started page
1. [15787](https://github.com/influxdata/influxdb/pull/15787): Allow the users to filter with labels in telegraph input search

## v2.0.0-alpha.19 [2019-10-30]

### Features

1. [15313](https://github.com/influxdata/influxdb/pull/15313): Add shortcut for toggling comments in script editor
1. [15650](https://github.com/influxdata/influxdb/pull/15650): Expose last run status and last run error in task API

### UI Improvements

1. [15503](https://github.com/influxdata/influxdb/pull/15503): Redesign page headers to be more space efficient
1. [15426](https://github.com/influxdata/influxdb/pull/15426): Add 403 handler that redirects back to the sign-in page on oats-generated routes.
1. [15710](https://github.com/influxdata/influxdb/pull/15710): Add button to nginx and redis configuration sections to make interaction more clear

### Bug Fixes

1. [15295](https://github.com/influxdata/influxdb/pull/15295): Ensures users are created with an active status
1. [15306](https://github.com/influxdata/influxdb/pull/15306): Added missing string values for CacheStatus type
1. [15348](https://github.com/influxdata/influxdb/pull/15348): Disable saving for threshold check if no threshold selected
1. [15354](https://github.com/influxdata/influxdb/pull/15354): Query variable selector shows variable keys, not values
1. [15246](https://github.com/influxdata/influxdb/pull/15427): UI/Telegraf filter functionality shows results based on input name
1. [13940](https://github.com/influxdata/influxdb/pull/15443): Create Label Overlay UI will disable the submit button and return a UI error if the name field is empty
1. [15452](https://github.com/influxdata/influxdb/pull/15452): Log error as info message on unauthorized API call attempts
1. [15504](https://github.com/influxdata/influxdb/pull/15504): Ensure members&owners eps 404 when /org resource does not exist
1. [15510](https://github.com/influxdata/influxdb/pull/15510): UI/Telegraf sort functionality fixed
1. [15549](https://github.com/influxdata/influxdb/pull/15549): UI/Task edit functionality fixed
1. [15559](https://github.com/influxdata/influxdb/pull/15559): Exiting a configuration of a dashboard cell now properly renders the cell content
1. [15556](https://github.com/influxdata/influxdb/pull/15556): Creating a check now displays on the checklist
1. [15592](https://github.com/influxdata/influxdb/pull/15592): Changed task runs success status code from 200 to 201 to match Swagger documentation.
1. [15634](https://github.com/influxdata/influxdb/pull/15634): TextAreas have the correct height
1. [15647](https://github.com/influxdata/influxdb/pull/15647): Ensures labels are unique by organization in the kv store
1. [15695](https://github.com/influxdata/influxdb/pull/15695): Ensures variable names are unique by organization

## v2.0.0-alpha.18 [2019-09-26]

### Features

1. [15151](https://github.com/influxdata/influxdb/pull/15151): Add jsonweb package for future JWT support
1. [15168](https://github.com/influxdata/influxdb/pull/15168): Added the JMeter Template dashboard
1. [15152](https://github.com/influxdata/influxdb/pull/15152): Add JWT support to http auth middleware

### UI Improvements

1. [15211](https://github.com/influxdata/influxdb/pull/15211): Display dashboards index as a grid
1. [15099](https://github.com/influxdata/influxdb/pull/15099): Add viewport scaling to html meta for responsive mobile scaling
1. [15056](https://github.com/influxdata/influxdb/pull/15056): Remove rename and delete functionality from system buckets
1. [15056](https://github.com/influxdata/influxdb/pull/15056): Prevent new buckets from being named with the reserved "\_" prefix
1. [15056](https://github.com/influxdata/influxdb/pull/15056): Prevent user from selecting system buckets when creating Scrapers, Telegraf configurations, read/write tokens, and when saving as a task
1. [15056](https://github.com/influxdata/influxdb/pull/15056): Limit values from draggable threshold handles to 2 decimal places
1. [15040](https://github.com/influxdata/influxdb/pull/15040): Redesign check builder UI to fill the screen and make more room for composing message templates
1. [14990](https://github.com/influxdata/influxdb/pull/14990): Move Tokens tab from Settings to Load Data page
1. [14990](https://github.com/influxdata/influxdb/pull/14990): Expose all Settings tabs in navigation menu
1. [15289](https://github.com/influxdata/influxdb/pull/15289): Added Stream and table functions to query builder

### Bug Fixes

1. [14931](https://github.com/influxdata/influxdb/pull/14931): Remove scrollbars blocking onboarding UI step.

## v2.0.0-alpha.17 [2019-08-14]

### Features

1. [14809](https://github.com/influxdata/influxdb/pull/14809): Add task middleware's for checks and notifications
1. [14495](https://github.com/influxdata/influxdb/pull/14495): optional gzip compression of the query CSV response.
1. [14567](https://github.com/influxdata/influxdb/pull/14567): Add task types.
1. [14604](https://github.com/influxdata/influxdb/pull/14604): When getting task runs from the API, runs will be returned in order of most recently scheduled first.
1. [14631](https://github.com/influxdata/influxdb/pull/14631): Added Github and Apache templates
1. [14631](https://github.com/influxdata/influxdb/pull/14631): Updated name of Local Metrics template
1. [14631](https://github.com/influxdata/influxdb/pull/14631): Dashboards for all Telegraf config bundles now created
1. [14694](https://github.com/influxdata/influxdb/pull/14694): Add ability to find tasks by name.
1. [14901](https://github.com/influxdata/influxdb/pull/14901): Add ability to Peek() on reads package StreamReader types.

### UI Improvements

1. [14917](https://github.com/influxdata/influxdb/pull/14917): Make first steps in Monitoring & Alerting more obvious
1. [14889](https://github.com/influxdata/influxdb/pull/14889): Make adding data to buckets more discoverable
1. [14709](https://github.com/influxdata/influxdb/pull/14709): Move Buckets, Telgrafs, and Scrapers pages into a tab called "Load Data" for ease of discovery
1. [14846](https://github.com/influxdata/influxdb/pull/14846): Standardize formatting of "updated at" timestamp in all resource cards
1. [14887](https://github.com/influxdata/influxdb/pull/14887): Move no buckets warning in telegraf tab above the search box

### Bug Fixes

1. [14480](https://github.com/influxdata/influxdb/pull/14480): Fix authentication when updating a task with invalid org or bucket.
1. [14497](https://github.com/influxdata/influxdb/pull/14497): Update the documentation link for Telegraf.
1. [14492](https://github.com/influxdata/influxdb/pull/14492): Fix to surface errors properly as task notifications on create.
1. [14569](https://github.com/influxdata/influxdb/pull/14569): Fix limiting of get runs for task.
1. [14779](https://github.com/influxdata/influxdb/pull/14779): Refactor tasks coordinator.
1. [14846](https://github.com/influxdata/influxdb/pull/14846): Ensure onboarding "advanced" button goes to correct location

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
