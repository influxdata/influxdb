## v1.5.0.0 [unreleased]

### Features

1.  [#3233](https://github.com/influxdata/chronograf/pull/3233): Add default retention policy field as option in source configuration for use in querying hosts from Host List page & Host pages
1.  [#3290](https://github.com/influxdata/chronograf/pull/3290): Add support for PagerDuty v2 in UI
1.  [#3369](https://github.com/influxdata/chronograf/pull/3369): Add support for OpsGenie v2 in UI
1.  [#3386](https://github.com/influxdata/chronograf/pull/3386): Add support for Kafka in UI to configure and create alert handlers
1.  [#3416](https://github.com/influxdata/chronograf/pull/3416): Allow kapacitor services to be disabled

### UI Improvements

1.  [#3204](https://github.com/influxdata/chronograf/pull/3204): Notify user when a dashboard cell is added, removed, or cloned
1.  [#3215](https://github.com/influxdata/chronograf/pull/3215): Fix Template Variables Control Bar to top of dashboard page
1.  [#3214](https://github.com/influxdata/chronograf/pull/3214): Remove extra click when creating dashboard cell
1.  [#3256](https://github.com/influxdata/chronograf/pull/3256): Reduce font sizes in dashboards for increased space efficiency
1.  [#3320](https://github.com/influxdata/chronograf/pull/3320): Add overlay animation to Template Variables Manager
1.  [#3245](https://github.com/influxdata/chronograf/pull/3245): Display 'no results' on cells without results
1.  [#3354](https://github.com/influxdata/chronograf/pull/3354): Disable template variables for non editing users
1.  [#3353](https://github.com/influxdata/chronograf/pull/3353): YAxisLabels in Dashboard Graph Builder not showing until graph is redrawn
1.  [#3378](https://github.com/influxdata/chronograf/pull/3378): Ensure table graphs have a consistent ux between chrome and firefox
1.  [#3401](https://github.com/influxdata/chronograf/pull/3401): Change AutoRefresh interval to paused.
1.  [#3404](https://github.com/influxdata/chronograf/pull/3404): Get cloned cell name for notification from cloned cell generator function

### Bug Fixes

1.  [#3252](https://github.com/influxdata/chronograf/pull/3252): Allows users to select tickscript editor with mouse
1.  [#3279](https://github.com/influxdata/chronograf/pull/3279): Change color when value is equal to or greater than threshold value
1.  [#3281](https://github.com/influxdata/chronograf/pull/3281): Fix base path for kapacitor logs
1.  [#3284](https://github.com/influxdata/chronograf/pull/3284): Fix logout when using basepath & simplify basepath usage (deprecates `PREFIX_ROUTES`)
1.  [#3349](https://github.com/influxdata/chronograf/pull/3349): Fix graphs in alert rule builder for queries that include groupby
1.  [#3345](https://github.com/influxdata/chronograf/pull/3345): Fix auto not showing in the group by dropdown and explorer getting disconnected
1.  [#3353](https://github.com/influxdata/chronograf/pull/3353): Display y-axis label on initial graph load
1.  [#3352](https://github.com/influxdata/chronograf/pull/3352): Fix not being able to change the source in the CEO display
1.  [#3357](https://github.com/influxdata/chronograf/pull/3357): Fix only the selected template variable value getting loaded
1.  [#3389](https://github.com/influxdata/chronograf/pull/3389): Fix Generic OAuth bug for GitHub Enterprise where the principal was incorrectly being checked for email being Primary and Verified
1.  [#3402](https://github.com/influxdata/chronograf/pull/3402): Fix missing icons when using basepath
1.  [#3412](https://github.com/influxdata/chronograf/pull/3412): Limit max-width of TICKScript editor.
1.  [#3166](https://github.com/influxdata/chronograf/pull/3166): Fixes naming of new TICKScripts
1.  [#3449](https://github.com/influxdata/chronograf/pull/3449): Fixes data explorer query error reporting regression
1.  [#3453](https://github.com/influxdata/chronograf/pull/3453): Fix Kapacitor Logs fetch regression

## v1.4.4.1 [2018-04-16]

### Bug Fixes

1.  [#3211](https://github.com/influxdata/chronograf/pull/3211): Snapshot all db struct types in migration files

## v1.4.4.0 [2018-04-13]

### Features

1.  [#2526](https://github.com/influxdata/chronograf/pull/2526): Add support for RS256/JWKS verification, support for id_token parsing (as in ADFS)
1.  [#3060](https://github.com/influxdata/chronograf/pull/3060): Add ability to set a color palette for Line, Stacked, Step-Plot, and Bar graphs
1.  [#3103](https://github.com/influxdata/chronograf/pull/3103): Add ability to clone dashboards
1.  [#3184](https://github.com/influxdata/chronograf/pull/3184): Add ability to clone cells
1.  [#3080](https://github.com/influxdata/chronograf/pull/3080): Add tabular data visualization option with features
1.  [#3120](https://github.com/influxdata/chronograf/pull/3120): Change :interval: to represent a raw influxql duration value
1.  [#2832](https://github.com/influxdata/chronograf/pull/2832): Add paginated measurements API to server
1.  [#3148](https://github.com/influxdata/chronograf/pull/3148): Data explorer measurements can be toggled open

### UI Improvements

1.  [#3088](https://github.com/influxdata/chronograf/pull/3088): New dashboard cells appear at bottom of layout and assume the size of the most common cell
1.  [#3096](https://github.com/influxdata/chronograf/pull/3096): Standardize delete confirmation interactions
1.  [#3096](https://github.com/influxdata/chronograf/pull/3096): Standardize save & cancel interactions
1.  [#3116](https://github.com/influxdata/chronograf/pull/3116): Improve cell renaming

### Bug Fixes

1.  [#3094](https://github.com/influxdata/chronograf/pull/3094): Always save template variables on first edit
1.  [#3104](https://github.com/influxdata/chronograf/pull/3104): Query annotations at auto-refresh interval
1.  [#3109](https://github.com/influxdata/chronograf/pull/3109): Display link to configure Kapacitor on Alerts Page if no configured kapacitor.
1.  [#3111](https://github.com/influxdata/chronograf/pull/3111): Fix saving of new TICKscripts
1.  [#3130](https://github.com/influxdata/chronograf/pull/3130): Fix appearance of cell Y-Axis titles
1.  [#3129](https://github.com/influxdata/chronograf/pull/3129): Only add stateChangesOnly to new rules
1.  [#3131](https://github.com/influxdata/chronograf/pull/3131): Fix 500s when deleting organizations
1.  [#3137](https://github.com/influxdata/chronograf/pull/3137): Fixes issues with providing regexp in query
1.  [#3144](https://github.com/influxdata/chronograf/pull/3144): Ensure correct basepath prefix in URL pathname when passing InfluxQL query param to Data Explorer
1.  [#3128](https://github.com/influxdata/chronograf/pull/3128): Fix type error bug in Kapacitor Alert Config page and persist deleting of team and recipient in OpsGenieConfig
1.  [#3149](https://github.com/influxdata/chronograf/pull/3149): Fixes errors caused by switching query tabs in CEO
1.  [#3162](https://github.com/influxdata/chronograf/pull/3162): Only send threshold value to parent on blur
1.  [#3168](https://github.com/influxdata/chronograf/pull/3168): Require that emails on GitHub & Generic OAuth2 principals be verified & primary, if those fields are provided
1.  [#3182](https://github.com/influxdata/chronograf/pull/3182): Send notification when rp creation returns a failure
1.  [#3181](https://github.com/influxdata/chronograf/pull/3181): Show valid time in custom time range when now is selected
1.  [#3179](https://github.com/influxdata/chronograf/pull/3179): Default to zero for gauges
1.  [#3237](https://github.com/influxdata/chronograf/pull/3237): Fixes now() time parsing when requesting annotations

## v1.4.3.1 [2018-04-02]

### Bug Fixes

1.  [#3107](https://github.com/influxdata/chronograf/pull/3107): Fixes template variable editing not allowing saving
1.  [#3094](https://github.com/influxdata/chronograf/pull/3094): Save template variables on first edit
1.  [#3101](https://github.com/influxdata/chronograf/pull/3101): Fix template variables not loading all values

## v1.4.3.0 [2018-03-28]

### Features

1.  [#2973](https://github.com/influxdata/chronograf/pull/2973): Add unsafe SSL to Kapacitor UI configuration
1.  [#3047](https://github.com/influxdata/chronograf/pull/3047): Add server flag to grant SuperAdmin status to users authenticating from a specific Auth0 Organization

### UI Improvements

1.  [#2910](https://github.com/influxdata/chronograf/pull/2910): Redesign system notifications

### Bug Fixes

1.  [#2911](https://github.com/influxdata/chronograf/pull/2911): Fix Heroku OAuth
1.  [#2953](https://github.com/influxdata/chronograf/pull/2953): Fix error reporting in DataExplorer
1.  [#2947](https://github.com/influxdata/chronograf/pull/2947): Fix Okta oauth2 provider support
1.  [#2866](https://github.com/influxdata/chronograf/pull/2866): Change hover text on delete mappings confirmation button to 'Delete'
1.  [#2919](https://github.com/influxdata/chronograf/pull/2919): Automatically add graph type 'line' to any graph missing a type
1.  [#2970](https://github.com/influxdata/chronograf/pull/2970): Fix hanging browser on docker host dashboard
1.  [#3006](https://github.com/influxdata/chronograf/pull/3006): Fix Kapacitor Rules task enabled checkboxes to only toggle exactly as clicked
1.  [#3048](https://github.com/influxdata/chronograf/pull/3048): Prevent Multi-Select Dropdown in InfluxDB Admin Users and Roles tabs from losing selection state
1.  [#3073](https://github.com/influxdata/chronograf/pull/3073): Fix Delete button in All Users admin page
1.  [#3068](https://github.com/influxdata/chronograf/pull/3068): Fix intermittent missing fill from graphs
1.  [#3087](https://github.com/influxdata/chronograf/pull/3087): Exit annotation edit mode when user navigates away from dashboard
1.  [#3079](https://github.com/influxdata/chronograf/pull/3082): Support custom time range in annotations api wrapper
1.  [#3068](https://github.com/influxdata/chronograf/pull/3068): Fix intermittent missing fill from graphs
1.  [#3079](https://github.com/influxdata/chronograf/pull/3082): Support custom time range in annotations api wrapper
1.  [#3087](https://github.com/influxdata/chronograf/pull/3087): Exit annotation edit mode when user navigates away from dashboard
1.  [#3073](https://github.com/influxdata/chronograf/pull/3073): Fix Delete button in All Users admin page

## v1.4.2.3 [2018-03-08]

## v1.4.2.2 [2018-03-07]

### Bug Fixes

1.  [#2859](https://github.com/influxdata/chronograf/pull/2859): Enable Mappings save button when valid
1.  [#2933](https://github.com/influxdata/chronograf/pull/2933): Include url in Kapacitor connection creation requests

## v1.4.2.1 [2018-02-28]

### Features

1.  [#2837](https://github.com/influxdata/chronograf/pull/2837): Prevent execution of queries in cells that are not in view on the dashboard page
1.  [#2829](https://github.com/influxdata/chronograf/pull/2829): Add an optional persistent legend which can toggle series visibility to dashboard cells
1.  [#2846](https://github.com/influxdata/chronograf/pull/2846): Allow user to annotate graphs via UI or API

### UI Improvements

1.  [#2848](https://github.com/influxdata/chronograf/pull/2848): Add ability to set a prefix and suffix on Single Stat and Gauge cell types
1.  [#2831](https://github.com/influxdata/chronograf/pull/2831): Rename 'Create Alerts' page to 'Manage Tasks'; Redesign page to improve clarity of purpose

### Bug Fixes

1.  [#2821](https://github.com/influxdata/chronograf/pull/2821): Save only selected template variable values into dashboards for non csv template variables
1.  [#2842](https://github.com/influxdata/chronograf/pull/2842): Use Generic APIKey for Oauth2 group lookup
1.  [#2850](https://github.com/influxdata/chronograf/pull/2850): Fix bug in which resizing any cell in a dashboard causes a Gauge cell to resize
1.  [#2886](https://github.com/influxdata/chronograf/pull/2886): Don't sort Single Stat & Gauge thresholds when editing threshold values
1.  [#2851](https://github.com/influxdata/chronograf/pull/2851): Maintain y axis labels in dashboard cells
1.  [#2819](https://github.com/influxdata/chronograf/pull/2819): Deprecate --new-sources in CLI

## v1.4.1.3 [2018-02-14]

### Bug Fixes

1.  [#2818](https://github.com/influxdata/chronograf/pull/2818): Allow self-signed certificates for Enterprise InfluxDB Meta nodes

## v1.4.1.2 [2018-02-13]

### Bug Fixes

1.  [9321336](https://github.com/influxdata/chronograf/commit/9321336): Respect basepath when fetching server api routes
1.  [#2812](https://github.com/influxdata/chronograf/pull/2812): Set default tempVar :interval: with data explorer csv download call.
1.  [#2811](https://github.com/influxdata/chronograf/pull/2811): Display series with value of 0 in a cell legend

## v1.4.1.1 [2018-02-12]

### Features

1.  [#2409](https://github.com/influxdata/chronograf/pull/2409): Allow multiple event handlers per rule
1.  [#2709](https://github.com/influxdata/chronograf/pull/2709): Add "send test alert" button to test kapacitor alert configurations
1.  [#2708](https://github.com/influxdata/chronograf/pull/2708): Link to kapacitor config panel from alert rule builder
1.  [#2722](https://github.com/influxdata/chronograf/pull/2722): Add auto refresh widget to hosts list page
1.  [#2784](https://github.com/influxdata/chronograf/pull/2784): Update go from 1.9.3 to 1.9.4
1.  [#2765](https://github.com/influxdata/chronograf/pull/2765): Update to go 1.9.3 and node 6.12.3 for releases
1.  [#2777](https://github.com/influxdata/chronograf/pull/2777): Allow user to delete themselves
1.  [#2703](https://github.com/influxdata/chronograf/pull/2703): Add All Users page, visible only to super admins
1.  [#2781](https://github.com/influxdata/chronograf/pull/2781): Introduce chronoctl binary for user CRUD operations
1.  [#2699](https://github.com/influxdata/chronograf/pull/2699): Introduce Mappings to allow control over new user organization assignments

### UI Improvements

1.  [#2698](https://github.com/influxdata/chronograf/pull/2698): Clarify terminology surrounding InfluxDB & Kapacitor connections
1.  [#2746](https://github.com/influxdata/chronograf/pull/2746): Separate saving TICKscript from exiting editor page
1.  [#2774](https://github.com/influxdata/chronograf/pull/2774): Enable Save (⌘ + Enter) and Cancel (Escape) hotkeys in Cell Editor Overlay
1.  [#2788](https://github.com/influxdata/chronograf/pull/2788): Enable customization of Single Stat "Base Color"

### Bug Fixes

1.  [#2684](https://github.com/influxdata/chronograf/pull/2684): Fix TICKscript Sensu alerts when no group by tags selected
1.  [#2756](https://github.com/influxdata/chronograf/pull/2756): Display 200 most-recent TICKscript log messages; prevent overlapping
1.  [#2757](https://github.com/influxdata/chronograf/pull/2757): Add "TO" to kapacitor SMTP config; improve config update error messages
1.  [#2761](https://github.com/influxdata/chronograf/pull/2761): Remove cli options from sysvinit service file
1.  [#2735](https://github.com/influxdata/chronograf/pull/2735): Remove cli options from systemd service file
1.  [#2788](https://github.com/influxdata/chronograf/pull/2788): Fix disappearance of text in Single Stat graphs during editing
1.  [#2780](https://github.com/influxdata/chronograf/pull/2780): Redirect to Alerts page after saving Alert Rule

## v1.4.0.1 [2018-1-9]

### Features

1.  [#2690](https://github.com/influxdata/chronograf/pull/2690): Add separate CLI flag for canned sources, kapacitors, dashboards, and organizations
1.  [#2672](https://github.com/influxdata/chronograf/pull/2672): Add telegraf interval configuration

### Bug Fixes

1.  [#2689](https://github.com/influxdata/chronograf/pull/2689): Allow insecure (self-signed) certificates for kapacitor and influxdb
1.  [#2664](https://github.com/influxdata/chronograf/pull/2664): Fix positioning of custom time indicator

## v1.4.0.0 [2017-12-22]

### UI Improvements

1.  [#2652](https://github.com/influxdata/chronograf/pull/2652): Add page header with instructional copy when adding initial source for consistency and clearer UX

### Bug Fixes

1.  [#2652](https://github.com/influxdata/chronograf/pull/2652): Make page render successfully when attempting to edit a source
1.  [#2664](https://github.com/influxdata/chronograf/pull/2664): Fix CustomTimeIndicator positioning
1.  [#2687](https://github.com/influxdata/chronograf/pull/2687): Remove series with "no value" from legend

## v1.4.0.0-rc2 [2017-12-21]

### UI Improvements

1.  [#2632](https://github.com/influxdata/chronograf/pull/2632): Tell user which organization they switched into and what role they have whenever they switch, including on Source Page

### Bug Fixes

1.  [#2639](https://github.com/influxdata/chronograf/pull/2639): Prevent SuperAdmin from modifying their own status
1.  [#2632](https://github.com/influxdata/chronograf/pull/2632): Give SuperAdmin DefaultRole when switching to organization where they have no role
1.  [#2642](https://github.com/influxdata/chronograf/pull/2642): Fix DE query config on first run

## v1.4.0.0-rc1 [2017-12-19]

### Features

1.  [#2593](https://github.com/influxdata/chronograf/pull/2593): Add option to use files for dashboards, organizations, data sources, and kapacitors
1.  [#2604](https://github.com/influxdata/chronograf/pull/2604): After chronograf version upgrade, backup database is created in ./backups

### UI Improvements

1.  [#2492](https://github.com/influxdata/chronograf/pull/2492): Cleanup style on login page with multiple OAuth2 providers

### Bug Fixes

1.  [#2502](https://github.com/influxdata/chronograf/pull/2502): Fix stale source data after updating or creating
1.  [#2616](https://github.com/influxdata/chronograf/pull/2616): Fix cell editing so query data choices are kept when updating a cell
1.  [#2612](https://github.com/influxdata/chronograf/pull/2612): Allow days as a valid duration value

## v1.4.0.0-beta2 [2017-12-14]

### UI Improvements

1.  [#2502](https://github.com/influxdata/chronograf/pull/2502): Fix cursor flashing between default and pointer
1.  [#2598](https://github.com/influxdata/chronograf/pull/2598): Allow appendage of a suffix to single stat visualizations
1.  [#2598](https://github.com/influxdata/chronograf/pull/2598): Allow optional colorization of text instead of background on single stat visualizations

### Bug Fixes

1.  [#2528](https://github.com/influxdata/chronograf/pull/2528): Fix template rendering to ignore template if not in query
1.  [#2563](https://github.com/influxdata/chronograf/pull/2563): Fix graph inversion if user input y-axis min greater than max

## v1.4.0.0-beta1 [2017-12-07]

### Features

1.  [#2506](https://github.com/influxdata/chronograf/pull/2506): Add support for multiple organizations, multiple users with role-based access control, and private instances
1.  [#2188](https://github.com/influxdata/chronograf/pull/2188): Add Kapacitor logs to the TICKscript editor
1.  [#2385](https://github.com/influxdata/chronograf/pull/2385): Add time shift feature to DataExplorer and Dashboards
1.  [#2426](https://github.com/influxdata/chronograf/pull/2426): Add auto group by time to Data Explorer
1.  [#2479](https://github.com/influxdata/chronograf/pull/2479): Support authentication for Enterprise Meta Nodes
1.  [#2456](https://github.com/influxdata/chronograf/pull/2456): Add boolean thresholds for kapacitor threshold alerts
1.  [#2460](https://github.com/influxdata/chronograf/pull/2460): Update kapacitor alerts to cast to float before sending to influx
1.  [#2400](https://github.com/influxdata/chronograf/pull/2400): Allow override of generic oauth2 keys for email

### UI Improvements

1.  [#2410](https://github.com/influxdata/chronograf/pull/2410): Introduce customizable Gauge visualization type for dashboard cells
1.  [#2427](https://github.com/influxdata/chronograf/pull/2427): Improve performance of Hosts, Alert History, and TICKscript logging pages when there are many items to display
1.  [#2384](https://github.com/influxdata/chronograf/pull/2384): Add filtering by name to Dashboard index page
1.  [#2477](https://github.com/influxdata/chronograf/pull/2477): Improve performance of hoverline rendering

### Bug Fixes

1.  [#2449](https://github.com/influxdata/chronograf/pull/2449): Fix .jsdep step fails when LDFLAGS is exported
1.  [#2157](https://github.com/influxdata/chronograf/pull/2157): Fix logscale producing console errors when only one point in graph
1.  [#2158](https://github.com/influxdata/chronograf/pull/2158): Fix 'Cannot connect to source' false error flag on Dashboard page
1.  [#2167](https://github.com/influxdata/chronograf/pull/2167): Add fractions of seconds to time field in csv export
1.  [#2087](https://github.com/influxdata/chronograf/pull/2087): Fix Chronograf requiring Telegraf's CPU and system plugins to ensure that all Apps appear on the HOST LIST page.
1.  [#2222](https://github.com/influxdata/chronograf/pull/2222): Fix template variables in dashboard query building.
1.  [#2291](https://github.com/influxdata/chronograf/pull/2291): Fix several kapacitor alert creation panics.
1.  [#2303](https://github.com/influxdata/chronograf/pull/2303): Add shadow-utils to RPM release packages
1.  [#2292](https://github.com/influxdata/chronograf/pull/2292): Source extra command line options from defaults file
1.  [#2327](https://github.com/influxdata/chronograf/pull/2327): After CREATE/DELETE queries, refresh list of databases in Data Explorer
1.  [#2327](https://github.com/influxdata/chronograf/pull/2327): Visualize CREATE/DELETE queries with Table view in Data Explorer
1.  [#2329](https://github.com/influxdata/chronograf/pull/2329): Include tag values alongside measurement name in Data Explorer result tabs
1.  [#2410](https://github.com/influxdata/chronograf/pull/2410): Redesign cell display options panel
1.  [#2386](https://github.com/influxdata/chronograf/pull/2386): Fix queries that include regex, numbers and wildcard
1.  [#2398](https://github.com/influxdata/chronograf/pull/2398): Fix apps on hosts page from parsing tags with null values
1.  [#2408](https://github.com/influxdata/chronograf/pull/2408): Fix updated Dashboard names not updating dashboard list
1.  [#2444](https://github.com/influxdata/chronograf/pull/2444): Fix create dashboard button
1.  [#2416](https://github.com/influxdata/chronograf/pull/2416): Fix default y-axis labels not displaying properly
1.  [#2423](https://github.com/influxdata/chronograf/pull/2423): Gracefully scale Template Variables Manager overlay on smaller displays
1.  [#2426](https://github.com/influxdata/chronograf/pull/2426): Fix Influx Enterprise users from deletion in race condition
1.  [#2467](https://github.com/influxdata/chronograf/pull/2467): Fix oauth2 logout link not having basepath
1.  [#2466](https://github.com/influxdata/chronograf/pull/2466): Fix supplying a role link to sources that do not have a metaURL
1.  [#2477](https://github.com/influxdata/chronograf/pull/2477): Fix hoverline intermittently not rendering
1.  [#2483](https://github.com/influxdata/chronograf/pull/2483): Update MySQL pre-canned dashboard to have query derivative correctly

### Features

1.  [#2188](https://github.com/influxdata/chronograf/pull/2188): Add Kapacitor logs to the TICKscript editor
1.  [#2384](https://github.com/influxdata/chronograf/pull/2384): Add filtering by name to Dashboard index page
1.  [#2385](https://github.com/influxdata/chronograf/pull/2385): Add time shift feature to DataExplorer and Dashboards
1.  [#2400](https://github.com/influxdata/chronograf/pull/2400): Allow override of generic oauth2 keys for email
1.  [#2426](https://github.com/influxdata/chronograf/pull/2426): Add auto group by time to Data Explorer
1.  [#2456](https://github.com/influxdata/chronograf/pull/2456): Add boolean thresholds for kapacitor threshold alerts
1.  [#2460](https://github.com/influxdata/chronograf/pull/2460): Update kapacitor alerts to cast to float before sending to influx
1.  [#2479](https://github.com/influxdata/chronograf/pull/2479): Support authentication for Enterprise Meta Nodes
1.  [#2477](https://github.com/influxdata/chronograf/pull/2477): Improve performance of hoverline rendering

### UI Improvements

## v1.3.10.0 [2017-10-24]

### Bug Fixes

1.  [#2095](https://github.com/influxdata/chronograf/pull/2095): Improve the copy in the retention policy edit page
1.  [#2122](https://github.com/influxdata/chronograf/pull/2122): Fix 'Could not connect to source' bug on source creation with unsafe-ssl
1.  [#2093](https://github.com/influxdata/chronograf/pull/2093): Fix when exporting `SHOW DATABASES` CSV has bad data
1.  [#2098](https://github.com/influxdata/chronograf/pull/2098): Fix not-equal-to highlighting in Kapacitor Rule Builder
1.  [#2130](https://github.com/influxdata/chronograf/pull/2130): Fix undescriptive error messages for database and retention policy creation
1.  [#2135](https://github.com/influxdata/chronograf/pull/2135): Fix drag and drop cancel button when writing data in the data explorer
1.  [#2128](https://github.com/influxdata/chronograf/pull/2128): Fix persistence of "SELECT AS" statements in queries

### Features

1.  [#2083](https://github.com/influxdata/chronograf/pull/2083): Every dashboard can now have its own time range
1.  [#2045](https://github.com/influxdata/chronograf/pull/2045): Add CSV download option in dashboard cells
1.  [#2133](https://github.com/influxdata/chronograf/pull/2133): Implicitly prepend source urls with http://
1.  [#2127](https://github.com/influxdata/chronograf/pull/2127): Add support for graph zooming and point display on the millisecond-level
1.  [#2103](https://github.com/influxdata/chronograf/pull/2103): Add manual refresh button for Dashboard, Data Explorer, and Host Pages

### UI Improvements

1.  [#2111](https://github.com/influxdata/chronograf/pull/2111): Increase size of Cell Editor query tabs to reveal more of their query strings
1.  [#2120](https://github.com/influxdata/chronograf/pull/2120): Improve appearance of Admin Page tabs on smaller screens
1.  [#2119](https://github.com/influxdata/chronograf/pull/2119): Add cancel button to TICKscript editor
1.  [#2104](https://github.com/influxdata/chronograf/pull/2104): Redesign dashboard naming & renaming interaction
1.  [#2104](https://github.com/influxdata/chronograf/pull/2104): Redesign dashboard switching dropdown

## v1.3.9.0 [2017-10-06]

### Bug Fixes

1.  [#2004](https://github.com/influxdata/chronograf/pull/2004): Fix Data Explorer disappearing query templates in dropdown
1.  [#2006](https://github.com/influxdata/chronograf/pull/2006): Fix missing alert for duplicate db name
1.  [#2015](https://github.com/influxdata/chronograf/pull/2015): Chronograf shows real status for windows hosts when metrics are saved in non-default db - thank you, @ar7z1!
1.  [#2019](https://github.com/influxdata/chronograf/pull/2006): Fix false error warning for duplicate kapacitor name
1.  [#2018](https://github.com/influxdata/chronograf/pull/2018): Fix unresponsive display options and query builder in dashboards
1.  [#2004](https://github.com/influxdata/chronograf/pull/2004): Fix DE query templates dropdown disappearance
1.  [#2006](https://github.com/influxdata/chronograf/pull/2006): Fix no alert for duplicate db name
1.  [#2015](https://github.com/influxdata/chronograf/pull/2015): Chronograf shows real status for windows hosts when metrics are saved in non-default db - thank you, @ar7z1!
1.  [#2019](https://github.com/influxdata/chronograf/pull/2006): Fix false error warning for duplicate kapacitor name
1.  [#2018](https://github.com/influxdata/chronograf/pull/2018): Fix unresponsive display options and query builder in dashboards
1.  [#1996](https://github.com/influxdata/chronograf/pull/1996): Able to switch InfluxDB sources on a per graph basis

### Features

1.  [#1885](https://github.com/influxdata/chronograf/pull/1885): Add `fill` options to data explorer and dashboard queries
1.  [#1978](https://github.com/influxdata/chronograf/pull/1978): Support editing kapacitor TICKscript
1.  [#1721](https://github.com/influxdata/chronograf/pull/1721): Introduce the TICKscript editor UI
1.  [#1992](https://github.com/influxdata/chronograf/pull/1992): Add .csv download button to data explorer
1.  [#2082](https://github.com/influxdata/chronograf/pull/2082): Add Data Explorer InfluxQL query and location query synchronization, so queries can be shared via a a URL
1.  [#1996](https://github.com/influxdata/chronograf/pull/1996): Able to switch InfluxDB sources on a per graph basis
1.  [#2041](https://github.com/influxdata/chronograf/pull/2041): Add now() as an option in the Dashboard date picker

### UI Improvements

1.  [#2002](https://github.com/influxdata/chronograf/pull/2002): Require a second click when deleting a dashboard cell
1.  [#2002](https://github.com/influxdata/chronograf/pull/2002): Sort database list in Schema Explorer alphabetically
1.  [#2002](https://github.com/influxdata/chronograf/pull/2002): Improve usability of dashboard cell context menus
1.  [#2002](https://github.com/influxdata/chronograf/pull/2002): Move dashboard cell renaming UI into Cell Editor Overlay
1.  [#2040](https://github.com/influxdata/chronograf/pull/2040): Prevent the legend from overlapping graphs at the bottom of the screen
1.  [#2054](https://github.com/influxdata/chronograf/pull/2054): Add a "Plus" icon to every button with an Add or Create action for clarity and consistency
1.  [#2052](https://github.com/influxdata/chronograf/pull/2052): Make hovering over series smoother
1.  [#2071](https://github.com/influxdata/chronograf/pull/2071): Reduce the number of pixels per cell to one point per 3 pixels
1.  [#2072](https://github.com/influxdata/chronograf/pull/2072): Remove tabs from Data Explorer
1.  [#2057](https://github.com/influxdata/chronograf/pull/2057): Improve appearance of placeholder text in inputs
1.  [#2057](https://github.com/influxdata/chronograf/pull/2057): Add ability to use "Default" values in Source Connection form
1.  [#2069](https://github.com/influxdata/chronograf/pull/2069): Display name & port in SourceIndicator tooltip
1.  [#2078](https://github.com/influxdata/chronograf/pull/2078): Improve UX/UI of Kapacitor Rule Builder to be more intuitive
1.  [#2078](https://github.com/influxdata/chronograf/pull/2078): Rename "Measurements" to "Measurements & Tags" in Query Builder

## v1.3.8.1 [unreleased]

### Bug Fixes

### Features

### UI Improvements

## v1.3.8.0 [2017-09-07]

### Bug Fixes

1.  [#1886](https://github.com/influxdata/chronograf/pull/1886): Fix the limit of 100 alert rules on alert rules page
1.  [#1930](https://github.com/influxdata/chronograf/pull/1930): Fix graphs when y-values are constant
1.  [#1951](https://github.com/influxdata/chronograf/pull/1951): Fix crosshair not being removed when user leaves graph
1.  [#1943](https://github.com/influxdata/chronograf/pull/1943): Fix inability to add kapacitor from source page on fresh install
1.  [#1947](https://github.com/influxdata/chronograf/pull/1947): Fix DataExplorer crashing if a field property is not present in the queryConfig
1.  [#1957](https://github.com/influxdata/chronograf/pull/1957): Fix the max y value of stacked graphs preventing display of the upper bounds of the chart
1.  [#1969](https://github.com/influxdata/chronograf/pull/1969): Fix for delayed selection of template variables using URL query params
1.  [#1982](https://github.com/influxdata/chronograf/pull/1982): Fix return code on meta nodes when raft redirects to leader

### Features

1.  [#1928](https://github.com/influxdata/chronograf/pull/1928): Add prefix, suffix, scale, and other y-axis formatting for cells in dashboards
1.  [#1934](https://github.com/influxdata/chronograf/pull/1934): Update the group by time when zooming in graphs
1.  [#1945](https://github.com/influxdata/chronograf/pull/1945): Add the ability to link directly to presentation mode in dashboards with the `present` boolean query parameter in the URL
1.  [#1969](https://github.com/influxdata/chronograf/pull/1969): Add the ability to select a template variable via a URL parameter

### UI Improvements

1.  [#1933](https://github.com/influxdata/chronograf/pull/1933): Use line-stacked graph type for memory information - thank you, @Joxit!
1.  [#1940](https://github.com/influxdata/chronograf/pull/1940): Improve cell sizes in Admin Database tables
1.  [#1942](https://github.com/influxdata/chronograf/pull/1942): Polish appearance of optional alert parameters in Kapacitor rule builder
1.  [#1944](https://github.com/influxdata/chronograf/pull/1944): Add active state for Status page navbar icon
1.  [#1944](https://github.com/influxdata/chronograf/pull/1944): Improve UX of navigation to a sub-nav item in the navbar
1.  [#1971](https://github.com/influxdata/chronograf/pull/1971): Resolve confusing deadman trigger alert rule UI

## v1.3.7.0 [2017-08-23]

### Features

1.  [#1928](https://github.com/influxdata/chronograf/pull/1928): Add prefix, suffix, scale, and other y-axis formatting

### UI Improvements

## v1.3.7.0

### Bug Fixes

1.  [#1795](https://github.com/influxdata/chronograf/pull/1795): Fix uptime status on Windows hosts running Telegraf
1.  [#1715](https://github.com/influxdata/chronograf/pull/1715): Chronograf now renders on IE11.
1.  [#1870](https://github.com/influxdata/chronograf/pull/1870): Fix console error for placing prop on div
1.  [#1864](https://github.com/influxdata/chronograf/pull/1864): Fix Write Data form upload button and add `onDragExit` handler
1.  [#1891](https://github.com/influxdata/chronograf/pull/1891): Fix Kapacitor config for PagerDuty via the UI
1.  [#1872](https://github.com/influxdata/chronograf/pull/1872): Prevent stats in the legend from wrapping line

### Features

1.  [#1863](https://github.com/influxdata/chronograf/pull/1863): Improve 'new-sources' server flag example by adding 'type' key

### UI Improvements

1.  [#1862](https://github.com/influxdata/chronograf/pull/1862): Show "Add Graph" button on cells with no queries

## v1.3.6.1 [2017-08-14]

**Upgrade Note** This release (1.3.6.1) fixes a possibly data corruption issue with dashboard cells' graph types. If you upgraded to 1.3.6.0 and visited any dashboard, once you have then upgraded to this release (1.3.6.1) you will need to manually reset the graph type for every cell via the cell's caret --> Edit --> Display Options. If you upgraded directly to 1.3.6.1, you should not experience this issue.

## Bug Fixes

1.  [#1795](https://github.com/influxdata/chronograf/pull/1795): Fix uptime status on Windows hosts running Telegraf
1.  [#1715](https://github.com/influxdata/chronograf/pull/1715): Chronograf now renders on IE11.
1.  [#1870](https://github.com/influxdata/chronograf/pull/1870): Fix console error for placing prop on div
1.  [#1864](https://github.com/influxdata/chronograf/pull/1864): Fix Write Data form upload button and add `onDragExit` handler
1.  [#1866](https://github.com/influxdata/chronograf/pull/1866): Fix missing cell type (and consequently single-stat)
1.  [#1891](https://github.com/influxdata/chronograf/pull/1891): Fix Kapacitor config for PagerDuty via the UI
1.  [#1897](https://github.com/influxdata/chronograf/pull/1897): Fix regression from [#1864](https://github.com/influxdata/chronograf/pull/1864) and redesign drag & drop interaction
1.  [#1872](https://github.com/influxdata/chronograf/pull/1872): Prevent stats in the legend from wrapping line
1.  [#1899](https://github.com/influxdata/chronograf/pull/1899): Fix raw query editor in Data Explorer not using selected time
1.  [#1922](https://github.com/influxdata/chronograf/pull/1922): Fix Safari display issues in the Cell Editor display options
1.  [#1715](https://github.com/influxdata/chronograf/pull/1715): Chronograf now renders on IE11.
1.  [#1866](https://github.com/influxdata/chronograf/pull/1866): Fix missing cell type (and consequently single-stat)
1.  [#1866](https://github.com/influxdata/chronograf/pull/1866): Fix data corruption issue with dashboard graph types
    **Note**: If you upgraded to 1.3.6.0 and visited any dashboard, you will need to manually reset the graph type for every cell via the cell's caret -> Edit -> Display Options.
1.  [#1870](https://github.com/influxdata/chronograf/pull/1870): Fix console error for placing prop on div
1.  [#1845](https://github.com/influxdata/chronograf/pull/1845): Fix inaccessible scroll bar in Data Explorer table
1.  [#1866](https://github.com/influxdata/chronograf/pull/1866): Fix non-persistence of dashboard graph types
1.  [#1872](https://github.com/influxdata/chronograf/pull/1872): Prevent stats in the legend from wrapping line

### Features

1.  [#1863](https://github.com/influxdata/chronograf/pull/1863): Improve 'new-sources' server flag example by adding 'type' key
1.  [#1898](https://github.com/influxdata/chronograf/pull/1898): Add an input and validation to custom time range calendar dropdowns
1.  [#1904](https://github.com/influxdata/chronograf/pull/1904): Add support for selecting template variables with URL params
1.  [#1859](https://github.com/influxdata/chronograf/pull/1859): Add y-axis controls to the API for layouts

### UI Improvements

1.  [#1862](https://github.com/influxdata/chronograf/pull/1862): Show "Add Graph" button on cells with no queries

## v1.3.6.1 [2017-08-14]

**Upgrade Note** This release (1.3.6.1) fixes a possibly data corruption issue with dashboard cells' graph types. If you upgraded to 1.3.6.0 and visited any dashboard, once you have then upgraded to this release (1.3.6.1) you will need to manually reset the graph type for every cell via the cell's caret --> Edit --> Display Options. If you upgraded directly to 1.3.6.1, you should not experience this issue.

### Bug Fixes

1.  [#1845](https://github.com/influxdata/chronograf/pull/1845): Fix inaccessible scroll bar in Data Explorer table
1.  [#1866](https://github.com/influxdata/chronograf/pull/1866): Fix non-persistence of dashboard graph types

### Features

1.  [#1859](https://github.com/influxdata/chronograf/pull/1859): Add y-axis controls to the API for layouts

### UI Improvements

1.  [#1846](https://github.com/influxdata/chronograf/pull/1846): Increase screen real estate of Query Maker in the Cell Editor Overlay

## v1.3.6.0 [2017-08-08]

### Bug Fixes

1.  [#1798](https://github.com/influxdata/chronograf/pull/1798): Fix domain not updating in visualizations when changing time range manually
1.  [#1799](https://github.com/influxdata/chronograf/pull/1799): Prevent console error spam from Dygraph's synchronize method when a dashboard has only one graph
1.  [#1813](https://github.com/influxdata/chronograf/pull/1813): Guarantee UUID for each Alert Table key to prevent dropping items when keys overlap
1.  [#1795](https://github.com/influxdata/chronograf/pull/1795): Fix uptime status on Windows hosts running Telegraf
1.  [#1715](https://github.com/influxdata/chronograf/pull/1715): Chronograf now renders properly on IE11.

### Features

1.  [#1744](https://github.com/influxdata/chronograf/pull/1744): Add a few time range shortcuts to the custom time range menu
1.  [#1714](https://github.com/influxdata/chronograf/pull/1714): Add ability to edit a dashboard graph's y-axis bounds
1.  [#1714](https://github.com/influxdata/chronograf/pull/1714): Add ability to edit a dashboard graph's y-axis label
1.  [#1744](https://github.com/influxdata/chronograf/pull/1744): Add a few pre-set time range selections to the custom time range menu-- be sure to add a sensible GROUP BY
1.  [#1744](https://github.com/influxdata/chronograf/pull/1744): Add a few time range shortcuts to the custom time range menu

### UI Improvements

1.  [#1796](https://github.com/influxdata/chronograf/pull/1796): Add spinner write data modal to indicate data is being written
1.  [#1805](https://github.com/influxdata/chronograf/pull/1805): Fix bar graphs overlapping
1.  [#1805](https://github.com/influxdata/chronograf/pull/1805): Assign a series consistent coloring when it appears in multiple cells
1.  [#1800](https://github.com/influxdata/chronograf/pull/1800): Increase size of line protocol manual entry in Data Explorer's Write Data overlay
1.  [#1812](https://github.com/influxdata/chronograf/pull/1812): Improve error message when request for Status Page News Feed fails
1.  [#1858](https://github.com/influxdata/chronograf/pull/1858): Provide affirmative UI choice for 'auto' in DisplayOptions with new toggle-based component

## v1.3.5.0 [2017-07-27]

### Bug Fixes

1.  [#1708](https://github.com/influxdata/chronograf/pull/1708): Fix z-index issue in dashboard cell context menu
1.  [#1752](https://github.com/influxdata/chronograf/pull/1752): Clarify BoltPath server flag help text by making example the default path
1.  [#1703](https://github.com/influxdata/chronograf/pull/1703): Fix cell name cancel not reverting to original name
1.  [#1751](https://github.com/influxdata/chronograf/pull/1751): Fix typo that may have affected PagerDuty node creation in Kapacitor
1.  [#1756](https://github.com/influxdata/chronograf/pull/1756): Prevent 'auto' GROUP BY as option in Kapacitor rule builder when applying a function to a field
1.  [#1773](https://github.com/influxdata/chronograf/pull/1773): Prevent clipped buttons in Rule Builder, Data Explorer, and Configuration pages
1.  [#1776](https://github.com/influxdata/chronograf/pull/1776): Fix JWT for the write path
1.  [#1777](https://github.com/influxdata/chronograf/pull/1777): Disentangle client Kapacitor rule creation from Data Explorer query creation

### Features

1.  [#1717](https://github.com/influxdata/chronograf/pull/1717): View server generated TICKscripts
1.  [#1681](https://github.com/influxdata/chronograf/pull/1681): Add the ability to select Custom Time Ranges in the Hostpages, Data Explorer, and Dashboards
1.  [#1752](https://github.com/influxdata/chronograf/pull/1752): Clarify BoltPath server flag help text by making example the default path
1.  [#1738](https://github.com/influxdata/chronograf/pull/1738): Add shared secret JWT authorization to InfluxDB
1.  [#1724](https://github.com/influxdata/chronograf/pull/1724): Add Pushover alert support
1.  [#1762](https://github.com/influxdata/chronograf/pull/1762): Restore all supported Kapacitor services when creating rules, and add most optional message parameters
1.  [#1681](https://github.com/influxdata/chronograf/pull/1681): Add the ability to select Custom Time Ranges in the Hostpages, Data Explorer, and Dashboards.
1.  [#1717](https://github.com/influxdata/chronograf/pull/1717): View server generated TICKscripts

### UI Improvements

1.  [#1707](https://github.com/influxdata/chronograf/pull/1707): Polish alerts table in status page to wrap text less
1.  [#1770](https://github.com/influxdata/chronograf/pull/1770): Specify that version is for Chronograf on Configuration page
1.  [#1779](https://github.com/influxdata/chronograf/pull/1779): Move custom time range indicator on cells into corner when in presentation mode
1.  [#1779](https://github.com/influxdata/chronograf/pull/1779): Highlight legend "Snip" toggle when active

## v1.3.4.0 [2017-07-10]

### Bug Fixes

1.  [#1612](https://github.com/influxdata/chronograf/pull/1612): Disallow writing to \_internal in the Data Explorer
1.  [#1655](https://github.com/influxdata/chronograf/pull/1655): Add more than one color to Line+Stat graphs
1.  [#1688](https://github.com/influxdata/chronograf/pull/1688): Fix updating Retention Policies in single-node InfluxDB instances
1.  [#1689](https://github.com/influxdata/chronograf/pull/1689): Lock the width of Template Variable dropdown menus to the size of their longest option

### Features

1.  [#1645](https://github.com/influxdata/chronograf/pull/1645): Add Auth0 as a supported OAuth2 provider
1.  [#1660](https://github.com/influxdata/chronograf/pull/1660): Add ability to add custom links to User menu via server CLI or ENV vars
1.  [#1660](https://github.com/influxdata/chronograf/pull/1660): Allow users to configure custom links on startup that will appear under the User menu in the sidebar
1.  [#1674](https://github.com/influxdata/chronograf/pull/1674): Add support for Auth0 organizations
1.  [#1695](https://github.com/influxdata/chronograf/pull/1695): Allow users to configure InfluxDB and Kapacitor sources on startup

### UI Improvements

1.  [#1644](https://github.com/influxdata/chronograf/pull/1644): Redesign Alerts History table on Status Page to have sticky headers
1.  [#1581](https://github.com/influxdata/chronograf/pull/1581): Refresh Template Variable values on Dashboard page load
1.  [#1655](https://github.com/influxdata/chronograf/pull/1655): Display current version of Chronograf at the bottom of Configuration page
1.  [#1655](https://github.com/influxdata/chronograf/pull/1655): Redesign Dashboards table and sort them alphabetically
1.  [#1655](https://github.com/influxdata/chronograf/pull/1655): Bring design of navigation sidebar in line with Branding Documentation

## v1.3.3.3 [2017-06-21]

### Bug Fixes

1.  [1651](https://github.com/influxdata/chronograf/pull/1651): Add back in x and y axes and revert some style changes on Line + Single Stat graphs

## v1.3.3.2 [2017-06-21]

### Bug Fixes

## v1.3.3.3 [2017-06-21]

### Bug Fixes

1.  [1651](https://github.com/influxdata/chronograf/pull/1651): Add back in x and y axes and revert some style changes on Line + Single Stat graphs

## v1.3.3.2 [2017-06-21]

### Bug Fixes

1.  [1650](https://github.com/influxdata/chronograf/pull/1650): Fix broken cpu reporting on hosts page and normalize InfluxQL

## v1.3.3.1 [2017-06-21]

### Bug Fixes

1.  [#1641](https://github.com/influxdata/chronograf/pull/1641): Fix enable / disable being out of sync on Kapacitor Rules Page

### Features

1.  [#1647](https://github.com/influxdata/chronograf/pull/1647): Add file uploader to Data Explorer for write protocol

### UI Improvements

1.  [#1642](https://github.com/influxdata/chronograf/pull/1642): Do not prefix basepath to external link for news feed

## v1.3.3.0 [2017-06-19]

### Bug Fixes

1.  [#1512](https://github.com/influxdata/chronograf/pull/1512): Prevent legend from flowing over window bottom bound
1.  [#1600](https://github.com/influxdata/chronograf/pull/1600): Prevent Kapacitor configurations from having the same name
1.  [#1600](https://github.com/influxdata/chronograf/pull/1600): Limit Kapacitor configuration names to 33 characters to fix display bug
1.  [#1622](https://github.com/influxdata/chronograf/pull/1622): Use function selector grid in Kapacitor rule builder query maker instead of dropdown

### Features

1.  [#1512](https://github.com/influxdata/chronograf/pull/1512): Synchronize vertical crosshair at same time across all graphs in a dashboard
1.  [#1609](https://github.com/influxdata/chronograf/pull/1609): Add automatic GROUP BY (time) functionality to dashboards
1.  [#1608](https://github.com/influxdata/chronograf/pull/1608): Add a Status Page with Recent Alerts bar graph, Recent Alerts table, News Feed, and Getting Started widgets

### UI Improvements

1.  [#1512](https://github.com/influxdata/chronograf/pull/1512): When dashboard time range is changed, reset graphs that are zoomed in
1.  [#1599](https://github.com/influxdata/chronograf/pull/1599): Bar graph option added to dashboard
1.  [#1600](https://github.com/influxdata/chronograf/pull/1600): Redesign source management table to be more intuitive
1.  [#1600](https://github.com/influxdata/chronograf/pull/1600): Redesign Line + Single Stat cells to appear more like a sparkline, and improve legibility
1.  [#1639](https://github.com/influxdata/chronograf/pull/1639): Improve graph synchronization performance

## v1.3.2.1 [2017-06-06]

### Bug Fixes

1.  [#1594](https://github.com/influxdata/chronograf/pull/1594): Restore Line + Single Stat styles

## v1.3.2.0 [2017-06-05]

### Bug Fixes

1.  [#1530](https://github.com/influxdata/chronograf/pull/1530): Update the query config's field ordering to always match the input query
1.  [#1535](https://github.com/influxdata/chronograf/pull/1535): Allow users to add functions to existing Kapacitor rules
1.  [#1564](https://github.com/influxdata/chronograf/pull/1564): Fix logout menu item regression
1.  [#1562](https://github.com/influxdata/chronograf/pull/1562): Fix InfluxQL parsing with multiple tag values for a tag key
1.  [#1582](https://github.com/influxdata/chronograf/pull/1582): Fix load localStorage and warning UX on fresh Chronograf install
1.  [#1584](https://github.com/influxdata/chronograf/pull/1584): Show submenus when the alert notification is present

### Features

1.  [#1537](https://github.com/influxdata/chronograf/pull/1537): Add UI to the Data Explorer for [writing data to InfluxDB](https://docs.influxdata.com/chronograf/latest/guides/transition-web-admin-interface/#writing-data)

### UI Improvements

1.  [#1508](https://github.com/influxdata/chronograf/pull/1508): Make the enter and escape keys perform as expected when renaming dashboards
1.  [#1524](https://github.com/influxdata/chronograf/pull/1524): Improve copy on the Kapacitor configuration page
1.  [#1549](https://github.com/influxdata/chronograf/pull/1549): Reset graph zoom when the user selects a new time range
1.  [#1544](https://github.com/influxdata/chronograf/pull/1544): Upgrade to new version of Influx Theme, and remove excess stylesheets
1.  [#1567](https://github.com/influxdata/chronograf/pull/1567): Replace the user icon with a solid style
1.  [#1561](https://github.com/influxdata/chronograf/pull/1561): Disable query save in cell editor mode if the query does not have a database, measurement, and field
1.  [#1575](https://github.com/influxdata/chronograf/pull/1575): Improve UX of applying functions to fields in the query builder
1.  [#1560](https://github.com/influxdata/chronograf/pull/1560): Apply mean to fields by default

## v1.3.1.0 [2017-05-22]

### Release notes

In versions 1.3.1+, installing a new version of Chronograf automatically clears the localStorage settings.

### Bug Fixes

1.  [#1450](https://github.com/influxdata/chronograf/pull/1450): Fix infinite spinner when `/chronograf` is a [basepath](https://docs.influxdata.com/chronograf/v1.3/administration/configuration/#p-basepath)
1.  [#1472](https://github.com/influxdata/chronograf/pull/1472): Remove the query templates dropdown from dashboard cell editor mode
1.  [#1458](https://github.com/influxdata/chronograf/pull/1458): New versions of Chronograf automatically clear localStorage settings
1.  [#1464](https://github.com/influxdata/chronograf/pull/1464): Fix the backwards sort arrows in table column headers
1.  [#1464](https://github.com/influxdata/chronograf/pull/1464): Fix the loading spinner on graphs
1.  [#1485](https://github.com/influxdata/chronograf/pull/1485): Filter out any template variable values that are empty, whitespace, or duplicates
1.  [#1484](https://github.com/influxdata/chronograf/pull/1484): Allow user to select SingleStat as Visualization Type before adding any queries and continue to be able to click Add Query
1.  [#1349](https://github.com/influxdata/chronograf/pull/1349): Add query for windows uptime
1.  [#1484](https://github.com/influxdata/chronograf/pull/1484): Allow users to click the add query button after selecting singleStat as the [visualization type](https://docs.influxdata.com/chronograf/v1.3/troubleshooting/frequently-asked-questions/#what-visualization-types-does-chronograf-support)
1.  [#1349](https://github.com/influxdata/chronograf/pull/1349): Add a query for windows uptime - thank you, @brianbaker!

### Features

1.  [#1477](https://github.com/influxdata/chronograf/pull/1477): Add log [event handler](https://docs.influxdata.com/chronograf/v1.3/troubleshooting/frequently-asked-questions/#what-kapacitor-event-handlers-are-supported-in-chronograf) - thank you, @mpchadwick!
1.  [#1491](https://github.com/influxdata/chronograf/pull/1491): Update Go (golang) vendoring to dep and committed vendor directory
1.  [#1500](https://github.com/influxdata/chronograf/pull/1500): Add autocomplete functionality to [template variable](https://docs.influxdata.com/chronograf/v1.3/guides/dashboard-template-variables/) dropdowns

### UI Improvements

1.  [#1451](https://github.com/influxdata/chronograf/pull/1451): Refactor scrollbars to support non-webkit browsers
1.  [#1453](https://github.com/influxdata/chronograf/pull/1453): Increase the query builder's default height in cell editor mode and in the data explorer
1.  [#1453](https://github.com/influxdata/chronograf/pull/1453): Give QueryMaker a greater initial height than Visualization
1.  [#1475](https://github.com/influxdata/chronograf/pull/1475): Add ability to toggle visibility of the Template Control Bar
1.  [#1464](https://github.com/influxdata/chronograf/pull/1464): Make the [template variables](https://docs.influxdata.com/chronograf/v1.3/guides/dashboard-template-variables/) manager more space efficient
1.  [#1464](https://github.com/influxdata/chronograf/pull/1464): Add page spinners to pages that did not have them
1.  [#1464](https://github.com/influxdata/chronograf/pull/1464): Denote which source is connected in the sources table
1.  [#1464](https://github.com/influxdata/chronograf/pull/1464): Make the logout button consistent with design
1.  [#1478](https://github.com/influxdata/chronograf/pull/1478): Use milliseconds in the InfluxDB dashboard instead of nanoseconds
1.  [#1498](https://github.com/influxdata/chronograf/pull/1498): Notify users when local settings are cleared

## v1.3.0 [2017-05-09]

### Bug Fixes

1.  [#1364](https://github.com/influxdata/chronograf/pull/1364): Fix the link to home when using the `--basepath` option
1.  [#1370](https://github.com/influxdata/chronograf/pull/1370): Remove the notification to login on the login page
1.  [#1376](https://github.com/influxdata/chronograf/pull/1376): Support queries that perform math on functions
1.  [#1399](https://github.com/influxdata/chronograf/pull/1399): Prevent the creation of blank template variables
1.  [#1406](https://github.com/influxdata/chronograf/pull/1406): Ensure thresholds for Kapacitor Rule Alerts appear on page load
1.  [#1412](https://github.com/influxdata/chronograf/pull/1412): Update the Kapacitor configuration page when the configuration changes
1.  [#1407](https://github.com/influxdata/chronograf/pull/1407): Fix Authentication when using Chronograf with a set `basepath`
1.  [#1417](https://github.com/influxdata/chronograf/pull/1417): Support escaping from presentation mode in Safari
1.  [#1365](https://github.com/influxdata/chronograf/pull/1365): Show red indicator on Hosts Page for an offline host
1.  [#1379](https://github.com/influxdata/chronograf/pull/1379): Re-implement level colors on the alerts page
1.  [#1433](https://github.com/influxdata/chronograf/pull/1433): Fix router bug introduced by upgrading to react-router v3.0
1.  [#1435](https://github.com/influxdata/chronograf/pull/1435): Show legend on Line+Stat visualization type
1.  [#1436](https://github.com/influxdata/chronograf/pull/1436): Prevent queries with `:dashboardTime:` from breaking the query builder

### Features

1.  [#1382](https://github.com/influxdata/chronograf/pull/1382): Add line-protocol proxy for InfluxDB/InfluxEnterprise Cluster data sources
1.  [#1391](https://github.com/influxdata/chronograf/pull/1391): Add `:dashboardTime:` to support cell-specific time ranges on dashboards
1.  [#1201](https://github.com/influxdata/chronograf/pull/1201): Add support for enabling and disabling TICKscripts that were created outside Chronograf
1.  [#1401](https://github.com/influxdata/chronograf/pull/1401): Allow users to delete Kapacitor configurations

### UI Improvements

1.  [#1378](https://github.com/influxdata/chronograf/pull/1378): Save user-provided relative time ranges in cells
1.  [#1373](https://github.com/influxdata/chronograf/pull/1373): Improve how cell legends and options appear on dashboards
1.  [#1385](https://github.com/influxdata/chronograf/pull/1385): Combine the measurements and tags columns in the Data Explorer and implement a new design for applying functions to fields
1.  [#602](https://github.com/influxdata/chronograf/pull/602): Normalize the terminology in Chronograf
1.  [#1392](https://github.com/influxdata/chronograf/pull/1392): Make overlays full-screen
1.  [#1395](https://github.com/influxdata/chronograf/pull/1395):Change the default global time range to past 1 hour
1.  [#1439](https://github.com/influxdata/chronograf/pull/1439): Add Source Indicator icon to the Configuration and Admin pages

## v1.2.0-beta10 [2017-04-28]

### Bug Fixes

1.  [#1337](https://github.com/influxdata/chronograf/pull/1337): Add support for blank hostnames on the Host List page
1.  [#1340](https://github.com/influxdata/chronograf/pull/1340): Fix case where the Explorer and cell editor falsely assumed there was no active query
1.  [#1338](https://github.com/influxdata/chronograf/pull/1338): Require url and name when adding a new source
1.  [#1348](https://github.com/influxdata/chronograf/pull/1348): Fix broken `Add Kapacitor` link on the Alerts page - thank you, @nickysemenza

### Features

1.  [#1154](https://github.com/influxdata/chronograf/issues/1154): Add template variables to Chronograf's customized dashboards
1.  [#1351](https://github.com/influxdata/chronograf/pull/1351): Add a canned dashboard for [phpfpm](https://github.com/influxdata/telegraf/tree/master/plugins/inputs/phpfpm) - thank you, @nickysemenza

### UI Improvements

1.  [#1335](https://github.com/influxdata/chronograf/pull/1335): Improve UX for sanitized Kapacitor event handler settings
1.  [#1342](https://github.com/influxdata/chronograf/pull/1342): Fix DB Management's abrupt database sort; only sort databases after refresh/returning to page
1.  [#1344](https://github.com/influxdata/chronograf/pull/1344): Remove the empty, default Kubernetes dashboard
1.  [#1340](https://github.com/influxdata/chronograf/pull/1340): Automatically switch to table view the query is a meta query

## v1.2.0-beta9 [2017-04-21]

### Bug Fixes

1.  [#1257](https://github.com/influxdata/chronograf/issues/1257): Fix function selection in the query builder
1.  [#1244](https://github.com/influxdata/chronograf/pull/1244): Fix the environment variable name for Google client secret
1.  [#1269](https://github.com/influxdata/chronograf/issues/1269): Add more functionality to the explorer's query generation process
1.  [#1318](https://github.com/influxdata/chronograf/issues/1318): Fix JWT refresh for auth-durations of zero and less than five minutes
1.  [#1332](https://github.com/influxdata/chronograf/pull/1332): Remove table toggle from dashboard visualization
1.  [#1335](https://github.com/influxdata/chronograf/pull/1335): Improve UX for sanitized kapacitor settings

### Features

1.  [#1292](https://github.com/influxdata/chronograf/pull/1292): Introduce Template Variable Manager
1.  [#1232](https://github.com/influxdata/chronograf/pull/1232): Fuse the query builder and raw query editor
1.  [#1265](https://github.com/influxdata/chronograf/pull/1265): Refactor the router to use auth and force /login route when auth expires
1.  [#1286](https://github.com/influxdata/chronograf/pull/1286): Add refreshing JWTs for authentication
1.  [#1316](https://github.com/influxdata/chronograf/pull/1316): Add templates API scoped within a dashboard
1.  [#1311](https://github.com/influxdata/chronograf/pull/1311): Display currently selected values in TVControlBar
1.  [#1315](https://github.com/influxdata/chronograf/pull/1315): Send selected TV values to proxy
1.  [#1302](https://github.com/influxdata/chronograf/pull/1302): Add support for multiple Kapacitors per InfluxDB source

### UI Improvements

1.  [#1259](https://github.com/influxdata/chronograf/pull/1259): Add a default display for empty dashboard
1.  [#1258](https://github.com/influxdata/chronograf/pull/1258): Display Kapacitor alert endpoint options as radio button group
1.  [#1321](https://github.com/influxdata/chronograf/pull/1321): Add yellow color to UI, Query Editor warnings are now appropriately colored

## v1.2.0-beta8 [2017-04-07]

### Bug Fixes

1.  [#1104](https://github.com/influxdata/chronograf/pull/1104): Fix Windows hosts on the host list page
1.  [#1125](https://github.com/influxdata/chronograf/pull/1125): Show cell name when editing dashboard cells
1.  [#1134](https://github.com/influxdata/chronograf/pull/1134): Fix Enterprise Kapacitor authentication
1.  [#1142](https://github.com/influxdata/chronograf/pull/1142): Fix Telegram Kapacitor configuration to display the correct disableNotification setting
1.  [#1124](https://github.com/influxdata/chronograf/pull/1124): Fix broken graph spinner in the explorer and when editing dashboard cells
1.  [#1124](https://github.com/influxdata/chronograf/pull/1124): Fix obscured legends in dashboards
1.  [#1149](https://github.com/influxdata/chronograf/pull/1149): Exit presentation mode on dashboards when using the browser back button
1.  [#1152](https://github.com/influxdata/chronograf/pull/1152): Widen single column results in the explorer
1.  [#1164](https://github.com/influxdata/chronograf/pull/1164): Restore ability to save raw queries to a dashboard cell
1.  [#1115](https://github.com/influxdata/chronograf/pull/1115): Fix `--basepath` issue where content would fail to render under certain circumstances
1.  [#1173](https://github.com/influxdata/chronograf/pull/1173): Actually save emails in Kapacitor alerts
1.  [#1178](https://github.com/influxdata/chronograf/pull/1178): Ensure Safari renders the explorer and CellEditorOverlay correctly
1.  [#1182](https://github.com/influxdata/chronograf/pull/1182): Fix empty tags for non-default retention policies
1.  [#1179](https://github.com/influxdata/chronograf/pull/1179): Render databases without retention policies on the admin page
1.  [#1128](https://github.com/influxdata/chronograf/pull/1128): Fix dashboard cell repositioning 👻
1.  [#1189](https://github.com/influxdata/chronograf/pull/1189): Improve dashboard cell renaming UX
1.  [#1202](https://github.com/influxdata/chronograf/pull/1202): Fix server returning unquoted InfluxQL keyword identifiers (e.g. `mean(count)`)
1.  [#1203](https://github.com/influxdata/chronograf/pull/1203): Fix redirect with authentication in Chronograf for InfluxEnterprise
1.  [#1095](https://github.com/influxdata/chronograf/pull/1095): Restore the logout button
1.  [#1209](https://github.com/influxdata/chronograf/pull/1209): Ask for the HipChat subdomain instead of the entire HipChat URL in the HipChat Kapacitor configuration
1.  [#1223](https://github.com/influxdata/chronograf/pull/1223): Use vhost as Chronograf's proxy to Kapacitor
1.  [#1205](https://github.com/influxdata/chronograf/pull/1205): Allow initial source to be an InfluxEnterprise source

### Features

1.  [#1112](https://github.com/influxdata/chronograf/pull/1112): Add ability to delete a dashboard
1.  [#1120](https://github.com/influxdata/chronograf/pull/1120): Allow admins to update user passwords
1.  [#1129](https://github.com/influxdata/chronograf/pull/1129): Allow InfluxDB and Kapacitor configuration via environment vars or CLI options
1.  [#1130](https://github.com/influxdata/chronograf/pull/1130): Add loading spinner to the alert history page
1.  [#1168](https://github.com/influxdata/chronograf/pull/1168): Expand support for `--basepath` on some load balancers
1.  [#1204](https://github.com/influxdata/chronograf/pull/1204): Add Slack channel per Kapacitor alert rule configuration
1.  [#1119](https://github.com/influxdata/chronograf/pull/1119): Add new auth duration CLI option; add client heartbeat
1.  [#1207](https://github.com/influxdata/chronograf/pull/1207): Add support for custom OAuth2 providers
1.  [#1212](https://github.com/influxdata/chronograf/pull/1212): Add meta query templates and loading animation to the RawQueryEditor
1.  [#1221](https://github.com/influxdata/chronograf/pull/1221): Remove the default query from empty cells on dashboards
1.  [#1101](https://github.com/influxdata/chronograf/pull/1101): Compress InfluxQL responses with gzip

### UI Improvements

1.  [#1132](https://github.com/influxdata/chronograf/pull/1132): Show blue strip next to active tab on the sidebar
1.  [#1135](https://github.com/influxdata/chronograf/pull/1135): Clarify Kapacitor alert configuration for Telegram
1.  [#1137](https://github.com/influxdata/chronograf/pull/1137): Clarify Kapacitor alert configuration for HipChat
1.  [#1136](https://github.com/influxdata/chronograf/pull/1136): Remove series highlighting in line graphs
1.  [#1124](https://github.com/influxdata/chronograf/pull/1124): Polish UI:
    * Polish dashboard cell drag interaction
    * Use Hover-To-Reveal UI pattern in all tables
    * Clarify source Indicator & Graph Tips
    * Improve DB Management page UI
1.  [#1187](https://github.com/influxdata/chronograf/pull/1187): Replace kill query confirmation modal with confirm buttons
1.  [#1185](https://github.com/influxdata/chronograf/pull/1185): Alphabetically sort databases and retention policies on the admin page
1.  [#1199](https://github.com/influxdata/chronograf/pull/1199): Move rename cell functionality to cell dropdown menu
1.  [#1211](https://github.com/influxdata/chronograf/pull/1211): Reverse the positioning of the graph visualization and the query builder on the Data Explorer page
1.  [#1222](https://github.com/influxdata/chronograf/pull/1222): Isolate cell repositioning to just those affected by adding a new cell

## v1.2.0-beta7 [2017-03-28]

### Bug Fixes

1.  [#1008](https://github.com/influxdata/chronograf/issues/1008): Fix unexpected redirection to create sources page when deleting a source
1.  [#1067](https://github.com/influxdata/chronograf/issues/1067): Fix issue creating retention policies
1.  [#1068](https://github.com/influxdata/chronograf/issues/1068): Fix issue deleting databases
1.  [#1078](https://github.com/influxdata/chronograf/issues/1078): Fix cell resizing in dashboards
1.  [#1070](https://github.com/influxdata/chronograf/issues/1070): Save GROUP BY tag(s) clauses on dashboards
1.  [#1086](https://github.com/influxdata/chronograf/issues/1086): Fix validation for deleting databases

### Features

### UI Improvements

1.  [#1092](https://github.com/influxdata/chronograf/pull/1092): Persist and render Dashboard Cell groupby queries

## v1.2.0-beta6 [2017-03-24]

### Bug Fixes

1.  [#1065](https://github.com/influxdata/chronograf/pull/1065): Add functionality to the `save` and `cancel` buttons on editable dashboards
2.  [#1069](https://github.com/influxdata/chronograf/pull/1069): Make graphs on pre-created dashboards un-editable
3.  [#1085](https://github.com/influxdata/chronograf/pull/1085): Make graphs resizable again
4.  [#1087](https://github.com/influxdata/chronograf/pull/1087): Hosts page now displays proper loading, host count, and error messages.

### Features

1.  [#1056](https://github.com/influxdata/chronograf/pull/1056): Add ability to add a dashboard cell
2.  [#1020](https://github.com/influxdata/chronograf/pull/1020): Allow users to edit cell names on dashboards
3.  [#1015](https://github.com/influxdata/chronograf/pull/1015): Add ability to edit a dashboard cell
4.  [#832](https://github.com/influxdata/chronograf/issues/832): Add a database and retention policy management page
5.  [#1035](https://github.com/influxdata/chronograf/pull/1035): Add ability to move and edit queries between raw InfluxQL mode and Query Builder mode

### UI Improvements

## v1.2.0-beta5 [2017-03-10]

### Bug Fixes

1.  [#936](https://github.com/influxdata/chronograf/pull/936): Fix leaking sockets for InfluxQL queries
2.  [#967](https://github.com/influxdata/chronograf/pull/967): Fix flash of empty graph on auto-refresh when no results were previously returned from a query
3.  [#968](https://github.com/influxdata/chronograf/issue/968): Fix wrong database used in dashboards

### Features

1.  [#993](https://github.com/influxdata/chronograf/pull/993): Add Admin page for managing users, roles, and permissions for [OSS InfluxDB](https://github.com/influxdata/influxdb) and InfluxData's [Enterprise](https://docs.influxdata.com/enterprise/v1.2/) product
2.  [#993](https://github.com/influxdata/chronograf/pull/993): Add Query Management features including the ability to view active queries and stop queries

### UI Improvements

1.  [#989](https://github.com/influxdata/chronograf/pull/989) Add a canned dashboard for mesos
2.  [#993](https://github.com/influxdata/chronograf/pull/993): Improve the multi-select dropdown
3.  [#993](https://github.com/influxdata/chronograf/pull/993): Provide better error information to users

## v1.2.0-beta4 [2017-02-24]

### Bug Fixes

1.  [#882](https://github.com/influxdata/chronograf/pull/882): Fix y-axis graph padding
2.  [#907](https://github.com/influxdata/chronograf/pull/907): Fix react-router warning
3.  [#926](https://github.com/influxdata/chronograf/pull/926): Fix Kapacitor RuleGraph display

### Features

1.  [#873](https://github.com/influxdata/chronograf/pull/873): Add [TLS](https://github.com/influxdata/chronograf/blob/master/docs/tls.md) support
2.  [#885](https://github.com/influxdata/chronograf/issues/885): Add presentation mode to the dashboard page
3.  [#891](https://github.com/influxdata/chronograf/issues/891): Make dashboard visualizations draggable
4.  [#892](https://github.com/influxdata/chronograf/issues/891): Make dashboard visualizations resizable
5.  [#893](https://github.com/influxdata/chronograf/issues/893): Persist dashboard visualization position
6.  [#922](https://github.com/influxdata/chronograf/issues/922): Additional OAuth2 support for [Heroku](https://github.com/influxdata/chronograf/blob/master/docs/auth.md#heroku) and [Google](https://github.com/influxdata/chronograf/blob/master/docs/auth.md#google)
7.  [#781](https://github.com/influxdata/chronograf/issues/781): Add global auto-refresh dropdown to all graph dashboards

### UI Improvements

1.  [#905](https://github.com/influxdata/chronograf/pull/905): Make scroll bar thumb element bigger
2.  [#917](https://github.com/influxdata/chronograf/pull/917): Simplify the sidebar
3.  [#920](https://github.com/influxdata/chronograf/pull/920): Display stacked and step plot graph types
4.  [#851](https://github.com/influxdata/chronograf/pull/851): Add configuration for [InfluxEnterprise](https://portal.influxdata.com/) meta nodes
5.  [#916](https://github.com/influxdata/chronograf/pull/916): Dynamically scale font size based on resolution

## v1.2.0-beta3 [2017-02-15]

### Bug Fixes

1.  [#879](https://github.com/influxdata/chronograf/pull/879): Fix several Kapacitor configuration page state bugs: [#875](https://github.com/influxdata/chronograf/issues/875), [#876](https://github.com/influxdata/chronograf/issues/876), [#878](https://github.com/influxdata/chronograf/issues/878)
2.  [#872](https://github.com/influxdata/chronograf/pull/872): Fix incorrect data source response

### Features

1.  [#896](https://github.com/influxdata/chronograf/pull/896) Add more docker stats

## v1.2.0-beta2 [2017-02-10]

### Bug Fixes

1.  [#865](https://github.com/influxdata/chronograf/issues/865): Support for String fields compare Kapacitor rules in Chronograf UI

### Features

1.  [#838](https://github.com/influxdata/chronograf/issues/838): Add [detail node](https://docs.influxdata.com/kapacitor/latest/nodes/alert_node/#details) to Kapacitor alerts
2.  [#847](https://github.com/influxdata/chronograf/issues/847): Enable and disable Kapacitor alerts from the alert manager page
3.  [#853](https://github.com/influxdata/chronograf/issues/853): Update builds to use yarn over npm install
4.  [#860](https://github.com/influxdata/chronograf/issues/860): Add gzip encoding and caching of static assets to server
5.  [#864](https://github.com/influxdata/chronograf/issues/864): Add support to Kapacitor rule alert configuration for:
    * HTTP
    * TCP
    * Exec
    * SMTP
    * Alerta

### UI Improvements

1.  [#822](https://github.com/influxdata/chronograf/issues/822): Simplify and improve the layout of the Data Explorer
    * The Data Explorer's intention and purpose has always been the ad hoc and ephemeral exploration of your schema and data.
      The concept of `Exploration` sessions and `Panels` betrayed this initial intention. The DE turned into a "poor man's"
      dashboarding tool. In turn, this introduced complexity in the code and the UI. In the future if I want to save, manipulate,
      and view multiple visualizations this will be done more efficiently and effectively in our dashboarding solution.

## v1.2.0-beta1 [2017-01-27]

### Bug Fixes

1.  [#788](https://github.com/influxdata/chronograf/pull/788): Fix missing fields in data explorer when using non-default retention policy
2.  [#774](https://github.com/influxdata/chronograf/issues/774): Fix gaps in layouts for hosts

### Features

1.  [#779](https://github.com/influxdata/chronograf/issues/779): Add layout for telegraf's diskio system plugin
2.  [#810](https://github.com/influxdata/chronograf/issues/810): Add layout for telegraf's net system plugin
3.  [#811](https://github.com/influxdata/chronograf/issues/811): Add layout for telegraf's procstat plugin
4.  [#737](https://github.com/influxdata/chronograf/issues/737): Add GUI for OpsGenie kapacitor alert service
5.  [#814](https://github.com/influxdata/chronograf/issues/814): Allows Chronograf to be mounted under any arbitrary URL path using the `--basepath` flag.

## v1.1.0-beta6 [2017-01-13]

### Bug Fixes

1.  [#748](https://github.com/influxdata/chronograf/pull/748): Fix missing kapacitors on source index page
2.  [#755](https://github.com/influxdata/chronograf/pull/755): Fix kapacitor basic auth proxying
3.  [#704](https://github.com/influxdata/chronograf/issues/704): Fix RPM and DEB install script and systemd unit file

### Features

1.  [#660](https://github.com/influxdata/chronograf/issues/660): Add option to accept any certificate from InfluxDB
2.  [#733](https://github.com/influxdata/chronograf/pull/733): Add optional Github organization membership checks to authentication
3.  [#564](https://github.com/influxdata/chronograf/issues/564): Add RabbitMQ pre-canned layout
4.  [#706](https://github.com/influxdata/chronograf/issues/706): Alerts on threshold where value is inside of range
5.  [#707](https://github.com/influxdata/chronograf/issues/707): Alerts on threshold where value is outside of range
6.  [#772](https://github.com/influxdata/chronograf/pull/772): Add X-Chronograf-Version header to all requests

### UI Improvements

1.  [#766](https://github.com/influxdata/chronograf/pull/766): Add click-to-insert functionality to rule message templates

## v1.1.0-beta5 [2017-01-05]

### Bug Fixes

1.  [#693](https://github.com/influxdata/chronograf/issues/693): Fix corrupted MongoDB pre-canned layout
2.  [#714](https://github.com/influxdata/chronograf/issues/714): Relative rules check data in the wrong direction
3.  [#718](https://github.com/influxdata/chronograf/issues/718): Fix bug that stopped apps from displaying

## v1.1.0-beta4 [2016-12-30]

### Features

1.  [#691](https://github.com/influxdata/chronograf/issues/691): Add server-side dashboard API
2.  [#709](https://github.com/influxdata/chronograf/pull/709): Add kapacitor range alerting to API
3.  [#672](https://github.com/influxdata/chronograf/pull/672): Added visual indicator for down hosts
4.  [#612](https://github.com/influxdata/chronograf/issues/612): Add dashboard menu

### Bug Fixes

1.  [679](https://github.com/influxdata/chronograf/issues/679): Fix version display

## v1.1.0-beta3 [2016-12-16]

### Features

1.  [#610](https://github.com/influxdata/chronograf/issues/610): Add ability to edit raw text queries in the Data Explorer

### UI Improvements

1.  [#688](https://github.com/influxdata/chronograf/issues/688): Add ability to visually distinguish queries in the Data Explorer
1.  [#618](https://github.com/influxdata/chronograf/issues/618): Add measurement name and field key to the query tab in the Data Explorer
1.  [#698](https://github.com/influxdata/chronograf/issues/698): Add color differentiation for Kapacitor alert levels
1.  [#698](https://github.com/influxdata/chronograf/issues/698): Clarify an empty Kapacitor configuration on the InfluxDB Sources page
1.  [#676](https://github.com/influxdata/chronograf/issues/676): Streamline the function selector in the Data Explorer

### Bug Fixes

1.  [#652](https://github.com/influxdata/chronograf/issues/652),[#670](https://github.com/influxdata/chronograf/issues/670): Allow text selecting in text box inputs
2.  [#679](https://github.com/influxdata/chronograf/issues/679): Add version information to the nightly builds
3.  [#675](https://github.com/influxdata/chronograf/issues/675): Fix user flow for Kapacitor connect

## v1.1.0-beta2 [2016-12-09]

### Features

1.  [#624](https://github.com/influxdata/chronograf/issues/624): Add time range selection to kapacitor alert rules
1.  Update Go to 1.7.4

### Bug Fixes

1.  [#664](https://github.com/influxdata/chronograf/issues/664): Fix Content-Type of single-page app to always be text/html
1.  [#671](https://github.com/influxdata/chronograf/issues/671): Fix multiple influxdb source freezing page

## v1.1.0-beta1 [2016-12-06]

### Layouts

1.  [#575](https://github.com/influxdata/chronograf/issues/556): Varnish Layout
2.  [#535](https://github.com/influxdata/chronograf/issues/535): Elasticsearch Layout

### Features

1.  [#565](https://github.com/influxdata/chronograf/issues/565) [#246](https://github.com/influxdata/chronograf/issues/246) [#234](https://github.com/influxdata/chronograf/issues/234) [#311](https://github.com/influxdata/chronograf/issues/311) Github Oauth login
2.  [#487](https://github.com/influxdata/chronograf/issues/487): Warn users if they are using a kapacitor instance that is configured to use an influxdb instance that does not match the current source
3.  [#597](https://github.com/influxdata/chronograf/issues/597): Filter host by series tags
4.  [#568](https://github.com/influxdata/chronograf/issues/568): [#569](https://github.com/influxdata/chronograf/issues/569): Add support for multiple y-axis, labels, and ranges
5.  [#605](https://github.com/influxdata/chronograf/issues/605): Singlestat visualization type in host view
6.  [#607](https://github.com/influxdata/chronograf/issues/607): Singlestat and line graph visualization type in host view

### Bug Fixes

1.  [#536](https://github.com/influxdata/chronograf/issues/536) Redirect the user to the kapacitor config screen if they are attempting to view or edit alerts without a configured kapacitor
2.  [#539](https://github.com/influxdata/chronograf/issues/539) Zoom works only on the first graph of a layout
3.  [#494](https://github.com/influxdata/chronograf/issues/494) Layouts should only be displayed when the measurement is present
4.  [#588](https://github.com/influxdata/chronograf/issues/588) Unable to connect to source
5.  [#586](https://github.com/influxdata/chronograf/issues/586) Allow telegraf database in non-default locations
6.  [#542](https://github.com/influxdata/chronograf/issues/542) Graphs in layouts do not show up in the order of the layout definition
7.  [#574](https://github.com/influxdata/chronograf/issues/574): Fix broken graphs on Postgres Layouts by adding aggregates
8.  [#644](https://github.com/influxdata/chronograf/pull/644): Fix bug that stopped apps from displaying
9.  [#510](https://github.com/influxdata/chronograf/issues/510): Fix connect button

## v1.1-alpha [2016-11-14]

### Release Notes

This is the initial alpha release of Chronograf 1.1.
