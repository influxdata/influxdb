@feature-dashboards
@dashboards-cellEdit
Feature: Dashboards - Dashboard - Cell Edit
  As a user I want to Create and Update Cells
  So that I can view specific Influxdbv2 data

@tested
  Scenario: Load Cell Edit View
    Given I reset the environment
    Given run setup over REST "DEFAULT"
    When open the signin page
    When UI sign in user "DEFAULT"
    When click nav menu item "Dashboards"
#    When hover over the "Dashboards" menu item
#    When click nav sub menu "Dashboards"
    Then the Dashboards page is loaded
    When API sign in user "DEFAULT"
    When generate a line protocol testdata for user "DEFAULT" based on:
    """
    { "points": 2880, "measurement":"pulse", "start": "-48h", "algo": "log", "prec": "sec", "name": "beat"}
    """
    When generate a line protocol testdata for user "DEFAULT" based on:
    """
    { "points": 7200, "measurement":"signal", "start": "-30d", "algo": "sine", "prec": "sec", "name": "foo"}
    """
    When click the empty Create dashboard dropdown button
    When click the create dashboard item "New Dashboard"
    Then the new dashboard page is loaded
    Then the empty dashboard contains a documentation link
    Then the empty dashboard contains Add a Cell button
    When name dashboard "Fitness"

    # +page-title - edit name this cell # covered in dashboard test
    # +Graph dropdown
    # +cog-cell--button
    # +cancel-cell-edit--button
    # +save-cell--button # already covered in dashboard test
    # time-machine--view
       # +empty-graph--no-queries
       # +giraffe-autosizer
       # +raw-data-table
    # time-machine--bottom
       # +raw-data--toggle
       # +CSV download
       # +Refresh Rate
       # +Reload
       # +Time Range
       # +switch-to-script-editor
       # +Submit#
       # +Add Query
       # +Queries
          # +Builder
             # +Schema navigator
             # +Aggregate functions
          # +Script Editor
             # +Aggregate functions
             # +Variables

@tested
  Scenario: Exercise Basic Cell Edit Controls
    When click the empty create cell button
    Then the cell edit overlay is loaded as "Name this Cell"
    When name dashboard cell "Kliky"
    When click dashboard cell edit cancel button
    Then there is no dashboard cell named "Kliky"
    When click the empty create cell button
    Then the cell edit overlay is loaded as "Name this Cell"
    When name dashboard cell "Kliky"
    When click the dashboard cell view type dropdown
    Then the dashboard cell view type dropdown list contains:
  """
  xy,line-plus-single-stat,heatmap,histogram,single-stat,gauge,table,scatter
  """
    When click the dashboard cell view type dropdown
    Then the cell view type dropdown list is not present
    When click cell view customize button
    Then the view options container is present
    Then the cell view customize button is highlighted
    When click cell view customize button
    Then the view options container is not present
    Then the cell view customize button is not highlighted
    Then the time machine view empty queries graph is visible
    When click time machine autorefresh dropdown
    Then the time machine autorefresh dropdown list contains:
  """
  Paused,5s,10s,30s,60s
  """
    When select the time machine autorefresh rate "60s"
    Then the time machine force refresh button is not present
    When click time machine autorefresh dropdown
    When select the time machine autorefresh rate "Paused"
    Then the time machine force refresh button is present
    When click the cell edit Time Range Dropdown
    Then the time machine Time Range dropdown list contains:
  """
  Custom Time Range,Past 5m,Past 15m,Past 1h,Past 6h,Past 12h,Past 24h,Past 2d,Past 7d,Past 30d
  """
    When click the cell edit Time Range Dropdown
    Then the time machine Time Range dropdown list is not present
    Then the time machine query builder is visible
    When click the cell edit Script Editor button
    Then the time machine flux editor is visible
    When click the cell edit Query Builder button
    When click the time machine flux editor
    Then the time machine flux editor is visible
    Then the time machine switch to Query Builder warning is not present
    When click the cell edit Query Builder button
    When click the cell edit Query Builder confirm button
    Then the time machine query builder is visible
    Then the time machine switch to Query Builder warning is not present
    Then the time machine flux editor is not present
    When click dashboard cell save button
    Then the dashboard contains a cell named "Kliky"

    # ~~page-title -- name edit
    # ~~Graph drop down
    # ~~cog-cell--button
    # cancel-cell-edit--button
    # save-cell-edit--button
    # ~~time-machine--view
       # ~~empty-graph--no-queries
    # time-machine--bottom
       # ~~Refresh Rate -- N.B. pause has update button which disappears with other refresh rate values
       # ~~Time Range
       # ~~switch-to-script-editor
       # ~~switch-to-query-builder

@tested
  Scenario: Exercise Query Builder
    When toggle context menu of dashboard cell named "Kliky"
    When click cell content popover configure
    Then the cell edit overlay is loaded as "Kliky"
    Then the time machine cell edit submit button is disabled
    Then the edit cell bucket selector contains buckets:
  """
  qa,_monitoring,_tasks
  """
    When filter the time machine bucket selector with "t"
    Then the edit cell bucket selector contains buckets:
  """
  _monitoring,_tasks
  """
    Then the bucket "qa" is not present in the time machine bucket selector
    When clear the time machine bucket selector filter
    Then the edit cell bucket selector contains buckets:
  """
  qa,_monitoring,_tasks
  """
    When click the time machine bucket selector item "_monitoring"
    Then time machine bulider card "1" contains the empty tag message
    When click the time machine bucket selector item "qa"
    Then there are "1" time machine builder cards
    Then time machine builder card "1" contains:
  """
  beat,foo
  """
    When click the tag selector dropdown of builder card "1"
    Then the tag selector dropdown of builder card "1" contains:
  """
  _field,_measurement,test
  """
    When click the tag selector dropdown item "_field" of builder card "1"
    Then time machine builder card "1" contains:
  """
  pulse,signal
  """
    When click the tag selector dropdown of builder card "1"
    When click the tag selector dropdown item "_measurement" of builder card "1"
    Then time machine builder card "1" contains:
  """
  beat,foo
  """
    When filter the tags in time machine builder card "1" with "eat"
    Then time machine builder card "1" does not contain "foo"
    When click the tag "beat" in builder card "1"
    Then the time machine cell edit submit button is enabled
    Then there are "2" time machine builder cards
    Then time machine builder card "2" contains:
  """
    pulse
  """
    When filter the tags in time machine builder card "2" with "ratfink"
    Then time machine builder card "2" is empty
    When clear the tags filter in time machine builder card "2"
    Then time machine builder card "2" contains:
  """
    pulse
  """
    When clear the tags filter in time machine builder card "1"
    Then time machine builder card "1" contains:
  """
  beat,foo
  """
    When click the tag "foo" in builder card "1"
    Then time machine builder card "2" contains:
  """
    pulse,signal
  """
    Then the selector count for builder card "1" contains the value "2"
    When click the tag selector dropdown of builder card "2"
    Then the tag selector dropdown of builder card "2" contains:
  """
  _field,test
  """
    When click the tag selector dropdown of builder card "2"
    Then the contents of tag selector dropodwn of build card "2" are not present
    When click the tag "foo" in builder card "1"
    Then the selector count for builder card "1" contains the value "1"
    When click the tag "beat" in builder card "1"
    Then the selector counf for builder card "1" is not present
    Then the delete button for builder card "1" is not present
    When click delete for builder card "2"
    Then there are "1" time machine builder cards
  # Check coverage of issue 16682 once fixed

@tested
  Scenario: Exercise Query Builder Functions
    # TODO 16682 - following will not have been reset
    # Then the time machine query builder function duration period is "auto"
    When click the tag "beat" in builder card "1"
    Then the time machine query builder function duration period is "auto (10s)"
    Then the query builder function list contains
  """
  mean, median, max, min, sum, derivative, nonnegative derivative, distinct, count, increase,
  skew, spread, stddev, first, last, unique, sort
  """
    When filter the query builder function list with "rx"
    Then the query builder function list has "0" items
    When clear the query builder function lis filter
    Then the query builder function list contains
  """
  mean, median, max, min, sum, derivative, nonnegative derivative, distinct, count, increase,
  skew, spread, stddev, first, last, unique, sort
  """
    When filter the query builder function list with "in"
    Then the query builder function list contains
  """
  min,distinct,increase
  """
    Then the query builder function list has "3" items
    When click the time machine query builder function duration input
    Then the query builder function duration suggestion drop down contains "14" suggestions
    Then the query builder function duration suggestion drop down includes
  """
    auto (10s),none,5s,15s,1m,5m,15m,1h,6h,12h,24h,2d,7d,30d
  """
    When click the query builder function duration suggestion "7d"
    Then the time machine query builder function duration period is "7d"
    When click the time machine query builder function duration input
    When click the query builder function duration suggestion "auto (10s)"
    Then the time machine query builder function duration period is "auto (10s)"
    When click dashboard cell edit cancel button

@tested
  Scenario: Create basic query
    Then the cell named "Kliky" contains the empty graph message
    When toggle context menu of dashboard cell named "Kliky"
    When click cell content popover configure
    Then the time machine view no results is visible
    Then the time machine cell edit submit button is disabled
    When click the time machine bucket selector item "qa"
    When click the tag "beat" in builder card "1"
    Then the time machine cell edit submit button is enabled
    When click the tag "pulse" in builder card "2"
    When click the time machine cell edit submit button
    Then the time machine cell edit preview graph is shown
    Then the time machine cell edit preview axes are shown

@error-collateral
  Scenario: Resize Preview
    When get metrics of time machine cell edit preview
    When get metrics of time machine query builder
    When get time machine preview canvas
    When get time machine preview axes
  ## Resize - larger
    When resize time machine preview area by "{ "dw": "0", "dh": "+200" }"
  #  When wait "3" seconds
  # Compare new dims Dashboard steps 387
    Then the time machine preview area has changed by "{ "dw": "0", "dh": "+200" }"
    Then the time machine query builder area has changed by "{ "dw": "0", "dh": "-200" }"
    Then the time machine preview canvas has changed
    Then the time machine preview axes have changed
  # Git dims again
    When get metrics of time machine cell edit preview
    When get metrics of time machine query builder
    When get time machine preview canvas
    When get time machine preview axes
  # Resize - smaller
    When resize time machine preview area by "{ "dw": "0", "dh": "-300" }"
  # Compare new dims
    Then the time machine preview area has changed by "{ "dw": "0", "dh": "-300" }"
    Then the time machine query builder area has changed by "{ "dw": "0", "dh": "+300" }"
    Then the time machine preview canvas has changed
    Then the time machine preview axes have changed
    When click dashboard cell save button
    Then the cell named "Kliky" contains a graph

@error-collateral
  Scenario: Create Second Query
    When get the current graph of the cell "Kliky"
    When toggle context menu of dashboard cell named "Kliky"
    When click cell content popover configure
    Then the time machine cell edit preview graph is shown
    Then the time machine cell edit preview axes are shown
    Then the bucket selected in the current time machine query is "qa"
    Then the tag selected in the current time machine query card "1" is "beat"
    Then the tag selected in the current time machine query card "2" is "pulse"
    When get time machine preview canvas
    When get time machine preview axes
    When click the time machine query builder add query button
    Then the bucket selected in the current time machine query is "qa"
    Then there are "1" time machine builder cards
    Then time machine builder card "1" contains:
  """
  beat,foo
  """
    Then there are no selected tags in time machine builder card "1"
    When click the tag "foo" in builder card "1"
    When click the tag "signal" in builder card "2"
    When click the query builder function "mean"
    When click the time machine query builder function duration input
    When click the query builder function duration suggestion "1m"
    When click the time machine cell edit submit button
    Then the time machine preview canvas has changed
    Then the time machine preview axes have changed
    When click dashboard cell save button
    Then the graph of the cell "Kliky" has changed

@error-collateral
  Scenario: Change Query Name
    When get the current graph of the cell "Kliky"
    When toggle context menu of dashboard cell named "Kliky"
    When click cell content popover configure
    Then query "Query 1" is the active query in query builder
    When click on query "Query 2" in the query builder
    Then the bucket selected in the current time machine query is "qa"
    Then the tag selected in the current time machine query card "1" is "foo"
    Then the tag selected in the current time machine query card "2" is "signal"
    Then the functions selected in the current time machine query card are "mean"
    When right click on the time machine query tab title "Query 2"
    When click the time machine query tab right click menu item "Edit"
    When enter "Dotaz B" into the time machine query tab name input
    Then there is no time machine query tab named "Query 2"
    Then query "Dotaz B" is the active query in query builder

@error-collateral
  Scenario: Hide Query
    When get time machine preview canvas
    When get time machine preview axes
    When click hide query of time machine query tab "Dotaz B"
    Then the time machine preview canvas has changed
    Then the time machine preview axes have changed
    When get time machine preview canvas
    When get time machine preview axes
    When click hide query of time machine query tab "Dotaz B"
    Then the time machine preview canvas has changed
    Then the time machine preview axes have changed
    When click dashboard cell save button

@tested
  Scenario: Delete Second Query
    When toggle context menu of dashboard cell named "Kliky"
    When click cell content popover configure
    When get the current graph of the cell "Kliky"
    When right click the time machine query tab "Dotaz B"
    When get time machine preview canvas
    When get time machine preview axes
    #When click delete of time machine query tab "Dotaz B"
    When click the time machine query tab right click menu item "remove"
    Then there is no time machine query tab named "Dotaz B"
    Then there are "1" time machine query tabs
    Then the time machine preview canvas has changed
    Then the time machine preview axes have changed
    When click dashboard cell save button
    Then the graph of the cell "Kliky" has changed

@tested
  Scenario: Edit Query
    When get the current graph of the cell "Kliky"
    When toggle context menu of dashboard cell named "Kliky"
    When click cell content popover configure
    When get time machine preview canvas
    When get time machine preview axes
    When click the cell edit Script Editor button
    Then the time machine script editor contains
  """
  from(bucket: "qa")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) => r["_measurement"] == "beat")
    |> filter(fn: (r) => r["_field"] == "pulse")
  """
    When change the time machine script editor contents to:
  """
  from(bucket: "qa")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "foo")
  |> filter(fn: (r) => r["_field"] == "signal")
  """
    When click the time machine cell edit submit button
    Then the time machine preview canvas has changed
    Then the time machine preview axes have changed
    When click the cell edit save button
    Then the graph of the cell "Kliky" has changed

@tested
  Scenario: Switch to Query Builder
    When get the current graph of the cell "Kliky"
    When toggle context menu of dashboard cell named "Kliky"
    When click cell content popover configure
    Then the time machine script editor contains
  """
  from(bucket: "qa")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "foo")
  |> filter(fn: (r) => r["_field"] == "signal")
  """
    When get time machine preview canvas
    When get time machine preview axes
    When click the cell edit Query Builder button
    Then the time machine switch to Query Builder warning is present
    When click the time machine flux editor
    Then the time machine switch to Query Builder warning is not present
    When click the cell edit Query Builder button
    When click the cell edit Query Builder confirm button
    Then the time machine query builder is visible
    # Issue 16731 todo - check how query is reflected in query builder state
    Then the time machine preview canvas has changed
    Then the time machine preview axes have changed
    When click dashboard cell edit cancel button
    Then the graph of the cell "Kliky" has not changed

@tested
  Scenario:  Edit invalid query
    When get the current graph of the cell "Kliky"
    When toggle context menu of dashboard cell named "Kliky"
    When click cell content popover configure
    Then the time machine script editor contains
  """
  from(bucket: "qa")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "foo")
  |> filter(fn: (r) => r["_field"] == "signal")
  """
    When change the time machine script editor contents to:
  """
  Muffin Man
  """
    When click the time machine cell edit submit button
    Then the time machine preview canvas is not present
    Then the time machine preview canvas axes are not present
    Then the time machine empty graph error message is:
  """
  error @1:1-1:7: undefined identifier Muffin
  """
    When click the cell edit save button
    Then the cell named "Kliky" contains a graph error
    # Popover has been replaced with message in cell
    #When hover over the error icon of the cell "Kliky"
    Then the cell error message of the cell named "Kliky" is:
  """
  error @1:1-1:7: undefined identifier Muffin
  """

@tested
  Scenario: Exercise Add functions - Query Builder
    When toggle context menu of dashboard cell named "Kliky"
    When click cell content popover configure
    When click the cell edit Query Builder button
    When click the cell edit Query Builder confirm button
    # TODO Clean up garbage from issue 16731
    When close all time machine builder cards
    When unselect any tags in time machine builder card "1"
    When click the time machine bucket selector item "qa"
    When click the tag "beat" in builder card "1"
    Then the time machine cell edit submit button is enabled
    When click the tag "pulse" in builder card "2"
    When click the time machine cell edit submit button
    When get time machine preview canvas
    When get time machine preview axes
    When click the time machine query builder function duration input
    When click the query builder function duration suggestion "auto (10s)"
    When click the query builder function "mean"
    When click the time machine cell edit submit button
    Then the time machine preview canvas has changed
    Then the time machine preview axes have changed
    When get time machine preview canvas
    When get time machine preview axes
    When click the time machine query builder function duration input
    When click the query builder function duration suggestion "1m"
    When click the time machine cell edit submit button
    Then the time machine preview canvas has changed
    Then the time machine preview axes have changed
    When get time machine preview canvas
    When get time machine preview axes
    When click the query builder function "mean"
    When click the query builder function "derivative"
    When click the time machine cell edit submit button
    Then the time machine preview canvas has changed
    Then the time machine preview axes have changed
    When get time machine preview canvas
    When get time machine preview axes
    When click the query builder function "derivative"
    When click the time machine cell edit submit button
    Then the time machine preview canvas has changed
    Then the time machine preview axes have changed
    When click dashboard cell save button
    Then the cell named "Kliky" contains a graph

@tested
  Scenario: Edit Query - Exercise functions
    When get the current graph of the cell "Kliky"
    When toggle context menu of dashboard cell named "Kliky"
    When click cell content popover configure
    When click the cell edit Script Editor button
    Then the time machine script editor contains
  """
  from(bucket: "qa")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) => r["_measurement"] == "beat")
    |> filter(fn: (r) => r["_field"] == "pulse")
  """
    Then the time machine query edit function categories are displayed:
  """
  Aggregates,Inputs,Type Conversions,Selectors,Transformations,Outputs,Miscellaneous,Tests
  """
    When filter the time machine query edit function list with "average"
    Then the following function are visible in the time machine function list:
  """
   exponentialMovingAverage,movingAverage,timedMovingAverage,highestAverage,lowestAverage
  """
    Then the following function are not visible in the time machine function list:
  """
   doubleEMA,pearsonr,sum,first,lowestCurrent
  """
    When clear the time machine query edit function list filter
    Then the following function are visible in the time machine function list:
  """
   doubleEMA,pearsonr,sum,first,lowestCurrent
  """
    When hover over time machine query edit function "skew"
    Then the time machine query edit function popup description contains:
  """
  Outputs the skew of non-null records as a float.
  """
    Then the time machine query edit function popup snippet contains:
  """
  skew(column: "_value")
  """
    When hover over the time machine query editor submit button
    Then the time machine query edit function popup is not visible
    When click dashboard cell edit cancel button

@tested
  Scenario: Edit Query - Add a Function
    When get the current graph of the cell "Kliky"
    When toggle context menu of dashboard cell named "Kliky"
    When click cell content popover configure
    When click the cell edit Script Editor button
    Then the time machine script editor contains
  """
  from(bucket: "qa")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) => r["_measurement"] == "beat")
    |> filter(fn: (r) => r["_field"] == "pulse")
  """
    When get time machine preview canvas
    When get time machine preview axes
    When click the time machine flux editor
    #When click the time machine query editor function "aggregateWindow"
    When click inject the time machine query editor function "aggregateWindow"
    Then the time machine script editor contains
  """
  from(bucket: "qa")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) => r["_measurement"] == "beat")
    |> filter(fn: (r) => r["_field"] == "pulse")
    |> aggregateWindow(every: v.windowPeriod, fn: mean)
  """
    # In CircleCi function popup can obscure the submit button
    #When click the time machine flux editor
    When click the filter functions input
    When click the time machine cell edit submit button
    Then the time machine preview canvas has changed
    When click the cell edit save button
    Then the graph of the cell "Kliky" has changed

@error-collateral
  Scenario: Change time range
    When get the current graph of the cell "Kliky"
    When toggle context menu of dashboard cell named "Kliky"
    When click cell content popover configure
    When get time machine preview canvas
    When get time machine preview axes
    When click the cell edit Time Range Dropdown
    When select the cell edit Time Range "past24h"
    #When click the time machine flux editor
    When click the filter functions input
    Then the time machine preview canvas has changed
    Then the time machine preview axes have changed
    When click the cell edit save button
    Then the graph of the cell "Kliky" has changed
    Then the dashboard Time Range Dropdown selected contains "Past 24h"
    #When click the dashboard Time Range Dropdown
    #When select dashboard Time Range "24h"
    #Then the graph of the cell "Kliky" has changed

@error-collateral
  Scenario: View raw data
    When toggle context menu of dashboard cell named "Kliky"
    When click cell content popover configure
    When get time machine preview canvas
    When get time machine preview axes
    Then the time machine raw data table is not present
    When click time machine raw data toggle
    Then the time machine raw data table is present
    Then the time machine preview canvas is not present
    When click time machine raw data toggle
    Then the time machine preview canvas has changed
    Then the time machine raw data table is not present
    When click dashboard cell edit cancel button
    Then the cell named "Kliky" contains a graph


@error-collateral
  Scenario: Download results as CSV
    When remove files ".*chronograf_data.csv" if exists
    When toggle context menu of dashboard cell named "Kliky"
    When click cell content popover configure
    When click time machine download CSV
    Then a file matching ".*chronograf_data.csv" exists
    When verify first CSV file matching ".*chronograf_data.csv" as containing
     """
     { "_time": "type:date", "_value": "type:double", "_field": "pulse", "_measurement": "beat", "test": "generic" }
     """
    When click dashboard cell edit cancel button
    Then the cell named "Kliky" contains a graph

@error-collateral
  Scenario: Refresh Rates
    #earlier signin may have timed out
    When API sign in user "DEFAULT"
    When toggle context menu of dashboard cell named "Kliky"
    When click cell content popover configure
    When get time machine preview canvas
    When get time machine preview axes
    When wait "20" seconds
    When generate a line protocol testdata for user "DEFAULT" based on:
    """
    { "points": 5, "measurement":"pulse", "start": "-5m", "algo": "log", "prec": "sec", "name": "beat"}
    """
    Then the time machine force refresh button is present
    Then the time machine autorefresh dropdown list is set to "Paused"
    When click time machine force refresh
    Then the time machine preview canvas has changed
    Then the time machine preview axes have changed
    When get time machine preview canvas
    When get time machine preview axes
    When click time machine autorefresh dropdown
    When select the time machine autorefresh rate "10s"
    Then the time machine force refresh button is not present
    When generate a line protocol testdata for user "DEFAULT" based on:
    """
    { "points": 5, "measurement":"pulse", "start": "-5m", "algo": "log", "prec": "sec", "name": "beat"}
    """
    When wait "20" seconds
    Then the time machine preview canvas has changed
    Then the time machine preview axes have changed
    When click time machine autorefresh dropdown
    When select the time machine autorefresh rate "Paused"
    When get time machine preview canvas
    When get time machine preview axes
    When generate a line protocol testdata for user "DEFAULT" based on:
    """
    { "points": 5, "measurement":"pulse", "start": "-5m", "algo": "log", "prec": "sec", "name": "beat"}
    """
    When wait "20" seconds
    Then the time machine preview canvas has not changed
