@feature-dataExplorer
@dataExplorer-dataExplorer
Feature: Data explorer
  As a user I want to Create queries in Data Explorer

  Scenario: Create a new bucket and add data
    Given I reset the environment
    Given run setup over REST "DEFAULT"
    When open the signin page
    When UI sign in user "DEFAULT"
    When click nav menu item "LoadData"
    Then the buckets tab is loaded
    When click the Create Bucket button
    Then the Create Bucket Popup is loaded
    When input the name of the bucket as "bucket_explorer_test"
    When click the Create Bucket popup Create button
    Then the bucket named "bucket_explorer_test" is in the list
    When click add data button for bucket "bucket_explorer_test"
    Then the add data popover for the bucket "bucket_explorer_test" is visible
    When click bucket card popover item "Line Protocol"
    Then the first page of the Line Protocol Wizard is loaded
    When click radio button "Enter Manually"
    Then the data point text area is visible
    When enter "12" datapoints with value named "foo" starting at "-2h" with "fibonacci" data of type "int" and prec "ms"
    When click the Line Protocol wizard precision dropdown
    When click the line Protocol wizard precision "ms"
    #When enter "<any>" into the line protocol text area
    When click the Line Protocol wizard continue button
    Then the Line Protocol wizard step status message is "Data Written Successfully"
    When click the Line Protocol wizard finish button


  Scenario: Create a simple query
    #When click nav menu item "Explorer"
    When click on the bucket named "bucket_explorer_test"
    Then the Data Explorer page is loaded
    When choose bucket named "bucket_explorer_test"
    When choose the item "fibonacci" in builder card "1"
    When choose the item "foo" in builder card "2"
    When choose the item "bucketSteps" in builder card "3"
    When click the submit button
    Then the time machine graph is shown

  Scenario: Hover over graph
    When hover over the graph
    Then the graph data point infobox is visible

  Scenario: Zoom graph horizontal
    When get the current graph
    When move horizontally to "2/5" of the graph
    When drag horizontally to "3/5" of the graph
    Then the graph has changed

  Scenario: Unzoom graph
    When get the current graph
    When Click at the point "{"x": "1/2", "y": "1/2"}" of the graph
    Then the graph has changed

  Scenario: Zoom graph vertical and refresh
    When get the current graph
    When move vertically to "2/5" of the graph
    When drag vertically to "3/5" of the graph
    Then the graph has changed
    When click the refresh button
    Then the graph has changed

  Scenario: Turn on the automatic refresh and check the graph changes
    When click the refresh button
    When get the current graph
    When click the automatic refresh - paused
    When select the automatic refresh "5s"
    When wait "6" seconds
    Then the graph has changed
    When click the automatic refresh - active
    When select the automatic refresh "paused"

  Scenario: Create second query with Script Editor
    When click the time machine add query button
    Then there are "2" time machine query tabs
    #When right click on the time machine query tab named "Query 2"
    #When click on "edit" in the query tab menu
    #When input a new query tab name as "testQueryTab"
    When click the Script Editor button
    When paste into Script Editor text area
  """
  from(bucket: "bucket_explorer_test")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "fibonacci")
  """
    When click the submit button
    Then the time machine graph is shown
    Then the graph has changed

  Scenario: Change graph view type
    When click the graph view type dropdown
    When select the graph view type "Graph + Single Stat"
    Then the graph view type is Graph plus Single Stat

  Scenario: Change the graph time range and view raw data
    When click the graph time range dropdown
    When select the graph time range "6h"
    Then the graph has changed
    When click the filter functions input
    When click the view raw data toggle
    Then the raw data table is visible

  Scenario: Use aggregate function
    When click the view raw data toggle
    Then the time machine graph is shown
    When click on the time machine query tab named "Query 1"
    #When search for function "mean"
    When select the function "mean"
    When click the submit button
    Then the graph has changed

  Scenario: Delete query tab
    When right click on the time machine query tab named "Query 2"
    When click on "remove" in the query tab menu
    Then there are "1" time machine query tabs

  Scenario: Save query as a dashboard cell
    When click on the Save as button
    Then the Save as overlay is visible
    When click the Target Dashboard dropdown
    When select Create a New Dashboard from the Target Dashboard dropdown
    When input the New Dashboard Name "Dashboard Cell Test"
    When input the Cell Name "DC Test Name"
    When click on the Save as Dashboard Cell button
    Then the success notification contains "Created dashboard successfully"
    When click nav menu item "dashboards"
    Then there is a dashboard card named "Dashboard Cell Test"
    When click the dashboard name "Dashboard Cell Test"
    Then the cell named "DC Test Name" is visible in the dashboard

  Scenario: Save query as a task
    When click nav menu item "Explorer"
    When click the submit button
    When click on the Save as button
    When click on tab "Task" in the Save As popup
    When input the Task Name "Task Test"
    When input the Task Interval "4h"
    When input the Task Offset "10"
    When click on the Save as Task button
    Then the error notification contains "Failed to create new task:"
    When input the Task Offset "10m"
    When click on the Save as Task button
    Then the success notification contains "New task created successfully"
    Then there is a task named "Task Test"

  Scenario: Save query as a variable
    When click nav menu item "Explorer"
    When click the submit button
    When click on the Save as button
    When click on tab "Variable" in the Save As popup
    When input the variable name "Variable Test"
    When click on the Save as Variable button
    Then the success notification contains "Successfully created new variable:"
    When click nav menu item "Settings"
    When there is a variable card for "Variable Test"





