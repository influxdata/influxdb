@feature-dashboards
@dashboards-dashboard
Feature: Dashboards - Dashboard - Base
  As a user I want to Read Create Update and Delete a Dashboard
  So that I can view specific Influxdbv2 data

@tested
  Scenario: Load Initial Dashboard view
    Given I reset the environment
    Given run setup over REST "DEFAULT"
    When open the signin page
    When UI sign in user "DEFAULT"
    When click nav menu item "Dashboards"
#    When hover over the "Dashboards" menu item
#    When click nav sub menu "Dashboards"
    Then the Dashboards page is loaded
    When API sign in user "DEFAULT"
    When API create a label "Cesko" described as "Pravda vitezi" with color "#AAFFAA" for user "DEFAULT"
    When API create a label "Mesto" described as "Matka mest" with color "#FFAAAA" for user "DEFAULT"
    When generate a line protocol testdata for user "DEFAULT" based on:
    """
    { "points": 120, "measurement":"level", "start": "-30d", "algo": "hydro", "prec": "sec", "name": "foo"}
    """
    When generate a line protocol testdata for user "DEFAULT" based on:
    """
    { "points": 120, "measurement":"beat", "start": "-30d", "algo": "sine", "prec": "sec", "name": "bar"}
    """
    When click the empty Create dashboard dropdown button
    When click the create dashboard item "New Dashboard"
    Then the new dashboard page is loaded
    Then the empty dashboard contains a documentation link
    Then the empty dashboard contains Add a Cell button
    When name dashboard "про́бный прибо́ров"

@error-collateral
  Scenario: Exercise Dashboard Dropdowns
    When click dashboard time locale dropdown
    Then the active dashboard dropdown contains items:
    """
    Local,UTC
    """
    When click dashboard refresh dropdown
    Then the active dashboard dropdown contains dividers:
    """
    Refresh
    """
    Then the active dashboard dropdown contains items:
    """
    Paused,5s,10s,15s,30s,60s
    """
    When click dashboard time range dropdown
    Then the active dashboard dropdown contains dividers:
    """
    Time Range
    """
    Then the active dashboard dropdown contains items:
    """
    Custom Time Range,Past 5m,Past 15m,Past 1h,Past 6h,Past 12h,Past 24h,Past 2d,Past 7d,Past 30d
    """

@tested
  Scenario: Create Cell
    When click the empty create cell button
    Then the cell edit overlay is loaded as "Name this Cell"
    When name dashboard cell "вре́менный"
    When click dashboard cell edit cancel button
    Then there is no dashboard cell named "вре́менный"
    When click the empty create cell button
    Then the cell edit overlay is loaded as "Name this Cell"
    When name dashboard cell "вре́менный"
    When click dashboard cell save button
    Then the dashboard contains a cell named "вре́менный"

    #Currently failing due to issue #16619
@tested
  Scenario: Add Note to Cell
    When toggle context menu of dashboard cell named "вре́менный"
    When click cell content popover add note
    Then the edit note popup is loaded
    Then dismiss the popup
    Then popup is not loaded
    When toggle context menu of dashboard cell named "вре́менный"
    When click cell content popover add note
    Then click popup cancel simple button
    Then popup is not loaded
    When toggle context menu of dashboard cell named "вре́менный"
    When click cell content popover add note
    When enter the cell note popup CodeMirror text:
  """
  __Шинель__\n
  _Гоголь_\n
В департаменте… но лучше не называть в каком департаменте...
  """
    Then the cell note popup Markdown preview panel contains
  """
В департаменте… но лучше не называть в каком департаменте...
  """
    When click the cell note popup save button
    Then popup is not loaded
    Then the cell named "вре́менный" has a note indicator
    When click the note indicator of the "вре́менный" cell
    Then the cell note popover contains:
  """
В департаменте… но лучше не называть в каком департаменте...
  """
    When click the cell title "вре́менный"
    Then the cell note popover is not loaded
    When toggle context menu of dashboard cell named "вре́менный"
    Then the cell content popover has item edit note
    When click the cell title "вре́менный"
    Then the cell content popover is not loaded

@tested
  Scenario: Edit Cell Note
    When toggle context menu of dashboard cell named "вре́менный"
    When click cell content popover add note
    Then the edit note popup is loaded
    Then the cell note popup Code Mirror text contains:
  """
  В департаменте… но лучше не называть в каком департаменте...
  """
    Then the cell note popup Code Mirror text contains:
  """
  _Гоголь_
  """
    Then the cell note popup Code Mirror text contains:
  """
  __Шинель__
  """
    Then the cell note popup Markdown preview panel contains
  """
В департаменте… но лучше не называть в каком департаменте...
  """
    When clear the cell note popup Code Mirror text
    Then the cell note popup markup preview panel has no text
    When enter the cell note popup CodeMirror text:
  """
  __LE MANTEAU__\n
  _Nikolaï Gogol_\n
Dans une administration russe... mieux vaut ne pas dire le nom de cette administration ...
  """
    Then the cell note popup Markdown preview panel contains
  """
Dans une administration russe... mieux vaut ne pas dire le nom de cette administration ...
  """
    When click the cell note popup save button
    Then popup is not loaded
    When click the note indicator of the "вре́менный" cell
    Then the cell note popover contains:
  """
Dans une administration russe... mieux vaut ne pas dire le nom de cette administration ...
  """
    When click the cell title "вре́менный"
    Then the cell content popover is not loaded

@error-collateral
  Scenario: Move cell
    When get metrics of cell named "вре́менный"
    When move the cell named "вре́менный" by "{ "dx": "+400", "dy": "+200" }"
    # Y is 0 below because of float to top feature - cells float to top row when repositioned
    Then the location of the cell named "вре́менный" is changed by "{ "dx": "+400", "dy": "0" }"
    When get metrics of cell named "вре́менный"
    When move the cell named "вре́менный" by "{ "dx": "-400", "dy": "+200" }"
    Then the location of the cell named "вре́менный" is changed by "{ "dx": "-400", "dy": "0" }"
    When get metrics of cell named "вре́менный"
    When move the cell named "вре́менный" by "{ "dx": "0", "dy": "+200" }"
    Then the location of the cell named "вре́менный" is changed by "{ "dx": "0", "dy": "0" }"
    When move the cell named "вре́менный" by "{ "dx": "+400", "dy": "+200" }"
    When get metrics of cell named "вре́менный"
    When click nav menu item "home"
    #When hover over the "Dashboards" menu item
    #When click nav menu item "home"
    When click nav menu item "Dashboards"
    #When hover over the "Dashboards" menu item
    #When click nav sub menu "Dashboards"
    When click the dashboard name "про́бный прибо́ров"
    Then the dashboard named "про́бный прибо́ров" is loaded
    Then the location of the cell named "вре́менный" is unchanged
    When move the cell named "вре́менный" by "{ "dx": "-400", "dy": "0" }"

@tested
  Scenario: Edit Cell - Simple
    Then the cell named "вре́менный" contains the empty graph message
    When toggle context menu of dashboard cell named "вре́менный"
    When click cell content popover configure
    Then the cell edit overlay is loaded as "вре́менный"
    When get the current cell edit preview graph
    When click the cell edit Time Range Dropdown
    When select the cell edit Time Range "Past 30d"
    When click the cell edit Script Editor button
    When paste into cell edit Script Editor
  """
  from(bucket: "qa")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "foo")
  |> filter(fn: (r) => r._field == "level")
  """
    When click the time machine cell edit submit button
    Then the time machine cell edit preview graph is shown
    Then the cell edit preview graph is changed
    When click the cell edit save button
    When click the dashboard Time Range Dropdown
    When select dashboard Time Range "30d"
    Then the cell named "вре́менный" contains a graph

@error-collateral
  Scenario: Resize Cell
    When get the current graph of the cell "вре́менный"
    When get metrics of cell named "вре́менный"
    When resize the cell name "вре́менный" by "{ "dw": "+300", "dh": "+100" }"
    Then the graph of the cell "вре́менный" has changed
    Then size of the cell named "вре́менный" has changed by "{ "dw": "+300", "dh": "+100" }"
    # Leave then return check TODO after issue 16180 fixed
    #When get the current graph of the cell "вре́менный"
    #When get metrics of cell named "вре́менный"
    #When hover over the "Dashboards" menu item
    #When click nav menu item "home"
    #When hover over the "Dashboards" menu item
    #When click nav sub menu "Dashboards"
    #When click the dashboard name "про́бный прибо́ров"
    #Then the dashboard named "про́бный прибо́ров" is loaded
    #Then the size of the of the cell named "вре́менный" is unchangd

@error-collateral
  Scenario: Hover Cell Graph
    When hover over the graph of the cell named "вре́менный"
    Then the cell graph data point infobox is visible

@error-collateral
  Scenario: Zoom Cell horizontal
    When get the current graph of the cell "вре́менный"
    When move horizontally to "2/5" of graph cell named "вре́менный"
    When drag horizontally to "3/5" of graph cell named "вре́менный"
    Then the graph of the cell "вре́менный" has changed

@error-collateral
  Scenario: Unzoom Cell
    When get the current graph of the cell "вре́менный"
    When Click at the point "{"x": "1/2", "y": "1/2"}" of graph cell named "вре́менный"
    Then the graph of the cell "вре́менный" has changed

@error-collateral
  Scenario: Zoom Cell vertical
    When get the current graph of the cell "вре́менный"
    When move vertically to "2/5" of graph cell named "вре́менный"
    When drag vertically to "3/5" of graph cell named "вре́менный"
    Then the graph of the cell "вре́менный" has changed

@error-collateral
  Scenario: Unzoom Cell 2
    When get the current graph of the cell "вре́менный"
    When Click at the point "{"x": "1/2", "y": "1/2"}" of graph cell named "вре́менный"
    Then the graph of the cell "вре́менный" has changed

@tested
  Scenario: Rename Cell
    When toggle context menu of dashboard cell named "вре́менный"
    When click cell content popover configure
    Then the cell edit overlay is loaded as "вре́менный"
    When click on the cell edit name
    When change the cell edit name to "dočasný"
    When click the cell edit save button
    Then the cell named "dočasný" is visible in the dashboard

@tested
  Scenario: Clone Cell
    When toggle context menu of dashboard cell named "dočasný"
    When click cell edit content popover clone
    Then the cell named "dočasný (Clone)" is visible in the dashboard
    When toggle context menu of dashboard cell named "dočasný (Clone)"
    When click cell content popover configure
    When clear the cell edit Script Editor
    When paste into cell edit Script Editor
  """
  from(bucket: "qa")
|> range(start: v.timeRangeStart, stop: v.timeRangeStop)
|> filter(fn: (r) => r._measurement == "foo")
|> filter(fn: (r) => r._field == "level")
|> movingAverage(n: 5)
  """
    When click the time machine cell edit submit button
    When click on the cell edit name
    When change the cell edit name to "klouzavý průměr"
    When click the cell edit save button
    Then the graph of the cell "dočasný" differs from "klouzavý průměr"
    # Following step fails due to issue #16619
    When click the note indicator of the "klouzavý průměr" cell
    Then the cell note popover contains:
  """
Dans une administration russe... mieux vaut ne pas dire le nom de cette administration ...
  """
    When click the cell title "klouzavý průměr"
    Then the cell content popover is not loaded

@error-collateral
  Scenario: Two cells column to row
    When get metrics of cell named "dočasný"
    When get metrics of cell named "klouzavý průměr"
    When move the cell named "dočasný" by "{ "dx": "+300", "dy": "0" }"
    Then the location of the cell named "klouzavý průměr" is changed by "{ "dx": "0", "dy": "-380" }"

@error-collateral
  Scenario: Two cells row to column
    When get metrics of cell named "dočasný"
    When get metrics of cell named "klouzavý průměr"
    When move the cell named "dočasný" by "{ "dx": "-300", "dy": "0" }"
    Then the location of the cell named "klouzavý průměr" is changed by "{ "dx": "0", "dy": "+380" }"

@error-collateral
  Scenario: Two cells enlarge first into second
    When get metrics of cell named "klouzavý průměr"
    When move the cell named "dočasný" by "{ "dx": "+300", "dy": "0" }"
    Then the location of the cell named "klouzavý průměr" is changed by "{ "dx": "0", "dy": "-380" }"
    When get metrics of cell named "dočasný"
    When get metrics of cell named "klouzavý průměr"
    When resize the cell name "klouzavý průměr" by "{ "dw": "+300", "dh": "0" }"
    Then the location of the cell named "dočasný" is changed by "{ "dx": "0", "dy": "+380" }"
    Then size of the cell named "klouzavý průměr" has changed by "{ "dw": "+300", "dh": "0" }"

@error-collateral
  Scenario: Two cells reduce first when above second
    When get metrics of cell named "dočasný"
    When get metrics of cell named "klouzavý průměr"
    When resize the cell name "klouzavý průměr" by "{ "dw": "-300", "dh": "0" }"
    Then the location of the cell named "dočasný" is changed by "{ "dx": "0", "dy": "-380" }"
    Then size of the cell named "klouzavý průměr" has changed by "{ "dw": "-300", "dh": "0" }"

@error-collateral
  Scenario: Two cells column to row - Moved cell drops down
    When get metrics of cell named "dočasný"
    When get metrics of cell named "klouzavý průměr"
    When move the cell named "dočasný" by "{ "dx": "-150", "dy": "150" }"
    Then the location of the cell named "dočasný" is changed by "{ "dx": "-150", "dy": "+380" }"

@tested
  Scenario Outline: Delete Cell
    When toggle context menu of dashboard cell named "<NAME>"
    When click cell content popover delete
    When click cell content popover delet confirm
    # following should be notification-primary [data-testid=notification-primary]
    #Then the success notification contains "Cell deleted from dashboard"
    Then the primary notification contains "Cell deleted from dashboard"
    Then close all notifications
    Then the cell named "<NAME>" is no longer present
  Examples:
    |NAME|
    |klouzavý průměr|
    |dočasný        |

# TODO - Dark Mode / Light Mode
