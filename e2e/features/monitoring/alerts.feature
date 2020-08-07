@feature-monitoring
@monitoring-alerts
@use-live-data
Feature: Monitoring - Alerts - Base
  As a user I want to setup alerts
  So that I can be notified of important changes in the data

@tested
Scenario: Load Initial Alerts view
  Given I reset the environment
  Given run setup over REST "DEFAULT"
  #When hover over the "alerting" menu item
  #When click nav sub menu "Monitoring & Alerting"
  When API sign in user "DEFAULT"
  When API create a label "Peano" described as "Theorie des ensembles" with color "#AAFFAA" for user "DEFAULT"
  When API create a label "Euclide" described as "Geometrie euclidienne" with color "#FFAAAA" for user "DEFAULT"
  When API create a label "Leibniz" described as "Calcul infinitésimal" with color "#AAAAFF" for user "DEFAULT"
  When API create a label "Descartes" described as "Géométrie analytique" with color "#FFFFAA" for user "DEFAULT"
  When start live data generator
  # It seems 5s is the quickest we can use stably given default values in create check controls
  # Tried 1s, but need to use agg function like mean so the checks do not seem to match
  """
  { "pulse": 5000, "model": "count10" }
  """
  When open the signin page
  When UI sign in user "DEFAULT"
  When click nav menu item "Alerting"
  Then the Alerting page is loaded
  When wait "10" seconds

@tested
Scenario: Exercise Initial Alerts view Controls
  Then the notification rules create dropdown is disabled
  When click alerting tab "checks"
  When click the create check button
  Then the create check dropodown list contains the items
  """
  threshold,deadman
  """
  When click the create check button
  Then the create check dropdown list is not visible
  When hover the create check question mark
  Then the create check tooltip is visible
  When hover the alerts page title
  Then the create check tooltip is not visible
  When click alerting tab "endpoints"
  When hover the create endpoint question mark
  Then the create endpoint tooltip is visible
  When hover the alerts page title
  Then the create endpoint tooltip is not visible
  When click alerting tab "rules"
  When hover the create rule question mark
  Then the create rules tooltip is visible
  When hover the alerts page title
  Then the create rules tooltip is not visible
  When click alerting tab "endpoints"
  When click create endpoint button
  Then the create endpoint popup is loaded
  When dismiss the popup
  Then popup is not loaded
  When click alerting tab "checks"
  When click the first time create threshold check
  Then the edit check overlay is loaded
  When dismiss edit check overlay
  Then the edit check overlay is not loaded
  When click the first time create deadman check
  Then the edit check overlay is loaded
  When dismiss edit check overlay
  Then the edit check overlay is not loaded

# Create and start endpoint listener for notification checks - maybe move to separate endpoints test suite

# Exercise Configure Check -- N.B. try and reuse dashboard time machine for Define Query
# TODO - Check illogical alert thresholds
# TODO - add simple tags check

@tested
  Scenario: Exercise Configure Check - Threshold
    When click the create check button
    When click the create check dropdown item "Threshold"
  # Query Builder steps cover same library as in dashboards - TODO - check for gaps
  # For now cover just configure check step
    When click check editor configure check button
    Then the configure check view is loaded
    Then the create check checklist contains:
  """
  [{ "state": "error", "text": "One field" },
  { "state": "valid", "text": "One aggregate function" },
  { "state": "error", "text": "One or more thresholds"}]
  """
    Then the check interval hint dropdown list is not visible
    When click on check interval input
    Then the check interval hint dropdown list includes
  """
  5s,15s,1m,6h,24h,30d
  """
    When click the interval hint dropdown list item "5m"
    Then the check interval hint dropdown list is not visible
    Then the interval indicator is set to "5m"
    Then the check offset hint dropdown list is not visible
    When click the check offset interval input
    Then the check offset hint dropdown list includes
  """
  0s,5s,1m,1h,12h,2d
  """
    When click the offset hint dropdown list item "1m"
    Then the check offset hint dropdown list is not visible
    Then the offset input is set to "1m"
    When update the check message template to
  """
  Kapela z Varsavy
  """
    Then the check message tempate contains
  """
  Kapela z Varsavy
  """
    When click add threshold condition "CRIT"
    When click the threshold definition dropdown for condition "CRIT"
  # TODO - after issue 17729 is resolved - should be equal criteria e.g. n == 0
    Then the threshold definition dropdown for "CRIT" contain items:
  """
  is above,is below,is inside range,is outside range
  """
    When click the threshold definition dropodown item "Is Inside Range" for condition "CRIT"
    Then there is a binary boundary for the threshold "CRIT" with values "20" and "100"
  # N.B. currently cannot easily set negatve values - TODO use negative values once #17782 is resolved
  # N.B. TODO - check dimensions of inputs - currently in smaller views they are unreadable #17783
    When set the binary boundary for the threshold "CRIT" from "0" to "1000"
    Then there is a binary boundary for the threshold "CRIT" with values "0" and "1000"
    When click add threshold condition "WARN"
    When click the threshold definition dropdown for condition "WARN"
    When click the threshold definition dropodown item "Is Below" for condition "WARN"
    When set the unary boundary value for the threshold definition "WARN" to "0"
    Then there is a unary boundary for the threshhold "WARN" with the value "0"
    When dismiss edit check overlay
    Then the first time create threshold check is visible
    Then the first time create deadman check is visible

@error-collateral
  Scenario: Exercise configure check Deadman
  # Just check Deadman fields others were covered in threshold test
    When click the create check button
    When click the create check dropdown item "Deadman"
    When click check editor configure check button
    Then the create check checklist contains:
  """
  [{ "state": "error", "text": "One field" }]
  """
    When click the deadman definition No Values For input
    Then the deadman definition hints dropdown contains:
  """
  15s,5m,1h,12h,7d
  """
    When click the deadman definition hint dropdown item "1m"
    Then the deadman definition No Values For input contains "1m"
    When set the value of the deadman definition No Values for input to "30m"
    When click the deadman definition level dropdown
    Then the deadman definition level dropdown contains:
  """
  CRIT,WARN,INFO,OK
  """
    When click the deadman definition level dropdown item "WARN"
    Then the deadman definition level dropdown selected item is "WARN"
    When click the deadman definition Stop Checking input
    Then the deadman definition stop hints dropdown contains:
  """
  5s,1m,1h,24h,7d,30d
  """
    When click the deadman definition stop hint dropdown item "5m"
    Then the deadman definition stop input contains "5m"
    When set the value of the definition stop input to "10m"
    Then the deadman definition stop input contains "10m"
    When dismiss edit check overlay
    Then the first time create threshold check is visible
    Then the first time create deadman check is visible

# Create Threshold Alerts
@tested
  Scenario: Create Simple Threshold Check
    When click the first time create threshold check
    Then the create check checklist contains:
  """
  [{ "state": "error", "text": "One field" },
  { "state": "valid", "text": "One aggregate function" },
  { "state": "error", "text": "One or more thresholds"}]
  """
    Then the save check button is disabled
    When enter the alert check name "Simple Count Check"
    When send keys "ENTER"
    When click the tag "test" in builder card "1"
    When click the tag "val" in builder card "2"
    When click the query builder function "mean"
    Then the create check checklist contains:
  """
  [{ "state": "valid", "text": "One field" },
  { "state": "valid", "text": "One aggregate function" },
  { "state": "error", "text": "One or more thresholds"}]
  """
    Then the save check button is disabled
  # 7.8.20 -- feature disabled - see issue 19249
    When click the time machine query builder function duration input
    When click the query builder function duration suggestion "5s"
    When click the time machine cell edit submit button
    Then the time machine cell edit preview graph is shown
    When click check editor configure check button
    Then the interval indicator is set to "5s"
    Then the time machine cell edit preview graph is shown
    When enter into interval offset "1s"
    When send keys "ENTER"
    When update the check message template to
  """
${ r._check_name } is: ${ r._level } value was ${string(v: r.val)}
  """
    When click add threshold condition "CRIT"
    When click the threshold definition dropdown for condition "CRIT"
    When click the threshold definition dropodown item "Is Above" for condition "CRIT"
    When set the unary boundary value for the threshold definition "CRIT" to "7.5"
    Then the create check checklist is not present
    Then the save check button is enabled
    Then the time machine cell edit preview contains threshold markers:
    """
    CRIT
    """
    When click the check editor save button
    Then there is an alert card named "Simple Count Check"

    # Create Deadman Alerts
@error-collateral
  Scenario: Create simple Critical Deadman Check
  # Just check Deadman fields others were covered in threshold test
    When click the create check button
    When click the create check dropdown item "Deadman"
    When enter the alert check name "Deadman Critical Check"
    When click the tag "test" in builder card "1"
    When click the tag "val" in builder card "2"
    When click the time machine cell edit submit button
    Then the time machine cell edit preview graph is shown
    When click check editor configure check button
    When set the check interval input to "10s"
    When set the check offset interval input "2s"
    When click the edit check add tag button
    When set the check tag key of tag "1" to "mrtvola"
    When set the check tag value of tag "1" to "neboztik"
    When click the edit check add tag button
    When set the check tag key of tag "2" to "kartoffel"
    When set the check tag value of tag "2" to "brambor"
    When update the check message template to
  """
${ r._check_name } is: ${ r._level } value [${string(v: r.val)}] has stopped reporting
  """
    When set the value of the deadman definition No Values for input to "30s"
    When set the value of the definition stop input to "2m"
    When click the check editor save button
    Then there is an alert card named "Deadman Critical Check"

  # Need second card for filter and sort tests
@error-collateral
  Scenario: Create simple Warn Deadman Check
  # Just check Deadman fields others were covered in threshold test
    When click the create check button
    When click the create check dropdown item "Deadman"
    When enter the alert check name "Deadman Warn Check"
    When click the tag "test" in builder card "1"
    When click the tag "val" in builder card "2"
    When click the time machine cell edit submit button
    Then the time machine cell edit preview graph is shown
    When click check editor configure check button
    When set the check interval input to "10s"
    When set the check offset interval input "2s"
    When click the edit check add tag button
    When update the check message template to
  """
${ r._check_name } is: ${ r._level } has stopped reporting.  Last value [${string(v: r.val)}]
  """
    When set the value of the deadman definition No Values for input to "20s"
    When set the value of the definition stop input to "1m"
    When click the check editor save button
    Then the error notification contains "Failed to create check: tag must contain a key and a value"
    When close all notifications
    When remove check tag key "1"
    When click the check editor save button
    Then there is an alert card named "Deadman Warn Check"

# TODO - EDIT Threshold Check and drag threshold control in graph

# Edit Check Card
@error-collateral
Scenario: Edit Check Card
   When hover over the name of the check card "Deadman Warn Check"
   When click the name edit button of the check card "Deadman Warn Check"
   When update the active check card name input to "Veille automatique - Avertissement"
   When send keys "ENTER"
   Then there is an alert card named "Veille automatique - Avertissement"
   When hover over the description of the check card "Veille automatique - Avertissement"
   When click the description edit button of the check card "Veille automatique - Avertissement"
   When update the active check card description input to:
  """
Que ta voix, chat mystérieux, Chat séraphique, chat étrange... Baudelaire
  """
  When send keys "ENTER"
  Then the check card "Veille automatique - Avertissement" contains the description:
  """
Que ta voix, chat mystérieux, Chat séraphique, chat étrange... Baudelaire
  """

# Add labels to checks
@tested
Scenario: Add Labels To Checks
  When click empty label for check card "Deadman Critical Check"
  Then the add label popover is present
  # dismiss popover
  # TODO - once #17853 is fixed - use ESC key to dismiss popover
  When click the checks filter input
  Then the add label popover is not present
  When click the add labels button for check card "Deadman Critical Check"
  Then the add label popover is present
  Then the add label popover contains the labels
  """
  Peano,Euclide,Leibniz,Descartes
  """
  When click the label popover item "Peano"
  When click the label popover item "Leibniz"
  Then the add label popover contains the labels
  """
  Euclide,Descartes
  """
  Then the add label popover does not contain the labels:
  """
  Peano,Leibniz
  """
  When set the label popover filter field to "Godel"
  Then the add label popover does not contain the labels:
  """
  Euclide,Descartes
  """
  Then the label popover contains create new "Godel"
  When clear the popover label selector filter
  Then the add label popover contains the labels
  """
  Euclide,Descartes
  """
  Then the add label popover does not contain the labels:
  """
  Peano,Leibniz
  """
  Then the add label popover does not contain create new
  # TODO - use escape to close popover once #17853 is resolved
  When click the checks filter input
  Then the add label popover is not present
  Then the check card "Deadman Critical Check" contains the label pills:
  """
  Peano,Leibniz
  """
  When remove the label pill "Peano" from the check card "Deadman Critical Check"
  Then the check card "Deadman Critical Check" contains the label pills:
  """
  Leibniz
  """
  Then the check card "Deadman Critical Check" does not contain the label pills:
  """
  Peano
  """
  When click the add labels button for check card "Deadman Critical Check"
  Then the add label popover contains the labels
  """
  Peano,Euclide,Descartes
  """
  # TODO - use escape to close popover once #17853 is resolved
  When click the checks filter input

# Clone check
@error-collateral
  Scenario: Clone Check
    When hover over the name of the check card "Simple Count Check"
  #  When wait "1" seconds
    When click the check card "Simple Count Check" clone button
    When click the check card "Simple Count Check" clone confirm button
    Then there is an alert card named "Simple Count Check (clone 1)"
    When click the check card name "Simple Count Check (clone 1)"
    Then the edit check overlay is loaded
    Then the current edit check name is "Simple Count Check (clone 1)"
    Then the interval indicator is set to "5s"
    Then the offset input is set to "1s"
    Then the check message tempate contains
    """
${ r._check_name } is: ${ r._level } value was ${string(v: r.val)}
    """
    Then there is a unary boundary for the threshhold "CRIT" with the value "7.5"
    When click checkeditor define query button
    Then there are "3" time machine builder cards
    Then time machine builder card "1" contains:
  """
  test
  """
    Then time machine builder card "2" contains:
  """
  val
  """
    Then the item "test" in builder card "1" is selected
    Then the item "val" in builder card "2" is selected
    # TODO - verify Bucket Card contents after #17879 fixed
    When enter the alert check name "Bécik"
    When click the check editor save button
    Then there is an alert card named "Bécik"

# Filter Checks
@error-collateral
  Scenario: Filter Checks
    Then the check cards column contains
  """
  Simple Count Check, Deadman Critical Check, Veille automatique - Avertissement, Bécik
  """
    When enter into the check cards filter field "Check"
    Then the check cards column contains
  """
  Simple Count Check, Deadman Critical Check
  """
    Then the check cards column does not contain
  """
  Veille automatique - Avertissement, Bécik
  """
    When enter into the check cards filter field "Be"
    Then the "checks" cards column empty state message is "No checks match your search"
    When enter into the check cards filter field "Bé"
    Then the check cards column contains
  """
  Bécik
  """
    Then the check cards column does not contain
  """
  Simple Count Check, Deadman Critical Check
  """
    When clear the check cards filter field
    Then the check cards column contains
  """
  Simple Count Check, Deadman Critical Check, Veille automatique - Avertissement, Bécik
  """

  Scenario: Threshold Check history - basic
    When hover over the name of the check card "Simple Count Check"
    # Collect some data - generate at least 1 event
    When wait "10" seconds
    When click open history of the check card "Simple Count Check"
    When click open history confirm of the check card "Simple Count Check"
    # Just check page load
    # Check history will be separate test feature
    Then the Check statusses page is loaded
    Then there are at least "1" events in the history
    Then event no "1" contains the check name "Simple Count Check"
    When click the check name of event no "1"
    Then the edit check overlay is loaded
    Then the current edit check name is "Simple Count Check"
    When dismiss edit check overlay
    Then the edit check overlay is not loaded
    Then the Alerting page is loaded
    Then there is an alert card named "Simple Count Check"

  Scenario: Deadman Check history - basic
    When stop live data generator
    When wait "60" seconds
    When hover over the name of the check card "Deadman Critical Check"
    When click open history of the check card "Deadman Critical Check"
    When click open history confirm of the check card "Deadman Critical Check"
    Then the Check statusses page is loaded
    Then there are at least "1" events in the history
    Then event no "1" contains the check name "Deadman Critical Check"
    Then there is at least "1" events at level "crit"
    When click the check name of event no "1"
    Then the edit check overlay is loaded
    Then the current edit check name is "Deadman Critical Check"
    When dismiss edit check overlay
    Then the edit check overlay is not loaded
    Then the Alerting page is loaded
    Then there is an alert card named "Deadman Critical Check"
    When start live data generator
    # restart live generator as above
    """
    { "pulse": 5000, "model": "count10" }
    """

# Delete Check
  Scenario Template: Delete Check
    When hover over the name of the check card "<NAME>"
    When click delete of the check card "<NAME>"
    When click delete confirm of the check card "<NAME>"
    Then there is no alert card named "<NAME>"
    Examples:
      |NAME|
      |Bécik|
      |Veille automatique - Avertissement|
      |Deadman Critical Check|
      |Simple Count Check|


# TODO - Edit Check definition -
# Edit Check definition

# Create Endpoints {HTTP, Slack, Pager Duty}

# Add labels to Endpoints

# Filter Endpoints

# Edit Endppints

# Create Rules

# Add labels to Rules

# Filter Rules

# Edit Rules

# Delete Checks (N.B. what is affect on dependent rules?)

# Delete Endpoints (N.B. what is affect on dependent rules?)

# Delete Rules

# Tear down data generator - In After All hook - needs to be torn down after failure as well as success

# Tear down http listened - In After All hook - ditto

# NOTE - perhaps should have five features - base, checks, endpoints, rules, full monitoring (too harvest alerts
# and notifications.) - breakup planned tests above into these feature files.
