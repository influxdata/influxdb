@feature-dashboards
@dashboards-dashboards
Feature: Dashboards - Base
  As a user I want to Read Create Update and Delete Dashboards
  So that I can organize my data in Influxdbv2

@tested
  Scenario: Load dashboards page
    Given I reset the environment
    Given run setup over REST "DEFAULT"
    When open the signin page
    When UI sign in user "DEFAULT"
    When click nav menu item "Dashboards"
    #When hover over the "Dashboards" menu item
    #When click nav sub menu "Dashboards"
    Then the Dashboards page is loaded
    When API sign in user "DEFAULT"
    When API create a label "Cesko" described as "Pravda vitezi" with color "#AAFFAA" for user "DEFAULT"
    When API create a label "Mesto" described as "Matka mest" with color "#FFAAAA" for user "DEFAULT"
    When generate a line protocol testdata for user "DEFAULT" based on:
    """
    { "points": 120, "measurement":"level", "start": "-30d", "algo": "hydro", "prec": "sec", "name": "hydro"}
    """
    When generate a line protocol testdata for user "DEFAULT" based on:
    """
    { "points": 120, "measurement":"beat", "start": "-30d", "algo": "sine", "prec": "sec", "name": "sine"}
    """


@tested
  Scenario: Create new dashboard
    When click the empty Create dashboard dropdown button
    Then the empty create dashboard dropdown list contains
    """
  New Dashboard,Import Dashboard,From a Template
    """
    When click the create dashboard item "New Dashboard"
    Then the new dashboard page is loaded
    When click nav menu item "Dashboards"
    #When hover over the "Dashboards" menu item
    #When click nav sub menu "Dashboards"
    Then the empty Create dashboard dropdown button is not present
    Then there is a dashboard card named "Name this Dashboard"

@tested
  Scenario: Rename dashboard from card
    When hover over dashboard card named "Name this Dashboard"
    Then the export button for the dashboard card "Name this Dashboard" is visible
    Then the clone button for the dashboard card "Name this Dashboard" is visible
    Then the delete button for the dashboard card "Name this Dashboard" is visible
    When hover over dashboard card name "Name this Dashboard"
    When click the edit dashboard card name button for "Name this Dashboard"
    When clear the name input of the dashboard card "Name this Dashboard"
    When enter the new name "Mercure" in the name input of the dashboard card "Name this Dashboard"
    When press the "ENTER" key
    Then there is a dashboard card named "Mercure"
    Then there is no dashboard card named "Name this Dashboard"

@tested
  Scenario: Add description to dashboard
    Then the description for card "Mercure" contains "No description"
    When hover over description of the dashboard card "Mercure"
    When click the edit description button for the dashboard card "Mercure"
    When enter into the dashboard card "Mercure" the description:
    """
    le dieu du commerce dans la mythologie romaine
    """
    When press the "ENTER" key
    Then the description for card "Mercure" contains "le dieu du commerce dans la mythologie romaine"

@tested
  Scenario: Add Labels to dashboard
    # Issue 16529 - possible design change
    When click empty label for the dashboard card "Mercure"
    Then the label "Cesko" in the popover selector is visible
    Then the label "Mesto" in the popover selector is visible
    Then the create new label item is not visible in the popover
    When enter "Slovensko" in the popover label selector filter
    Then the create new label item is visible in the popover
    Then there are "0" label pills in the select label popover
    When click the new label item in the add labels popover
    Then the create Label popup is loaded
    When dismiss the popup
    Then popup is not loaded
    Then the add label popover is not present
    When click the add label button for the dashboard card "Mercure"
    # Issue 16528 - control values not cleared
    Then there are "2" label pills in the select label popover
    When enter "Slovensko" in the popover label selector filter
    When click the new label item in the add labels popover
    When click the label popup Create Label button
    Then popup is not loaded
    Then the dashboard card "Mercure" has the label "Slovensko"
    When hover over the label "Slovensko" of the dashboard card "Mercure"
    When click remove label "Slovensko" from the dashboard card "Mercure"
    Then the dashboard card "Mercure" labels empty message is visible
    Then the label "Slovenkso" of the dashboard card "Mercure" is not present
    When click the add label button for the dashboard card "Mercure"
    Then there are "3" label pills in the select label popover
    When enter "sko" in the popover label selector filter
    Then there are "2" label pills in the select label popover
    Then the label "Mesto" is not present in the popover selector
    When clear the popover label selector filter
    Then there are "3" label pills in the select label popover
    # TODO use escape key once #17853 is resolved
    #When press the "ESCAPE" key
    When click the dashboards filter input
    Then the add label popover is not present
    #TODO add check for issue #17964

@tested
  Scenario Outline: Create new Dashboard
    When click create dashboard control
    When click the create dashboard item "New Dashboard"
    When name dashboard "<NAME>"
    When click nav menu item "Dashboards"
    #When hover over the "Dashboards" menu item
    #When click nav sub menu "Dashboards"
    Then there is a dashboard card named "<NAME>"

    Examples:
    |NAME|
    |Terre|
    |Venus|
    |Jupiter|
    |Mars |

@tested
  Scenario: Access Dashboard from home page
    When hover over the "home" menu item
    When click nav menu item "home"
    Then the dashboards panel contains links:
    """
    Mercure,Venus,Terre,Mars,Jupiter
    """
    When click the dashboards panel link "Mercure"
    Then the dashboard named "Mercure" is loaded

@tested
  Scenario: Filter Dashboard Cards
    When click nav menu item "Dashboards"
    #When hover over the "Dashboards" menu item
    #When click nav sub menu "Dashboards"
    Then the dashboards page contains the cards:
    """
    Mercure,Venus,Terre,Mars,Jupiter
    """
    When enter the term "ter" in the dashboards filter
    Then the dashboards page contains the cards:
    """
    Terre,Jupiter
    """
    Then the dashboards page does not contain the cards:
    """
    Mercure,Venus,Mars
    """
    When clear the dashboards filter
    Then the dashboards page contains the cards:
    """
    Mercure,Venus,Terre,Mars,Jupiter
    """

@error-collateral
  Scenario: Import Dashboard from file
    When click create dashboard control
    When click the create dashboard item "Import Dashboard"
    Then the Import Dashboard popup is loaded
    When upload the import dashboard file "etc/test-data/alpha_dashboard.json"
    Then the import dashboard drag and drop header contains success "etc/test-data/alpha_dashboard.json"
    When click the Import Dashboard button
    Then popup is not loaded
    Then the success notification contains "Successfully imported dashboard."
    Then close all notifications
    Then there is a dashboard card named "Alpha Centauri"

@tested
  Scenario: Import Dashboard paste json
    When click create dashboard control
    When click the create dashboard item "Import Dashboard"
    Then the Import Dashboard popup is loaded
    When click the Import Dashboard popup radio button Paste Json
    Then the Import Dashboard file upload control is not present
    When paste contents of file "etc/test-data/tau_ceti_dashboard.json" into the JSON textarea
    When click the Import Dashboard button
    Then popup is not loaded
    Then the success notification contains "Successfully imported dashboard."
    Then close all notifications
    Then there is a dashboard card named "Tau Ceti"

@tested
  Scenario: Create Dashboard from template
    When create a new template from the file "etc/test-data/sine-test-template.json" for user "DEFAULT"
    When click nav menu item "Dashboards"
    #When hover over the "Dashboards" menu item
    #When click nav sub menu "Dashboards"
    Then the Dashboards page is loaded
    When click create dashboard control
    When click the create dashboard item "From a Template"
    Then the Create Dashboard from Template popup is loaded
    Then the Dashboard from Template create button is disabled
    When click Dashboard from Template popup cancel button
    Then popup is not loaded
    When click create dashboard control
    When click the create dashboard item "From a Template"
    Then dismiss the popup
    Then popup is not loaded
    When click create dashboard control
    When click the create dashboard item "From a Template"
    When click the template item "Sinusoid test data-Template"
    Then the template preview cell "Beat goes on" is visible
    When click Dashboard from Template create button
    Then there is a dashboard card named "Sinusoid test data"

@tested
  Scenario: Sort Dashboards by Name
    When close all notifications
    Then the dashboards are sorted as:
    """
    Alpha Centauri,Jupiter,Mars,Mercure,Sinusoid test data,Tau Ceti,Terre,Venus
    """
    When click dashboards sort type dropdown
    When click dashboards sort by "Name Desc"
    #When click dashboards sort by name
    Then the dashboards are sorted as:
    """
    Venus,Terre,Tau Ceti,Sinusoid test data,Mercure,Mars,Jupiter,Alpha Centauri
    """
    When click dashboards sort type dropdown
    When click dashboards sort by "Name Asc"
    Then the dashboards are sorted as:
    """
    Alpha Centauri,Jupiter,Mars,Mercure,Sinusoid test data,Tau Ceti,Terre,Venus
    """
  # Scenario: Sort Dashboards by Modified time
  # TODO - implement after issue #15610 is resolved

@error-collateral
  Scenario: Export Dashboard as Template
    When hover over dashboard card named "Alpha Centauri"
    When click export of the dashboard card named "Alpha Centauri"
    When click confirm export of the dashboard card "Alpha Centauri"
    When click Export Dashboard popup Save as Template
    Then the success notification contains "Successfully saved dashboard as template"
    Then a REST template document for user "DEFAULT" titled "Alpha Centauri-Template" exists
    Then close all notifications

@error-collateral
  Scenario: Export Dashboard copy to clipboard
    When hover over dashboard card named "Jupiter"
    When click export of the dashboard card named "Jupiter"
    When click confirm export of the dashboard card "Jupiter"
    When click Export Dashboard popup Copy to Clipboard
    Then the success notification contains "Copied dashboard to the clipboard"
    Then close all notifications
#N.B. clipboard not accessible for comparison in headless mode
    When click the Export Dashboard dismiss button
    Then popup is not loaded

@error-collateral
  Scenario: Export Dashboard to file
    When remove file "tau_ceti.json" if exists
    When hover over dashboard card named "Tau Ceti"
    When click export of the dashboard card named "Tau Ceti"
    When click confirm export of the dashboard card "Tau Ceti"
    Then the Export Dashboard popup is loaded
    When click the Export Dashboard dismiss button
    Then popup is not loaded
    When hover over dashboard card named "Tau Ceti"
    When click export of the dashboard card named "Tau Ceti"
    When click confirm export of the dashboard card "Tau Ceti"
    When click Export Dashboard popup Download JSON for "tau_ceti.json"
    Then popup is not loaded
    Then the file "tau_ceti.json" has been downloaded
    When remove file "tau_ceti.json" if exists

@tested
  Scenario: Clone Dashboard
    When hover over dashboard card named "Tau Ceti"
    When click clone of the dashboard card named "Tau Ceti"
    When click the clone confirm of dashboard card "Tau Ceti"
    Then the dashboard named "Tau Ceti (clone 1)" is loaded
    Then the dashboard contains a cell named "Hydro derivative"
    Then the dashboard contains a cell named "Sinusoid sum - missed points"
    Then the dashboard contains a cell named "Note"
    When click nav menu item "Dashboards"
    #When hover over the "Dashboards" menu item
    #When click nav sub menu "Dashboards"
    Then there is a dashboard card named "Tau Ceti (clone 1)"

@tested
  Scenario Outline: Delete dashboards
    When hover over dashboard card named "<NAME>"
    When click delete of dashboard card "<NAME>"
    When click delete confirm of dashboard card "<NAME>"
    Then the success notification contains "Dashboard <NAME> deleted successfully"
    When close all notifications
    Then there is no dashboard card named "<NAME>"

    Examples:
      |NAME|
      |Mercure|
      |Terre|
      |Venus|
      |Jupiter|
      |Mars|
      |Alpha Centauri|
      |Tau Ceti|
      |Tau Ceti (clone 1)|
      |Sinusoid test data|
