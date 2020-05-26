@feature-influx
@influx-influx
Feature: Influx common
  Click through the controls common to all influx pages

@tested
  Scenario: Open home page
    Given I reset the environment
    Given run setup over REST "DEFAULT"
    When open the signin page
    When UI sign in user "DEFAULT"
    When open page "HOME" for user "DEFAULT"
    Then influx page is loaded
    #Then the header contains the org name "DEFAULT"
    Then the home page header contains "Getting Started"

@tested
  Scenario: Click Data Explorer
    #Then the menu item text "Data Explorer" is "hidden"
    #When hover over the "explorer" menu item
    #Then the menu item text "Data Explorer" is "visible"
    When click nav menu item "Explorer"
    Then the Data Explorer page is loaded

@tested
   Scenario: Click Dashboards
     #Then the menu item text "Dashboards" is "hidden"
     #When hover over the "dashboards" menu item
     #Then the menu item text "Dashboards" is "visible"
     When click nav menu item "Dashboards"
     Then the Dashboards page is loaded

@tested
  Scenario: Click Tasks
    #Then the menu item text "Tasks" is "hidden"
    #When hover over the "tasks" menu item
    #Then the menu item text "Tasks" is "visible"
    When click nav menu item "Tasks"
    Then the Tasks page is loaded

@tested
  Scenario: Click Alerting
    #Then the menu item text "Monitoring & Alerting" is "hidden"
    #When hover over the "alerting" menu item
    #Then the menu item text "Monitoring & Alerting" is "visible"
    When click nav menu item "Alerting"
    Then the Alerting page is loaded

@tested
  Scenario: Click Load Data
    #Then the menu item text "Settings" is "hidden"
    #When hover over the "loadData" menu item
    #Then the menu item text "Load Data" is "visible"
    #Then the menu item text "Buckets" is "visible"
    #Then the menu item text "Telegraf" is "visible"
    #Then the menu item text "Scrapers" is "visible"
    When click nav menu item "LoadData"
    Then the buckets tab is loaded

  Scenario: Click Settings
    #Then the menu item text "Settings" is "hidden"
    #When hover over the "settings" menu item
    #Then the menu item text "Settings" is "visible"
    When click nav menu item "Settings"
    Then the Settings page is loaded

  # Item no longer exists commit=bd91a81123 build_date=2020-04-07T07:57:22Z
  #Scenario: Hover Feedback
  #  Then the menu item text "Feedback" is "hidden"
  #  When hover over the "feedback" menu item
  #  Then the menu item text "Feedback" is "visible"
  #  Then the feedback URL should include "https://docs.google.com/forms"

  Scenario: Click home item
    #Then the home submenu items are "hidden"
    #When hover over the "home" menu item
    #Then the home submenu items are "visible"
    #When click nav sub menu "Create Organization"
    #Then the Create Organization form is loaded
    #When open page "HOME" for user "DEFAULT"
    #When hover over the "home" menu item
    When click nav menu item "home"
    Then the home page is loaded
    #Then the sign in page is loaded
    #When UI sign in user "DEFAULT"

# This appears to have been removed 2020-05-12
# See closed issue #17991
#  Scenario: Click Organization
#    When click nav menu item "Organization"
#    Then the Organization page is loaded

  Scenario: Click User
    When click nav menu item "User"
    Then the user menu items are "visible"

  # TODO - create organization from Home Menu Item


