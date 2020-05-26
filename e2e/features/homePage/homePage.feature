@feature-homePage
@homePage-homePage
Feature: Home Page

  Scenario: logout home page
    Given I reset the environment
    Given run setup over REST "DEFAULT"
    When open the signin page
    When UI sign in user "DEFAULT"
    When open page "HOME" for user "DEFAULT"
    When click logout from the home page
    Then the sign in page is loaded
    When UI sign in user "DEFAULT"
    Then the home page is loaded

  Scenario: Click Data Collector Panel
    When I click the panel "Data Collector"
    Then the Telegraf Tab is loaded

@tested
  Scenario: Check Dashboards Panel
    When hover over the "home" menu item
    When click nav menu item "home"
    When I click the panel "Dashboard"
    Then the Dashboards page is loaded
    When click create dashboard control
    When click "New Dashboard" in create dashboard dropdown
    When name dashboard "Test Dashboard"
    When hover over the "home" menu item
    When click nav menu item "home"
    Then the dashboards panel contains a link to "Test Dashboard"
    When click the dashboard link to "Test Dashboard"
    Then the dashboard named "Test Dashboard" is loaded

@tested
  Scenario: Click Alerting Panel
    When hover over the "home" menu item
    When click nav menu item "home"
    When I click the panel "Alerting"
    Then the Alerting page is loaded

  Scenario: Logout from Menu
    When hover over the "home" menu item
    When click nav menu item "home"
    When click logout from the home page
    Then the sign in page is loaded
    When UI sign in user "DEFAULT"



