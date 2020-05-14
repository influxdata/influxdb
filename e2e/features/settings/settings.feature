@feature-settings
@settings-settings
Feature: Settings - Base
  As a user I want to open the settings page
  So that I can explore how this Influx2 installation is set up

# TODO with alerting / load data refactor now submenu is richer

  Scenario: Verify Tabs
    Given I reset the environment
    Given run setup over REST "DEFAULT"
    When open the signin page
    When UI sign in user "DEFAULT"
    When open page "settings" for user "DEFAULT"
    When click the settings tab "Variables"
    Then the variables Tab is loaded
    When click the settings tab "Templates"
    Then the templates Tab is loaded
    When click the settings tab "Labels"
    Then the labels Tab is loaded
#    When click the settings tab "Tokens" # Tokens tab is no longer available
#    Then the tokens Tab is loaded # Tokens tab is no longer available
  # The following items are no longer present commit=bd91a81123 build_date=2020-04-07T07:57:22Z
#    When click the settings tab "Profile"
#    Then the org profile Tab is loaded
#    When click the settings tab "Members"
#    Then the members Tab is loaded
