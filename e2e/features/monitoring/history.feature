@feature-monitoring
@monitoring-alerts
@use-live-data
Feature: Monitoring - Alerts - History
  As a user I want to setup alerts
  So that I can be notified of important changes in the data

  Scenario: Load Initial Alerts view
    Given I reset the environment
    Given run setup over REST "DEFAULT"
    When API sign in user "DEFAULT"
    When start live data generator
  # It seems 5s is the quickest we can use stably given default values in create check controls
  # Tried 1s, but need to use agg function like mean so the checks do not seem to match
  """
  { "pulse": 5000, "model": "count10" }
  """
    When wait "10" seconds
    When create check over API from file "etc/test-data/test_threshold_check.json" for user "DEFAULT"
    When create check over API from file "etc/test-data/test_deadman_crit_check.json" for user "DEFAULT"
    When open the signin page
    When UI sign in user "DEFAULT"
    When click nav menu item "Alerting"
    Then the Alerting page is loaded

# Create checks over API

# Exercise controls

# Toggle markers

# Filter - N.B. clear filter shows all checks


