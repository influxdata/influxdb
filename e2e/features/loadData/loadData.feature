@feature-loadData
@loadData-loadData
Feature: Load Data - Base
  As a user I want to open the Load Data page
  So that I can explore how data is being loaded into Influxdbv2

@tested
  Scenario: Verify Tabs
    Given I reset the environment
    Given run setup over REST "DEFAULT"
    When open the signin page
    When UI sign in user "DEFAULT"
    When open page "load-data" for user "DEFAULT"
    When click load data tab "Buckets"
    Then the buckets tab is loaded
    When click load data tab "Telegrafs"
    Then the Telegraf Tab is loaded
    When click load data tab "Scrapers"
    Then the Scrapers Tab is loaded
    When click load data tab "Tokens"
    Then the tokens tab is loaded
